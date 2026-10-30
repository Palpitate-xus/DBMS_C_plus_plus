#include "transaction/LockManager.h"
#include "common/DbError.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {
using dbms::LockManager;
using Mode = LockManager::LockMode;
enum class Kind { Table, Row, Page };
bool same(const LockManager::LockCheckpoint& a, const LockManager::LockCheckpoint& b) {
    return a.tableCounts == b.tableCounts && a.tableModes == b.tableModes && a.rowLocks == b.rowLocks &&
        a.rowModes == b.rowModes && a.pageLocks == b.pageLocks && a.pageModes == b.pageModes && a.gapCounts == b.gapCounts;
}
bool acquire(LockManager& manager, Kind kind, const std::string& database, const std::string& table, Mode mode) {
    if (kind == Kind::Row) return mode == Mode::Shared ? manager.rowLockShared(table, 7) : manager.rowLockExclusive(table, 7);
    if (kind == Kind::Page) return mode == Mode::Shared ? manager.pageLockShared(database, table, 3) : manager.pageLockExclusive(database, table, 3);
    switch (mode) {
        case Mode::Shared: return manager.lockShared(table);
        case Mode::IntentShared: return manager.lockIntentShared(table);
        case Mode::IntentExclusive: return manager.lockIntentExclusive(table);
        case Mode::Exclusive: return manager.lockExclusive(table);
        case Mode::Metadata: return manager.lockMetadata(table);
    }
    return false;
}
void release(LockManager& manager, Kind kind, const std::string& database, const std::string& table, size_t depth) {
    if (kind == Kind::Row) manager.rowUnlock(table, 7);
    else if (kind == Kind::Page) manager.pageUnlock(database, table, 3);
    else for (size_t n = 0; n < depth; ++n) manager.unlock(table);
}
bool strong(const LockManager::LockCheckpoint& before, const LockManager::LockCheckpoint& after, Kind kind, Mode mode) {
    if (kind == Kind::Table) return before.tableCounts == after.tableCounts && after.tableModes.size() == 1 && after.tableModes.begin()->second == mode;
    if (kind == Kind::Row) return before.rowLocks == after.rowLocks && after.rowModes.size() == 1 && after.rowModes.begin()->second == Mode::Exclusive;
    return before.pageLocks == after.pageLocks && after.pageModes.size() == 1 && after.pageModes.begin()->second == Mode::Exclusive;
}
}

int main() {
    size_t controls = 0, failures = 0, ordinaryCases = 0, deadlockCases = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "LOCK_UPGRADE_FAILURE_FAIL " << label << '\n'; }
    };
    const std::string directory = "upgrade_failure_private";
    const bool owned = std::filesystem::create_directory(directory);
    check(owned, "create privately owned process-lock namespace");
    if (!owned) return 2;

    const auto exercise = [&](Kind kind, bool process, Mode initial, Mode wanted, size_t depth, const std::string& policy) {
        ++ordinaryCases;
        LockManager manager;
        const std::string database = process ? directory : "";
        const std::string table = "case_" + std::to_string(ordinaryCases);
        manager.setResourceNamespace(database);
        manager.setLockTimeout(policy == "timeout" ? 50 : 0);
        bool initialGood = true;
        for (size_t n = 0; n < depth; ++n) initialGood &= acquire(manager, kind, database, table, initial);
        check(initialGood, "acquire all original weak tokens");
        const auto before = manager.captureCheckpoint();
        std::mutex mutex;
        std::condition_variable condition;
        bool ready = false, freePeer = false, peerGood = false;
        std::thread peer([&] {
            manager.setResourceNamespace(database);
            peerGood = acquire(manager, kind, database, table, Mode::Shared);
            { std::lock_guard<std::mutex> guard(mutex); ready = true; }
            condition.notify_all();
            { std::unique_lock<std::mutex> guard(mutex); condition.wait(guard, [&] { return freePeer; }); }
            if (peerGood) release(manager, kind, database, table, 1);
        });
        { std::unique_lock<std::mutex> guard(mutex); condition.wait(guard, [&] { return ready; }); }
        check(peerGood, "real compatible peer holds the conflicting upgrade resource");
        bool observedWait = false, preservedWhileWaiting = true;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        manager.setInterruptHandler([&] {
            if (!manager.getLockWaits().empty()) {
                observedWait = true;
                preservedWhileWaiting &= same(before, manager.captureCheckpoint());
                if (policy == "cancel") throw dbms::DbError("57014", "native upgrade cancellation");
                if (policy == "success") {
                    { std::lock_guard<std::mutex> guard(mutex); freePeer = true; }
                    condition.notify_all();
                }
            }
            if (std::chrono::steady_clock::now() >= deadline)
                throw dbms::DbError("57014", "fixture guard, not expected cancellation");
        });
        bool upgraded = false;
        std::string state, message;
        try { upgraded = acquire(manager, kind, database, table, wanted); }
        catch (const dbms::DbError& error) { state = error.sqlState(); message = error.what(); }
        manager.clearInterruptHandler();
        check(observedWait && preservedWhileWaiting, "original exact checkpoint remains present throughout actual wait");
        check(policy == "success" ? upgraded && state.empty() : policy == "timeout" ? !upgraded && state.empty() :
              !upgraded && state == "57014" && message.find("native upgrade cancellation") != std::string::npos,
              "precise timeout/cancel/success result, no hang-guard substitution");
        const auto after = manager.captureCheckpoint();
        check(policy == "success" ? strong(before, after, kind, wanted) : same(before, after),
              "old ownership retained on failure, converted depth retained on success");
        manager.rollbackToCheckpoint(before);
        check(same(before, manager.captureCheckpoint()), "savepoint checkpoint is genuinely recoverable");
        { std::lock_guard<std::mutex> guard(mutex); freePeer = true; }
        condition.notify_all(); peer.join();
        bool reader = false, writer = false;
        std::thread observer([&] {
            manager.setResourceNamespace(database); manager.setLockTimeout(10);
            reader = acquire(manager, kind, database, table, Mode::Shared);
            if (reader) release(manager, kind, database, table, 1);
            writer = acquire(manager, kind, database, table, Mode::Exclusive);
            if (writer) release(manager, kind, database, table, 1);
        });
        observer.join();
        check(reader, "restored original weak mode still permits real reader");
        check(!writer, "only original owner still excludes real writer after competing peer exits");
        release(manager, kind, database, table, depth);
        check(manager.getLockHolds().empty(), "explicit owner release removes the final original token");
        check(manager.getLockWaits().empty(), "all timeout/cancel/observer wait edges cleaned");
    };
    for (bool process : {false, true}) {
        for (auto initial : {Mode::Shared, Mode::IntentShared, Mode::IntentExclusive})
            for (auto wanted : {Mode::Exclusive, Mode::Metadata})
                for (size_t depth : {1U, 3U})
                    for (const auto* policy : {"timeout", "cancel", "success"})
                        exercise(Kind::Table, process, initial, wanted, depth, policy);
        for (auto kind : {Kind::Row, Kind::Page})
            for (const auto* policy : {"timeout", "cancel", "success"})
                exercise(kind, process, Mode::Shared, Mode::Exclusive, 1, policy);
    }

    for (bool process : {false, true}) for (auto kind : {Kind::Table, Kind::Row, Kind::Page}) {
        ++deadlockCases;
        LockManager manager;
        const std::string database = process ? directory : "";
        const std::string table = "deadlock_" + std::to_string(deadlockCases);
        std::mutex mutex; std::condition_variable condition; size_t ready = 0;
        bool weak[2] = {}, outcome[2] = {}, preserved[2] = {};
        const size_t depth = kind == Kind::Table ? 3 : 1;
        std::thread threads[2];
        for (size_t index = 0; index < 2; ++index) threads[index] = std::thread([&, index] {
            manager.setResourceNamespace(database); manager.setLockTimeout(1000);
            weak[index] = true;
            for (size_t n = 0; n < depth; ++n) weak[index] &= acquire(manager, kind, database, table, Mode::Shared);
            const auto before = manager.captureCheckpoint();
            { std::unique_lock<std::mutex> guard(mutex); ++ready; condition.notify_all(); condition.wait(guard, [&] { return ready == 2; }); }
            outcome[index] = acquire(manager, kind, database, table, Mode::Exclusive);
            const auto after = manager.captureCheckpoint();
            preserved[index] = outcome[index] ? strong(before, after, kind, Mode::Exclusive) : same(before, after);
            release(manager, kind, database, table, depth);
        });
        for (auto& thread : threads) thread.join();
        check(weak[0] && weak[1], "two real upgrade participants begin as weak owners");
        check(outcome[0] != outcome[1], "upgrade cycle produces one victim and one eventual winner");
        check(preserved[0] && preserved[1], "victim retains old weak ownership until explicit release");
        check(manager.getLockHolds().empty() && manager.getLockWaits().empty(), "upgrade cycle fully cleaned after both callers handle results");
    }
    check(ordinaryCases == 84 && deadlockCases == 6, "all blocked/deadlock upgrade schedules reached");
    std::filesystem::remove_all(directory);
    if (controls != 866) { ++failures; std::cerr << "LOCK_UPGRADE_FAILURE_BAD_REACH " << controls << '\n'; }
    std::cout << "LOCK_UPGRADE_FAILURE_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
