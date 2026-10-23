#include "transaction/LockManager.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main() {
    using dbms::LockManager;
    using Mode = LockManager::LockMode;
    size_t controls = 0, failures = 0, cases = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "LOCK_UPGRADE_DEPTH_FAIL " << label << '\n'; }
    };
    const std::string database = "lock_upgrade_depth_private";
    const bool owned = std::filesystem::create_directory(database);
    check(owned, "create private process-lock namespace");
    if (!owned) return 2;
    const auto acquire = [](LockManager& locks, const std::string& table, Mode mode) {
        switch (mode) {
            case Mode::Shared: return locks.lockShared(table);
            case Mode::IntentShared: return locks.lockIntentShared(table);
            case Mode::IntentExclusive: return locks.lockIntentExclusive(table);
            case Mode::Exclusive: return locks.lockExclusive(table);
            case Mode::Metadata: return locks.lockMetadata(table);
        }
        return false;
    };
    std::vector<std::pair<Mode, Mode>> upgrades;
    for (auto before : {Mode::Shared, Mode::IntentShared, Mode::IntentExclusive})
        for (auto after : {Mode::Exclusive, Mode::Metadata}) upgrades.emplace_back(before, after);
    upgrades.emplace_back(Mode::Shared, Mode::IntentExclusive);
    upgrades.emplace_back(Mode::IntentShared, Mode::IntentExclusive);
    for (const auto& modes : upgrades) for (size_t depth : {1U, 2U, 3U}) {
        ++cases;
        LockManager locks;
        locks.setResourceNamespace(database);
        const std::string table = "case_" + std::to_string(cases);
        bool acquired = true;
        for (size_t n = 0; n < depth; ++n) acquired &= acquire(locks, table, modes.first);
        check(acquired, "all original acquisitions succeeded");
        const auto caller = locks.captureCheckpoint();
        check(acquire(locks, table, modes.second), "actual stronger mode acquired");
        auto held = locks.captureCheckpoint();
        check(held.tableCounts.size() == 1 && held.tableCounts.begin()->second == depth &&
              held.tableModes.begin()->second == modes.second, "mode conversion preserves depth without adding a token");
        const auto peer = [&](Mode mode) {
            bool result = false;
            std::thread thread([&] {
                locks.setResourceNamespace(database);
                locks.setLockTimeout(10);
                result = acquire(locks, table, mode);
                if (result) locks.unlock(table);
            });
            thread.join(); return result;
        };
        locks.rollbackToCheckpoint(caller);
        held = locks.captureCheckpoint();
        check(held.tableCounts == caller.tableCounts && held.tableModes == caller.tableModes,
              "checkpoint restores exact caller count and original mode");
        check(peer(Mode::Shared) && !peer(Mode::Exclusive), "original reader compatibility and DDL exclusion restored");
        check(acquire(locks, table, modes.second), "second genuine conversion succeeds");
        locks.unlock(table);
        held = locks.captureCheckpoint();
        const size_t remaining = depth - 1;
        check(remaining == 0 ? held.tableCounts.empty() :
              held.tableCounts.size() == 1 && held.tableCounts.begin()->second == remaining &&
              held.tableModes.begin()->second == modes.second, "one unlock consumes exactly one converted token");
        check(peer(Mode::Exclusive) == (remaining == 0), "real peer DDL allowed only after all original tokens released");
        for (size_t n = 0; n < remaining; ++n) locks.unlock(table);
        check(locks.captureCheckpoint().tableCounts.empty() && locks.getLockHolds().empty() && locks.getLockWaits().empty(),
              "all original tokens and peer wait graph cleaned");
    }
    check(cases == 24, "all eight upgrade pairs at three nesting depths reached");
    std::filesystem::remove_all(database);
    if (controls != 218) { ++failures; std::cerr << "LOCK_UPGRADE_DEPTH_BAD_REACH " << controls << '\n'; }
    std::cout << "LOCK_UPGRADE_DEPTH_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
