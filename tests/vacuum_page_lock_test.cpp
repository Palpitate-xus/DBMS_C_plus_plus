#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "storage/PageAllocator.h"
#include "storage/PageWrapper.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls = 0, failures = 0, cases = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "VACUUM_PAGE_LOCK_FAIL " << label << '\n'; }
    };
    const std::string database = testDbPath("vacuum_page_lock");
    check(g_engine.createDatabase(database) == DBStatus::OK, "create isolated database");
    auto& locks = g_engine.getLockManager();
    const auto rows = [&](const std::string& table) {
        auto result = g_engine.query(database, table, {}, {"id", "payload"}, {});
        std::sort(result.begin(), result.end()); return result;
    };
    const auto sameLocks = [](const LockManager::LockCheckpoint& a, const LockManager::LockCheckpoint& b) {
        return a.tableCounts == b.tableCounts && a.tableModes == b.tableModes &&
            a.pageLocks == b.pageLocks && a.pageModes == b.pageModes && a.rowLocks == b.rowLocks &&
            a.rowModes == b.rowModes && a.gapCounts == b.gapCounts;
    };
    // Every case has its own real fragmented heap; a previously compacted
    // page must not make later "no modification while locked" checks vacuous.
    const auto exercise = [&](int workers, bool sharedHolder, const std::string& outcome) {
        ++cases;
        const std::string table = "case_" + std::to_string(cases);
        TableSchema schema; schema.tablename = table; schema.formatVersion = 2;
        schema.append(makeIntColumn("id", false, 2, true));
        schema.append(makeVarCharColumn("payload", false, 800, false));
        check(g_engine.createTable(database, schema) == DBStatus::OK, "create actual heap");
        bool inserted = true, deleted = true;
        for (int id = 0; id < 48; ++id)
            inserted &= g_engine.insert(database, table, {{"id", std::to_string(id)},
                {"payload", std::string(400, char('a' + id % 26))}}) == DBStatus::OK;
        check(inserted, "insert all 48 wide records");
        for (int id = 0; id < 48; id += 4)
            deleted &= g_engine.remove(database, table, {"=id " + std::to_string(id)}) == DBStatus::OK;
        // A committed MVCC DELETE only sets xmax; its line pointer remains
        // LP_NORMAL. Use actual INSERT undo, as the existing horizon fixture
        // does, to create LP_UNUSED records that this compactor can reclaim.
        deleted &= g_engine.beginTransaction(database) == DBStatus::OK;
        for (int id = 100; id < 112; ++id)
            deleted &= g_engine.insert(database, table, {{"id", std::to_string(id)},
                {"payload", std::string(400, 'r')}}) == DBStatus::OK;
        deleted &= g_engine.rollbackTransaction() == DBStatus::OK;
        check(deleted, "create genuine unused pointers with real INSERT undo");
        auto* allocator = g_engine.getPageAllocator(database, table);
        check(allocator && allocator->numPages() > 2, "actual multi-page heap admits parallel workers");
        if (!allocator) return;
        const auto originalRows = rows(table);
        check(originalRows.size() == 36, "complete live row fixture");
        uint32_t target = 0;
        for (uint32_t pageId = 1; pageId < allocator->numPages(); ++pageId) {
            auto* bytes = allocator->fetchPage(pageId);
            if (!bytes) continue;
            PageWrapper page(bytes, allocator->pageSize(), schema.formatVersion);
            if (page.liveCount() < page.slotCount()) target = pageId;
            allocator->unpinPage(pageId);
            if (target) break;
        }
        check(target != 0, "target has real compaction work");
        if (!target) return;
        const bool acquired = sharedHolder ? locks.pageLockShared(database, table, target)
                                           : locks.pageLockExclusive(database, table, target);
        check(acquired, "caller owns actual reader/writer page lock");
        if (!acquired) return;
        auto* bytes = allocator->fetchPage(target);
        check(bytes != nullptr, "read actual caller-owned page");
        if (!bytes) { locks.pageUnlock(database, table, target); return; }
        const std::string originalBytes(bytes, allocator->pageSize());
        allocator->unpinPage(target);
        std::atomic<bool> finished{false};
        auto interrupt = std::make_shared<SessionInterruptState>();
        std::string state;
        bool inherited = false, preserved = false;
        std::thread vacuum([&] {
            locks.setResourceNamespace(database);
            locks.setLockTimeout(outcome == "timeout" ? 200 : 0);
            setCurrentQueryInterruptState(interrupt);
            locks.setInterruptHandler([] { checkForQueryInterrupt(); });
            inherited = locks.lockShared(table);
            const auto before = locks.captureCheckpoint();
            try { (void)g_engine.vacuum(database, table, true, workers); }
            catch (const DbError& error) { state = error.sqlState(); }
            catch (const std::exception& error) { state = std::string("exception: ") + error.what(); }
            preserved = sameLocks(before, locks.captureCheckpoint());
            if (inherited) locks.unlock(table);
            locks.clearInterruptHandler();
            setCurrentQueryInterruptState(nullptr);
            finished.store(true, std::memory_order_release);
        });
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!finished.load(std::memory_order_acquire) && locks.getLockWaits().empty() &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        const bool waited = !locks.getLockWaits().empty();
        check(waited, "real wait edge precedes completion while page is owned");
        if (outcome == "cancel") interrupt->cancelRequested.store(true, std::memory_order_release);
        if (outcome != "success") {
            while (!finished.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            check(finished.load(std::memory_order_acquire), "timeout/cancel returns before page-holder releases");
        }
        // Once a real waiter or completed old invocation is observed, this
        // specific page is stable. The wall deadline is a hang guard only.
        bool unchanged = false;
        if (waited || finished.load(std::memory_order_acquire)) {
            bytes = allocator->fetchPage(target);
            unchanged = bytes && std::string(bytes, allocator->pageSize()) == originalBytes;
            if (bytes) allocator->unpinPage(target);
        }
        check(unchanged, "VACUUM never changes bytes under another reader/writer page lock");
        locks.pageUnlock(database, table, target);
        vacuum.join();
        check(state == (outcome == "timeout" ? "55P03" : outcome == "cancel" ? "57014" : ""),
              "precise success/lock-timeout/cancellation result, no worker terminate");
        check(inherited && preserved, "callee releases only its own locks and preserves caller depth/mode");
        check(rows(table) == originalRows, "all 36 exact surviving rows preserved");
        const auto holds = locks.getLockHolds();
        for (const auto& hold : holds)
            std::cout << "VACUUM_PAGE_LOCK_REMAINING " << hold.resource << ' ' << hold.holderTid << ' ' << hold.mode << '\n';
        check(locks.getLockWaits().empty() && holds.empty(), "all worker locks and wait edges released");
    };
    for (int workers : {1, 4}) for (bool shared : {false, true}) exercise(workers, shared, "success");
    for (int workers : {1, 4}) exercise(workers, false, "timeout");
    for (int workers : {1, 4}) exercise(workers, false, "cancel");
    check(cases == 8, "all eight actual serial/parallel/reader/writer/error schedules reached");
    check(g_engine.dropDatabase(database) == DBStatus::OK, "drop private database after cleanup");
    cleanupTestDb("vacuum_page_lock");
    if (controls != 119) { ++failures; std::cerr << "VACUUM_PAGE_LOCK_BAD_REACH " << controls << '\n'; }
    std::cout << "VACUUM_PAGE_LOCK_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
