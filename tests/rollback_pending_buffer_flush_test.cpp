#include "commands/TableManage.h"
#include "common/DbError.h"
#include "storage/PageAllocator.h"
#include "test_utils.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const auto pendingDb = testDbPath("rollback_pending_buffer");
    const auto healthyDb = testDbPath("rollback_healthy_buffer");
    TableSchema table;
    table.tablename = "rows";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 2, true));
    for (const auto& database : {pendingDb, healthyDb}) {
        assert(g_engine.createDatabase(database) == DBStatus::OK);
        assert(g_engine.createTable(database, table) == DBStatus::OK);
        assert(g_engine.insert(database, "rows", {{"id", "1"}}) == DBStatus::OK);
    }

    assert(g_engine.beginTransaction(pendingDb) == DBStatus::OK);
    assert(g_engine.insert(pendingDb, "rows", {{"id", "2"}}) == DBStatus::OK);
    bool healthyUndo = false;
    g_engine.registerDdlUndo([&] { return healthyUndo; });
    assert(g_engine.rollbackTransaction() == DBStatus::IO_ERROR);
    assert(g_engine.inTransaction());

    PageAllocator* healthyAllocator = nullptr;
    std::thread healthyBackend([&] {
        assert(g_engine.insert(healthyDb, "rows", {{"id", "2"}}) == DBStatus::OK);
        healthyAllocator = g_engine.getPageAllocator(healthyDb, "rows");
        assert(healthyAllocator && healthyAllocator->fetchPage(1));
        // Mark an unchanged, valid production page dirty through the normal
        // API. The pin proves it stays dirty until this backend releases it;
        // no page bytes, worker intervals, or checkpoint settings are changed.
        healthyAllocator->markDirty(1);
        assert(healthyAllocator->bufferPool()->isPageDirty(1));
        healthyAllocator->unpinPage(1);
        g_engine.endBackendSession();
    });
    healthyBackend.join();
    for (int attempt = 0; attempt < 100 &&
         healthyAllocator->bufferPool()->isPageDirty(1); ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    assert(!healthyAllocator->bufferPool()->isPageDirty(1));
    assert(g_engine.inTransaction());
    bool rejected = false;
    try { (void)g_engine.getTableSchema(pendingDb, "rows"); }
    catch (const DbError& error) { rejected = error.sqlState() == "58030"; }
    assert(rejected);

    healthyUndo = true;
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(!g_engine.inTransaction());
    assert(g_engine.query(pendingDb, "rows", {}, {"id"}) == std::vector<std::string>{"1 "});
    assert(g_engine.query(healthyDb, "rows", {}, {"id"}) ==
           (std::vector<std::string>{"1 ", "2 "}));
    assert(g_engine.dropDatabase(pendingDb) == DBStatus::OK);
    assert(g_engine.dropDatabase(healthyDb) == DBStatus::OK);
    std::cout << "[ROLLBACK BUFFER] pending database skipped; healthy valid page written back\n";
}
