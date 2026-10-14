#include "commands/TableManage.h"
#include "storage/PageAllocator.h"
#include "storage/BufferPool.h"
#include "test_utils.h"
#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    engine.setBackgroundIntervals(5, 60000);
    const auto name = "schema_background_read_owner";
    const auto database = testDbPath(name);
    cleanupTestDb(name);
    assert(engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "dirty_source";
    table.append(makeIntColumn("id", true, 2, true));
    assert(engine.createTable(database, table) == DBStatus::OK);
    assert(engine.insertRow(database, table.tablename, {{"id", "1"}}) == DBStatus::OK);
    assert(engine.beginTransaction(database, true) == DBStatus::OK);
    auto* allocator = engine.getPageAllocator(database, table.tablename);
    assert(allocator && allocator->bufferPool());
    auto* pool = allocator->bufferPool();
    assert(pool->fetchPage(1));
    pool->markDirty(1);  // Identical bytes, genuine pending physical frame.
    pool->unpinPage(1);
    engine.wakeBackgroundWorker();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    // The schema/domain files are owned by this DDL-capable transaction.
    // A worker must not read those physical files through a warm model and
    // then consume its pending dirty frame while that ownership is held.
    assert(pool->isPageDirty(1));
    assert(engine.commitTransaction() == DBStatus::OK);
    engine.wakeBackgroundWorker();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (pool->isPageDirty(1) && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(!pool->isPageDirty(1));
    assert(engine.query(database, table.tablename, {}, {"id"}) == std::vector<std::string>{"1 "});
    assert(engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[SCHEMA BACKGROUND OWNER] actual pending frame deferred and retried after DDL owner COMMIT\n";
}
