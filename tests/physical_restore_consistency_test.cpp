#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

namespace {

size_t rowCount(dbms::StorageEngine& engine, const std::string& database) {
    size_t rows = 0;
    assert(engine.forEachRow(
        database, "t",
        [&](uint32_t, uint16_t, const char*, size_t) { ++rows; }));
    return rows;
}

}  // namespace

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string database = testDbPath("restore_consistency_db");
    const std::string backup = testDbPath("restore_consistency_image");

    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "t";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(engine.insert(database, "t", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(engine.physicalBackup(database, backup));

    // Refuse an unsafe lock upgrade and never replace files containing the
    // caller's own uncommitted state.
    assert(engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(engine.insert(database, "t", {{"id", "2"}}) ==
           dbms::DBStatus::OK);
    assert(!engine.physicalRestore(database, backup));
    assert(engine.rollbackTransaction() == dbms::DBStatus::OK);

    std::atomic<bool> writerReady{false};
    std::atomic<bool> allowCommit{false};
    std::atomic<bool> restoreStarted{false};
    std::atomic<bool> restoreDone{false};
    bool restoreResult = false;

    std::thread writer([&] {
        assert(engine.beginTransaction(database) == dbms::DBStatus::OK);
        assert(engine.insert(database, "t", {{"id", "3"}}) ==
               dbms::DBStatus::OK);
        writerReady.store(true, std::memory_order_release);
        while (!allowCommit.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        assert(engine.commitTransaction() == dbms::DBStatus::OK);
    });
    while (!writerReady.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    std::thread restoreThread([&] {
        restoreStarted.store(true, std::memory_order_release);
        restoreResult = engine.physicalRestore(database, backup);
        restoreDone.store(true, std::memory_order_release);
    });
    while (!restoreStarted.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(!restoreDone.load(std::memory_order_acquire));

    allowCommit.store(true, std::memory_order_release);
    writer.join();
    restoreThread.join();
    assert(restoreResult);

    // Reuse the same engine instance. Any allocator or schema cache still
    // attached to the displaced directory would expose the pre-restore rows.
    assert(engine.tableExists(database, "t"));
    assert(rowCount(engine, database) == 1);

    finalCleanupTestData();
    std::cout << "[PHYSICAL RESTORE CONSISTENCY] transaction and cache boundary OK\n";
    return 0;
}
