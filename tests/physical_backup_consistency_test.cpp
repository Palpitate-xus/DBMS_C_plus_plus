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

    const std::string database = testDbPath("backup_consistency_source");
    const std::string backup = testDbPath("backup_consistency_image");
    const std::string restored = testDbPath("backup_consistency_restored");
    const std::string rejected = testDbPath("backup_consistency_rejected");

    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "t";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, table) == dbms::DBStatus::OK);

    // The same backend must not snapshot its own uncommitted physical state
    // or deadlock trying to upgrade its transaction's shared database lock.
    assert(engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(engine.insert(database, "t", {{"id", "0"}}) ==
           dbms::DBStatus::OK);
    assert(!engine.physicalBackup(database, rejected));
    assert(engine.rollbackTransaction() == dbms::DBStatus::OK);

    std::atomic<bool> writerReady{false};
    std::atomic<bool> allowCommit{false};
    std::atomic<bool> backupStarted{false};
    std::atomic<bool> backupDone{false};
    bool backupResult = false;

    std::thread writer([&] {
        assert(engine.beginTransaction(database) == dbms::DBStatus::OK);
        assert(engine.insert(database, "t", {{"id", "1"}}) ==
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

    std::thread backupThread([&] {
        backupStarted.store(true, std::memory_order_release);
        backupResult = engine.physicalBackup(database, backup);
        backupDone.store(true, std::memory_order_release);
    });
    while (!backupStarted.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(!backupDone.load(std::memory_order_acquire));

    allowCommit.store(true, std::memory_order_release);
    writer.join();
    backupThread.join();
    assert(backupResult);

    dbms::StorageEngine restoredEngine;
    assert(restoredEngine.physicalRestore(restored, backup));
    assert(restoredEngine.tableExists(restored, "t"));
    assert(rowCount(restoredEngine, restored) == 1);

    finalCleanupTestData();
    std::cout << "[PHYSICAL BACKUP CONSISTENCY] transaction snapshot OK\n";
    return 0;
}
