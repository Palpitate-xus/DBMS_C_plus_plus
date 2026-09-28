#include "TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <chrono>
#include <thread>

int main() {
    const std::string name = "ddl_cache_relation_lock_order";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    dbms::StorageEngine engine;
    assert(engine.createDatabase(db) == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 0, true));
    assert(engine.createTable(db, schema) == dbms::DBStatus::OK);

    auto& locks = engine.getLockManager();
    locks.setResourceNamespace(db);
    assert(locks.lockShared("items"));

    dbms::DBStatus ddlStatus = dbms::DBStatus::OK;
    std::thread ddl([&] {
        locks.setLockTimeout(1000);
        ddlStatus = engine.alterTableAddColumn(
            db, "items", dbms::makeIntColumn("extra", true, 0, false));
    });
    bool ddlWaiting = false;
    const auto waitDeadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(500);
    while (std::chrono::steady_clock::now() < waitDeadline) {
        if (!locks.getLockWaits().empty()) {
            ddlWaiting = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    assert(ddlWaiting);

    bool readerLocked = false;
    std::chrono::milliseconds readerWait{0};
    std::thread reader([&] {
        locks.setResourceNamespace(db);
        const auto start = std::chrono::steady_clock::now();
        readerLocked = locks.lockShared("items");
        if (readerLocked) {
            // Table scans call getCommitLog() while holding their shared
            // relation lock. A waiting DDL must not hold cacheMutex_ here.
            assert(engine.getCommitLog(db) != nullptr);
            locks.unlock("items");
        }
        readerWait = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
    });
    reader.join();
    ddl.join();
    locks.unlock("items");

    assert(readerLocked);
    assert(readerWait < std::chrono::milliseconds(400));
    assert(ddlStatus == dbms::DBStatus::LOCK_CONFLICT);
    cleanupTestDb(name);
}
