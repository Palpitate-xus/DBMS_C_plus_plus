// Storage-layer lock failures must be returned to callers instead of being
// swallowed after the LockManager has detected a timeout or deadlock.

#include "TableManage.h"
#include "test_utils.h"

#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>

int main() {
    const std::string db = testDbPath("lock_failure_propagation");
    cleanupTestDb("lock_failure_propagation");

    dbms::StorageEngine engine;
    assert(engine.createDatabase(db) == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 0, true));
    assert(engine.createTable(db, table) == dbms::DBStatus::OK);

    assert(engine.getLockManager().lockExclusive("items"));

    std::atomic<dbms::DBStatus> createIndexStatus{dbms::DBStatus::OK};
    std::thread ddl([&] {
        engine.getLockManager().setLockTimeout(50);
        createIndexStatus = engine.createIndex(db, "items", "id");
    });
    ddl.join();

    assert(createIndexStatus.load() == dbms::DBStatus::LOCK_CONFLICT);
    assert(engine.getLockManager().getLockWaits().empty());
    engine.getLockManager().unlock("items");
    assert(engine.getLockManager().getLockHolds().empty());

    cleanupTestDb("lock_failure_propagation");
    std::cout << "[LOCK PROPAGATION] storage call site fails closed OK\n";
    return 0;
}
