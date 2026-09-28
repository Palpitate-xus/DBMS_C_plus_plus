#include "TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <thread>

int main() {
    const std::string name = "drop_database_active_transaction";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    dbms::StorageEngine engine;
    assert(engine.createDatabase(db) == dbms::DBStatus::OK);
    assert(engine.beginTransaction(db) == dbms::DBStatus::OK);

    // A transaction in this thread must not attempt to upgrade its own
    // database-wide shared lock, nor permit the directory to be removed.
    assert(engine.dropDatabase(db) == dbms::DBStatus::DATABASE_IN_USE);
    assert(dbms::sqlstateForDBStatus(dbms::DBStatus::DATABASE_IN_USE) == "55006");
    assert(engine.databaseExists(db));

    dbms::DBStatus concurrentDrop = dbms::DBStatus::OK;
    std::thread dropper([&] { concurrentDrop = engine.dropDatabase(db); });
    dropper.join();
    assert(concurrentDrop == dbms::DBStatus::DATABASE_IN_USE);
    assert(engine.databaseExists(db));

    assert(engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(engine.dropDatabase(db) == dbms::DBStatus::OK);
    assert(!engine.databaseExists(db));
}
