#include "TableManage.h"
#include "NetworkServer.h"
#include "test_utils.h"

#include <cassert>

extern dbms::StorageEngine g_engine;

int main() {
    const std::string name = "database_drop_connection_registry";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    assert(g_engine.createDatabase(db) == dbms::DBStatus::OK);

    const auto active = dbms::registerProcess("alice", "local", db);
    assert(active.pid != 0);
    assert(!dbms::reserveDatabaseDrop(db));
    dbms::unregisterProcess(active.pid);

    assert(dbms::reserveDatabaseDrop(db));
    assert(!dbms::reserveDatabaseDrop(db));
    assert(dbms::registerProcess("bob", "local", db).pid == 0);
    assert(!dbms::trySwitchProcessDb(0, db));
    dbms::releaseDatabaseDrop(db);

    assert(dbms::trySwitchProcessDb(0, db));
    const auto after = dbms::registerProcess("bob", "local", db);
    assert(after.pid != 0);
    dbms::unregisterProcess(after.pid);
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    assert(dbms::registerProcess("bob", "local", db).pid == 0);
}
