#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    const std::string name = "drop_database_current_connection";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    assert(g_engine.createDatabase(db) == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;

    std::ostringstream output;
    auto* oldBuffer = std::cout.rdbuf(output.rdbuf());
    const bool rejected = ddl.executeSql("DROP DATABASE " + db, session);
    std::cout.rdbuf(oldBuffer);
    assert(rejected);
    assert(output.str().find("SQLSTATE 55006") != std::string::npos);
    assert(session.currentDB == db);
    assert(g_engine.databaseExists(db));

    session.currentDB = "info";
    assert(!ddl.executeSql("DROP DATABASE " + db, session));
    assert(!g_engine.databaseExists(db));
}
