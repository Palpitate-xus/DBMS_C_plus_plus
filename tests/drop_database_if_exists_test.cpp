#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    const std::string name = "drop_database_if_exists";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = "info";
    dbms::DdlExecutor ddl;

    std::ostringstream output;
    auto* oldBuffer = std::cout.rdbuf(output.rdbuf());
    const bool missingIfExists =
        ddl.executeSql("DROP DATABASE IF EXISTS " + db, session);
    std::cout.rdbuf(oldBuffer);
    assert(!missingIfExists);
    assert(output.str().find("NOTICE:") != std::string::npos);

    output.str("");
    output.clear();
    oldBuffer = std::cout.rdbuf(output.rdbuf());
    const bool missingWithoutIfExists =
        ddl.executeSql("DROP DATABASE " + db, session);
    std::cout.rdbuf(oldBuffer);
    assert(missingWithoutIfExists);
    assert(output.str().find("SQLSTATE 3D000") != std::string::npos);

    assert(g_engine.createDatabase(db) == dbms::DBStatus::OK);
    assert(ddl.executeSql("DROP DATABASE " + db + " unsupported", session));
    assert(g_engine.databaseExists(db));
    assert(!ddl.executeSql("DROP DATABASE IF EXISTS " + db, session));
    assert(!g_engine.databaseExists(db));
}
