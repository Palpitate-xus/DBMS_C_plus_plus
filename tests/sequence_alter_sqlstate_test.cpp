#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "sequence_alter_sqlstate", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username = "admin"; session.permission = 1; session.currentDB = db;
    DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE SEQUENCE descending_default INCREMENT -2 NO MINVALUE NO MAXVALUE", session));
    assert(g_engine.nextval(db, "descending_default") == -1);
    assert(g_engine.nextval(db, "descending_default") == -3);
    std::ostringstream output;
    struct RestoreOutput {
        std::streambuf* previous;
        ~RestoreOutput() { std::cout.rdbuf(previous); }
    };
    bool failed = false;
    {
        RestoreOutput restore{std::cout.rdbuf(output.rdbuf())};
        failed = ddl.executeSql(
            "ALTER SEQUENCE descending_default INCREMENT 2 START 1 RESTART", session);
    }
    std::cout << "ALTER STATE OUTPUT " << output.str() << std::flush;
    assert(failed);
    assert(output.str().find("SQLSTATE 22023") != std::string::npos);
    assert(g_engine.nextval(db, "descending_default") == -5);
    SequenceInfo unchanged;
    assert(g_engine.getSequenceInfo(db, "descending_default", unchanged) == DBStatus::OK);
    assert(unchanged.increment == -2 && unchanged.maxValue == -1);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[SEQUENCE ALTER SQLSTATE] passed\n";
}
