#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;
int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "routine_array_signature";
    cleanupTestDb(name);
    const auto database = testDbPath(name);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser"; session.permission = 1; session.currentDB = database;
    dbms::setCurrentSession(&session);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE FUNCTION f(p INT[]) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 77; END$$", session));
    auto stored = g_engine.getUDF(database, "f");
    assert(stored.paramTypes.size() == 1 &&
           dbms::ExprHelper::canonicalResultTypeName(stored.paramTypes[0]) == "integer[]");
    const auto reject = [&](const std::string& sql, const char* state) {
        std::ostringstream diagnostic;
        auto* previous = std::cout.rdbuf(diagnostic.rdbuf());
        const bool failed = ddl.executeSql(sql, session);
        std::cout.rdbuf(previous);
        std::cout << "ROUTINE_ARRAY_REJECT " << diagnostic.str();
        assert(failed && diagnostic.str().find(state) != std::string::npos);
    };
    reject("CREATE OR REPLACE FUNCTION f(p INT) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 88; END$$", "42P13");
    assert(g_engine.getUDF(database, "f").expression == stored.expression);
    reject("CREATE OR REPLACE FUNCTION f(p TEXT[]) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 88; END$$", "42P13");
    reject("CREATE OR REPLACE FUNCTION f(p INT[]) RETURNS INT[] LANGUAGE plpgsql AS $$BEGIN RETURN p; END$$", "42P13");
    assert(!ddl.executeSql("CREATE OR REPLACE FUNCTION f(p INTEGER[][]) RETURNS INTEGER LANGUAGE plpgsql AS $$BEGIN RETURN 78; END$$", session));
    assert(dbms::ExprHelper::canonicalResultTypeName(g_engine.getUDF(database, "f").paramTypes[0]) == "integer[]");
    reject("CREATE FUNCTION invalid_array(p imaginary[]) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 1; END$$", "42704");
    reject("CREATE FUNCTION invalid_void_array(p VOID[]) RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 1; END$$", "42704");
    assert(!g_engine.udfExists(database, "invalid_array") && !g_engine.udfExists(database, "invalid_void_array"));
    assert(!ddl.executeSql("DROP FUNCTION f", session));
    dbms::setCurrentSession(nullptr);
    cleanupTestDb(name); finalCleanupTestData();
    std::cout << "[ROUTINE ARRAY SIGNATURE] passed\n";
}
