#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_create_function_single_param() {
    std::string db = "func_single";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE FUNCTION inc(x int) RETURNS int AS 'x + 1' LANGUAGE sql", s));
    assert(g_engine.udfExists(db, "inc"));
    auto info = g_engine.getUDF(db, "inc");
    assert(info.expression == "x + 1");

    cleanup(db);
    std::cout << "[FUNCTION] single param OK" << std::endl;
}

static void test_create_function_multi_param() {
    std::string db = "func_multi";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE FUNCTION add(a int, b int) RETURNS int AS 'a + b' LANGUAGE sql", s));
    assert(g_engine.udfExists(db, "add"));
    auto info = g_engine.getUDF(db, "add");
    assert(info.paramNames.size() == 2);

    cleanup(db);
    std::cout << "[FUNCTION] multi param OK" << std::endl;
}

static void test_create_tvf() {
    std::string db = "func_tvf";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))", s));
    assert(!ddl.executeSql("CREATE FUNCTION get_t() RETURNS TABLE AS 'SELECT * FROM t' LANGUAGE sql", s));
    assert(g_engine.tvfExists(db, "get_t"));
    assert(!g_engine.getTVFSQL(db, "get_t").empty());

    cleanup(db);
    std::cout << "[FUNCTION] table-valued OK" << std::endl;
}

static void test_create_procedure() {
    std::string db = "proc_basic";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY)", s));
    assert(!ddl.executeSql("CREATE PROCEDURE ins() AS 'insert into t values (1); insert into t values (2)' LANGUAGE sql", s));
    auto stmts = g_engine.getProcedureStatements(db, "ins");
    assert(stmts.size() == 2);

    cleanup(db);
    std::cout << "[PROCEDURE] basic OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_function_single_param();
    test_create_function_multi_param();
    test_create_tvf();
    test_create_procedure();
    std::cout << "[FUNCTION/PROCEDURE] all passed" << std::endl;
    return 0;
}
