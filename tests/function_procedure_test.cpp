#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_create_function_single_param() {
    std::string db = testDbPath("func_single");
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
    std::string db = testDbPath("func_multi");
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
    std::string db = testDbPath("func_tvf");
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
    std::string db = testDbPath("proc_basic");
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

static void test_create_function_volatility() {
    std::string db = testDbPath("func_volatile");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE FUNCTION immutable_add1(x int) RETURNS int IMMUTABLE AS 'x + 1' LANGUAGE sql", s));
    auto info_i = g_engine.getUDF(db, "immutable_add1");
    assert(info_i.provolatile == 'i');

    assert(!ddl.executeSql("CREATE FUNCTION stable_add1(x int) RETURNS int STABLE AS 'x + 1' LANGUAGE sql", s));
    auto info_s = g_engine.getUDF(db, "stable_add1");
    assert(info_s.provolatile == 's');

    assert(!ddl.executeSql("CREATE FUNCTION volatile_add1(x int) RETURNS int AS 'x + 1' LANGUAGE sql", s));
    auto info_v = g_engine.getUDF(db, "volatile_add1");
    assert(info_v.provolatile == 'v');

    cleanup(db);
    std::cout << "[FUNCTION] volatility persistence OK" << std::endl;
}

static void test_builtin_volatility() {
    dbms::ExprEvaluator eval;
    assert(eval.volatility("abs") == 'i');
    assert(eval.volatility("length") == 'i');
    assert(eval.volatility("now") == 's');
    assert(eval.volatility("random") == 'v');
    assert(eval.volatility("nextval") == 'v');
    assert(eval.volatility("unknown_func") == 'v');
    std::cout << "[FUNCTION] builtin volatility OK" << std::endl;
}

static void test_metadata_sidecar_failures_and_duplicates() {
    std::string db = testDbPath("func_metadata_failures");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    // A sidecar path occupied by a regular file must never be treated as a
    // successful object definition.
    {
        std::ofstream blocker(std::filesystem::path(db) / ".views");
        blocker << "not a directory";
    }
    assert(g_engine.createView(db, "blocked_view", "SELECT 1") ==
           dbms::DBStatus::IO_ERROR);
    std::filesystem::remove(std::filesystem::path(db) / ".views");

    {
        std::ofstream blocker(std::filesystem::path(db) / ".funcs");
        blocker << "not a directory";
    }
    assert(g_engine.createUDF(db, "blocked_function", "x", "x + 1") ==
           dbms::DBStatus::IO_ERROR);
    std::filesystem::remove(std::filesystem::path(db) / ".funcs");

    {
        std::ofstream blocker(std::filesystem::path(db) / ".tvf");
        blocker << "not a directory";
    }
    assert(g_engine.createTVF(db, "blocked_tvf", "x", "SELECT x") ==
           dbms::DBStatus::IO_ERROR);
    std::filesystem::remove(std::filesystem::path(db) / ".tvf");

    {
        std::ofstream blocker(std::filesystem::path(db) / ".procs");
        blocker << "not a directory";
    }
    assert(g_engine.createProcedure(db, "blocked_procedure", {}, {"SELECT 1"}) ==
           dbms::DBStatus::IO_ERROR);
    std::filesystem::remove(std::filesystem::path(db) / ".procs");

    assert(g_engine.createView(db, "duplicate_view", "SELECT 1") ==
           dbms::DBStatus::OK);
    assert(g_engine.createView(db, "duplicate_view", "SELECT 2") ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(g_engine.getViewSQL(db, "duplicate_view") == "SELECT 1");
    assert(g_engine.createUDF(db, "duplicate_function", "x", "x + 1") ==
           dbms::DBStatus::OK);
    assert(g_engine.createUDF(db, "duplicate_function", "x", "x + 2") ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(g_engine.getUDF(db, "duplicate_function").expression == "x + 1");
    assert(g_engine.createTVF(db, "duplicate_tvf", "x", "SELECT x") ==
           dbms::DBStatus::OK);
    assert(g_engine.createTVF(db, "duplicate_tvf", "x", "SELECT x + 1") ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(g_engine.getTVFSQL(db, "duplicate_tvf") == "SELECT x");
    assert(g_engine.createProcedure(db, "duplicate_procedure", {}, {"SELECT 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createProcedure(db, "duplicate_procedure", {}, {"SELECT 2"}) ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(g_engine.getProcedureStatements(db, "duplicate_procedure") ==
           std::vector<std::string>{"SELECT 1"});

    cleanup(db);
    std::cout << "[FUNCTION/PROCEDURE] metadata failure and duplicate guards OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_function_single_param();
    test_create_function_multi_param();
    test_create_function_volatility();
    test_builtin_volatility();
    test_create_tvf();
    test_create_procedure();
    test_metadata_sidecar_failures_and_duplicates();
    std::cout << "[FUNCTION/PROCEDURE] all passed" << std::endl;
    return 0;
}
