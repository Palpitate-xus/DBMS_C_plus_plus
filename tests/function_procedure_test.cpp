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
    assert(info.returnType == "int");
    assert(info.paramTypes == std::vector<std::string>{"int"});
    std::string result;
    bool resultIsNull = false;
    assert(g_engine.callUDF(db, "inc", {"4"}, result, &resultIsNull));
    assert(!resultIsNull && result == "5");

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
    assert(info.language == "sql");
    assert(info.returnType == "int");
    std::string result;
    bool resultIsNull = false;
    assert(g_engine.callUDF(db, "add", {"20", "22"}, result,
                            &resultIsNull));
    assert(!resultIsNull && result == "42");

    cleanup(db);
    std::cout << "[FUNCTION] multi param OK" << std::endl;
}

static void test_function_signature_validation() {
    std::string db = testDbPath("func_signature_validation");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(ddl.executeSql(
        "CREATE FUNCTION bad_return() RETURNS no_such_type "
        "LANGUAGE sql AS 'SELECT 1'", s));
    assert(!g_engine.udfExists(db, "bad_return"));
    assert(ddl.executeSql(
        "CREATE FUNCTION bad_parameter(x no_such_type) RETURNS int "
        "LANGUAGE sql AS 'SELECT 1'", s));
    assert(!g_engine.udfExists(db, "bad_parameter"));
    assert(ddl.executeSql(
        "CREATE FUNCTION bad_table_parameter(x no_such_type) RETURNS TABLE "
        "LANGUAGE sql AS 'SELECT 1'", s));
    assert(!g_engine.tvfExists(db, "bad_table_parameter"));
    assert(ddl.executeSql(
        "CREATE FUNCTION bad_language() RETURNS int "
        "LANGUAGE python AS 'SELECT 1'", s));
    assert(!g_engine.udfExists(db, "bad_language"));

    cleanup(db);
    std::cout << "[FUNCTION] signature validation OK" << std::endl;
}

static void test_create_or_replace_function() {
    std::string db = testDbPath("func_replace");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE FUNCTION replace_me(x int) RETURNS int "
        "LANGUAGE sql AS 'SELECT x + 1'", s));
    assert(!ddl.executeSql(
        "CREATE OR REPLACE FUNCTION replace_me(x integer) RETURNS integer "
        "STRICT LANGUAGE sql AS 'SELECT x + 2'", s));
    auto info = g_engine.getUDF(db, "replace_me");
    assert(info.strict && info.expression == "SELECT x + 2");
    std::string result;
    bool resultIsNull = false;
    assert(g_engine.callUDF(db, "replace_me", {"3"}, result,
                            &resultIsNull));
    assert(!resultIsNull && result == "5");

    assert(ddl.executeSql(
        "CREATE OR REPLACE FUNCTION replace_me(x bigint) RETURNS integer "
        "LANGUAGE sql AS 'SELECT 99'", s));
    assert(ddl.executeSql(
        "CREATE OR REPLACE FUNCTION replace_me(x integer) RETURNS text "
        "LANGUAGE sql AS 'SELECT 99'", s));
    assert(g_engine.getUDF(db, "replace_me").expression == "SELECT x + 2");

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE OR REPLACE FUNCTION replace_me(x integer) RETURNS integer "
        "LANGUAGE sql AS 'SELECT x + 10'", s));
    assert(g_engine.getUDF(db, "replace_me").expression == "SELECT x + 10");
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.getUDF(db, "replace_me").expression == "SELECT x + 2");

    cleanup(db);
    std::cout << "[FUNCTION] create or replace OK" << std::endl;
}

static void test_routine_parameter_metadata() {
    std::string db = testDbPath("routine_parameter_metadata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    assert(g_engine.createUDF(
        db, "typed_udf", std::vector<std::string>{"amount", "label"},
        std::vector<std::string>{"numeric(10,2)", "varchar(20)"},
        "SELECT amount", 'v', "sql", "numeric(10,2)") ==
        dbms::DBStatus::OK);
    const auto udf = g_engine.getUDF(db, "typed_udf");
    assert(udf.paramNames ==
           std::vector<std::string>({"amount", "label"}));
    assert(udf.paramTypes ==
           std::vector<std::string>({"numeric(10,2)", "varchar(20)"}));

    assert(g_engine.createUDF(
        db, "zero_arg", "", "SELECT 1", 'v', "sql", "int") ==
        dbms::DBStatus::OK);
    assert(g_engine.getUDF(db, "zero_arg").paramNames.empty());

    std::vector<dbms::StorageEngine::ProcParam> params = {
        {"amount", "IN", "numeric(10,2)"},
        {"label", "IN", "varchar(20)"},
    };
    assert(g_engine.createProcedure(db, "typed_proc", params, {"SELECT 1"}) ==
           dbms::DBStatus::OK);
    const auto storedParams = g_engine.getProcedureParams(db, "typed_proc");
    assert(storedParams.size() == 2);
    assert(storedParams[0].name == "amount" &&
           storedParams[0].mode == "IN" &&
           storedParams[0].type == "numeric(10,2)");
    assert(storedParams[1].name == "label" &&
           storedParams[1].type == "varchar(20)");

    // Existing sidecars must remain readable after the unambiguous V2 format
    // becomes the writer default.
    {
        std::ofstream legacy(
            std::filesystem::path(db) / ".funcs" / "legacy_udf.func");
        legacy << "legacy_arg\nSELECT legacy_arg\nv\nsql\n"
                  "RETURNS:text\nSTRICT:0\n";
    }
    const auto legacyUdf = g_engine.getUDF(db, "legacy_udf");
    assert(legacyUdf.paramNames ==
           std::vector<std::string>({"legacy_arg"}));
    {
        std::ofstream legacy(
            std::filesystem::path(db) / ".procs" / "legacy_proc.proc");
        legacy << "PARAMS:x:IN:int,y:IN:text\nSELECT 1\n";
    }
    const auto legacyProcedure =
        g_engine.getProcedureParams(db, "legacy_proc");
    assert(legacyProcedure.size() == 2);
    assert(legacyProcedure[0].name == "x" &&
           legacyProcedure[0].type == "int");
    assert(legacyProcedure[1].name == "y" &&
           legacyProcedure[1].type == "text");

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE PROCEDURE decimal_proc(x numeric(10,2), y int) "
        "LANGUAGE sql AS 'SELECT 1'", s));
    assert(!ddl.executeSql(
        "CREATE OR REPLACE PROCEDURE decimal_proc(x numeric(10,2), y integer) "
        "LANGUAGE sql AS 'SELECT 2'", s));
    assert(g_engine.getProcedureParams(db, "decimal_proc").size() == 2);

    cleanup(db);
    std::cout << "[FUNCTION/PROCEDURE] parameter metadata OK" << std::endl;
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

    assert(!ddl.executeSql(
        "CREATE PROCEDURE replace_proc(x int) LANGUAGE sql "
        "AS 'SELECT ?x'", s));
    assert(!ddl.executeSql(
        "CREATE OR REPLACE PROCEDURE replace_proc(x integer) "
        "AS 'SELECT 2' LANGUAGE sql", s));
    assert(g_engine.getProcedureStatements(db, "replace_proc") ==
           std::vector<std::string>{"SELECT 2"});
    assert(ddl.executeSql(
        "CREATE OR REPLACE PROCEDURE replace_proc(x bigint) "
        "LANGUAGE sql AS 'SELECT 3'", s));
    assert(ddl.executeSql(
        "CREATE OR REPLACE PROCEDURE replace_proc(x integer) "
        "LANGUAGE plpgsql AS 'BEGIN NULL; END'", s));
    assert(g_engine.getProcedureStatements(db, "replace_proc") ==
           std::vector<std::string>{"SELECT 2"});

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE OR REPLACE PROCEDURE replace_proc(x int) "
        "LANGUAGE sql AS 'SELECT 4'", s));
    assert(g_engine.getProcedureStatements(db, "replace_proc") ==
           std::vector<std::string>{"SELECT 4"});
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.getProcedureStatements(db, "replace_proc") ==
           std::vector<std::string>{"SELECT 2"});

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

    assert(!ddl.executeSql(
        "CREATE FUNCTION strict_add1(x int) RETURNS int STRICT "
        "AS 'x + 1' LANGUAGE sql", s));
    auto strict_info = g_engine.getUDF(db, "strict_add1");
    assert(strict_info.strict);
    std::string result;
    bool resultIsNull = false;
    const std::vector<bool> nullArg{true};
    assert(g_engine.callUDF(db, "strict_add1", {""}, result,
                            &resultIsNull, &nullArg));
    assert(resultIsNull && result.empty());

    cleanup(db);
    std::cout << "[FUNCTION] volatility persistence OK" << std::endl;
}

static void test_builtin_volatility() {
    dbms::ExprEvaluator eval;
    assert(eval.volatility("abs") == 'i');
    assert(eval.volatility("length") == 'i');
    assert(eval.volatility("now") == 's');
    assert(eval.volatility("current_date") == 's');
    assert(eval.volatility("current_timestamp") == 's');
    assert(eval.volatility("transaction_timestamp") == 's');
    assert(eval.volatility("statement_timestamp") == 's');
    assert(eval.volatility("clock_timestamp") == 'v');
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
    test_function_signature_validation();
    test_create_or_replace_function();
    test_routine_parameter_metadata();
    test_create_function_volatility();
    test_builtin_volatility();
    test_create_tvf();
    test_create_procedure();
    test_metadata_sidecar_failures_and_duplicates();
    std::cout << "[FUNCTION/PROCEDURE] all passed" << std::endl;
    return 0;
}
