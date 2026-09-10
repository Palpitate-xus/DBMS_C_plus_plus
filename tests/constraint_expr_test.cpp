#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "expression/expr_helper.h"
#include <cassert>
#include <filesystem>
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

static std::string trimRight(const std::string& s) {
    size_t end = s.find_last_not_of(" \t\n\r");
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

static void test_expr_helper_basic() {
    auto r = dbms::ExprHelper::evalString("1 + 2 * 3", {});
    assert(r.ok);
    assert(r.value == "7");

    r = dbms::ExprHelper::evalString("'hello' || ' world'", {});
    assert(r.ok);
    assert(r.value == "hello world");

    r = dbms::ExprHelper::evalString("length('abc')", {});
    assert(r.ok);
    assert(r.value == "3");

    r = dbms::ExprHelper::evalString("sum('abc')", {});
    assert(!r.ok && r.error.find("SQLSTATE 42725") != std::string::npos);
    r = dbms::ExprHelper::evalString("avg(NULL)", {});
    assert(!r.ok && r.error.find("SQLSTATE 42725") != std::string::npos);
    r = dbms::ExprHelper::evalString("sum('abc'::text)", {});
    assert(!r.ok && r.error.find("SQLSTATE 42883") != std::string::npos);

    std::map<std::string, std::string> row = {{"x", "10"}, {"y", "20"}};
    std::map<std::string, std::string> types = {{"x", "int4"}, {"y", "int4"}};
    r = dbms::ExprHelper::evalString("x + y", row, types);
    assert(r.ok);
    assert(r.value == "30");

    std::string err;
    bool ok = dbms::ExprHelper::evalBool("x > 5 AND y < 50", row, types, &err);
    assert(ok);

    ok = dbms::ExprHelper::evalBool("x + y = 25", row, types, &err);
    assert(!ok);

    std::cout << "[EXPR_HELPER] basic OK" << std::endl;
}

static void test_default_literal() {
    std::string db = testDbPath("constraints_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, msg VARCHAR(50) DEFAULT 'hello')", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"msg"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "hello");

    cleanup(db);
    std::cout << "[DEFAULT] literal OK" << std::endl;
}

static void test_default_expression() {
    std::string db = testDbPath("constraints_t2");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, n INT DEFAULT 10 + 5)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"n"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "15");

    cleanup(db);
    std::cout << "[DEFAULT] expression OK" << std::endl;
}

static void test_generated_column() {
    std::string db = testDbPath("constraints_t3");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"a", "3"}, {"b", "4"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"c"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "7");

    cleanup(db);
    std::cout << "[GENERATED] column OK" << std::endl;
}

static void test_check_constraint_insert() {
    std::string db = testDbPath("constraints_t4");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT CHECK (price > 0))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"price", "0"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"price", "-5"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id", "price"});
    assert(rows.size() == 1);

    cleanup(db);
    std::cout << "[CHECK] insert OK" << std::endl;
}

static void test_check_constraint_update() {
    std::string db = testDbPath("constraints_t5");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT CHECK (price BETWEEN 1 AND 100))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"price", "50"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"price", "200"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"price"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "50");

    cleanup(db);
    std::cout << "[CHECK] update OK" << std::endl;
}

static void test_generated_identity() {
    std::string db = testDbPath("constraints_t6");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, msg VARCHAR(50))", s));
    assert(g_engine.insert(db, "t", {{"msg", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"msg", "b"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {}, {"id"});
    assert(rows.size() == 2);
    // IDs should be auto-incremented starting from 1.
    assert(trimRight(rows[0]) == "1" || trimRight(rows[0]) == "2");

    cleanup(db);
    std::cout << "[GENERATED] identity OK" << std::endl;
}

int main() {
    const std::map<std::string, std::string> resultTypes = {
        {"i", "integer"}, {"n", "numeric"}, {"d", "date"},
        {"v", "varchar"}, {"b", "blob"}
    };
    assert(dbms::ExprHelper::inferResultType("i + 1", resultTypes) == "integer");
    assert(dbms::ExprHelper::inferResultType("n * 2", resultTypes) == "numeric");
    assert(dbms::ExprHelper::inferResultType("i BETWEEN 1 AND 2", resultTypes) == "boolean");
    assert(dbms::ExprHelper::inferResultType("i IS DISTINCT FROM n", resultTypes) == "boolean");
    assert(dbms::ExprHelper::inferResultType("CAST(n AS integer)", resultTypes) == "integer");
    assert(dbms::ExprHelper::inferResultType("d + 1", resultTypes) == "date");
    assert(dbms::ExprHelper::inferResultType(
               "d - DATE '2024-01-01'", resultTypes) == "integer");
    assert(dbms::ExprHelper::inferResultType("length(v || 'x')", resultTypes) == "integer");
    assert(dbms::ExprHelper::inferResultType("b || b", resultTypes) == "bytea");
    assert(dbms::ExprHelper::inferResultType("reverse(b)", resultTypes) == "bytea");
    assert(dbms::ExprHelper::inferResultType(
               "decode('00', 'hex')", resultTypes) == "bytea");
    assert(dbms::ExprHelper::inferResultType(
               "convert(b, 'LATIN1', 'UTF8')", resultTypes) == "bytea");
    assert(dbms::ExprHelper::inferResultType(
               "convert_from(b, 'LATIN1')", resultTypes) == "text");
    assert(dbms::ExprHelper::inferResultType(
               "convert_to(v, 'LATIN1')", resultTypes) == "bytea");
    assert(dbms::ExprHelper::inferResultType("sha256(b)", resultTypes) ==
           "bytea");
    assert(dbms::ExprHelper::inferResultType("power(i + 1, 2)", resultTypes) == "double precision");
    assert(dbms::ExprHelper::inferResultType("round(n)", resultTypes) == "numeric");
    assert(dbms::ExprHelper::inferResultType("ARRAY[1,2]", resultTypes) == "integer[]");
    assert(dbms::ExprHelper::inferResultType(
               "CASE n WHEN 10 THEN 1 WHEN 20 THEN 2 ELSE 0 END", resultTypes) ==
           "integer");
    assert(dbms::ExprHelper::inferResultType("i::text", resultTypes) == "text");
    assert(dbms::ExprHelper::inferResultType(
               "(i + 1)::numeric(6,2)", resultTypes) == "numeric");
    assert(dbms::ExprHelper::inferResultType("sign(i)", resultTypes) ==
           "double precision");
    assert(dbms::ExprHelper::inferResultType(
               "case_when(=n 10,1,=n 20,2,0)", resultTypes) == "integer");
    assert(dbms::ExprHelper::inferResultType("cast(i,text)", resultTypes) ==
           "text");
    assert(dbms::ExprHelper::inferResultType("exp(1)") == "double precision");
    assert(dbms::ExprHelper::inferResultType(
               "string_to_array('a,b', ',')") == "text[]");
    assert(dbms::ExprHelper::inferResultType(
               "date_part('month', DATE '2026-05-06')") ==
           "double precision");
    assert(dbms::ExprHelper::inferResultType(
               "to_timestamp('2026-08-15', 'YYYY-MM-DD')") ==
           "timestamptz");
    assert(dbms::ExprHelper::inferResultType(
               "regexp_matches('abc', '(a)(b)')") == "text[]");
    assert(dbms::ExprHelper::inferResultType("log(2, 64)") == "numeric");
    assert(dbms::ExprHelper::inferResultType(
               "date '2026-01-31' + interval '1 month'") == "timestamp");
    assert(dbms::ExprHelper::inferResultType(
               "array_append(ARRAY[1,2], 3)") == "integer[]");
    assert(dbms::ExprHelper::inferResultType(
               "'{\"a\":1}'::json -> 'a'") == "json");
    assert(dbms::ExprHelper::inferResultType("'123'::int + 1") == "integer");
    assert(dbms::ExprHelper::inferResultType(
               "'2024-03-15'::date + 7") == "date");
    assert(dbms::ExprHelper::inferResultType(
               "null::boolean AND true") == "boolean");
    assert(dbms::ExprHelper::inferResultType(
               "NULL::text IS NULL") == "boolean");
    assert(dbms::ExprHelper::inferResultType(
               "timestamp '2024-06-01 00:30:00' at time zone 'UTC'") ==
           "timestamptz");
    assert(dbms::ExprHelper::inferResultType("power(9, 0.5)") == "numeric");
    assert(dbms::ExprHelper::inferResultType("round(2.5::float8)") ==
           "double precision");
    assert(dbms::ExprHelper::inferResultType(
               "array[1,2,3] @> array[1,2]") == "boolean");

    dbms::TypeRegistry::instance().bootstrap();
    test_expr_helper_basic();
    test_default_literal();
    test_default_expression();
    test_generated_column();
    test_check_constraint_insert();
    test_check_constraint_update();
    test_generated_identity();
    std::cout << "[CONSTRAINT_EXPR] all passed" << std::endl;
    return 0;
}
