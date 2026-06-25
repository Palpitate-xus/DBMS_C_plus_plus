#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "expression/expr_helper.h"
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
    std::string db = "constraints_t1";
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
    std::string db = "constraints_t2";
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
    std::string db = "constraints_t3";
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
    std::string db = "constraints_t4";
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
    std::string db = "constraints_t5";
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
    std::string db = "constraints_t6";
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
