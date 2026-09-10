#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "parser/parser.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
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

static bool runDml(const std::string& sql, Session& session,
                   std::string* output = nullptr) {
    std::ostringstream captured;
    std::streambuf* previous = std::cout.rdbuf(captured.rdbuf());
    bool handled = false;
    const bool error = dbms::tryDmlBridge(
        sql, dbms::SQLParser::classify(sql), session, handled);
    std::cout.rdbuf(previous);
    assert(handled);
    if (output) *output = captured.str();
    return error;
}

static size_t rowCount(const std::string& db, const std::string& table) {
    size_t count = 0;
    assert(g_engine.forEachRow(
        db, table,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
    return count;
}

static void test_stored_generated() {
    std::string db = testDbPath("gen_col_stored");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT GENERATED ALWAYS AS (a + b) STORED)", s));

    // User-supplied value for generated column is rejected.
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"a", "3"}, {"b", "4"}, {"c", "99"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"a", "3"}, {"b", "4"}, {"c", ""}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insertRow(
               db, "t", {{"id", std::string("1")}, {"a", std::string("3")},
                          {"b", std::string("4")}, {"c", std::nullopt}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(rowCount(db, "t") == 0);

    std::string output;
    assert(runDml(
        "INSERT INTO t (id, a, b, c) VALUES (1, 3, 4, NULL)", s,
        &output));
    assert(output.find("SQLSTATE 428C9") != std::string::npos);
    assert(runDml(
        "INSERT INTO t (id, a, b, c) VALUES (1, 3, 4, '')", s,
        &output));
    assert(output.find("SQLSTATE 428C9") != std::string::npos);
    assert(rowCount(db, "t") == 0);

    // DEFAULT and omission are the only accepted generated-column inputs.
    assert(!runDml(
        "INSERT INTO t (id, a, b, c) VALUES (1, 3, 4, DEFAULT)", s));
    assert(!runDml("INSERT INTO t (id, a, b) VALUES (2, 5, 6)", s));

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"c"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "7");

    // UPDATE recomputes STORED generated column.
    assert(g_engine.update(db, "t", {{"a", "10"}}, {"=id 1"}) == dbms::DBStatus::OK);
    rows = g_engine.query(db, "t", {"=id 1"}, {"c"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "14");

    // Direct update of generated column is rejected.
    assert(g_engine.update(db, "t", {{"c", "99"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[GENERATED] STORED OK" << std::endl;
}

static void test_virtual_generated() {
    std::string db = testDbPath("gen_col_virtual");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT GENERATED ALWAYS AS (a * b) VIRTUAL)", s));

    // User-supplied value for virtual generated column is rejected.
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"a", "6"}, {"b", "7"}, {"c", "99"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"a", "6"}, {"b", "7"}, {"c", ""}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insertRow(
               db, "t", {{"id", std::string("1")}, {"a", std::string("6")},
                          {"b", std::string("7")}, {"c", std::nullopt}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(rowCount(db, "t") == 0);

    std::string output;
    assert(runDml(
        "INSERT INTO t (id, a, b, c) VALUES (1, 6, 7, NULL)", s,
        &output));
    assert(output.find("SQLSTATE 428C9") != std::string::npos);
    assert(runDml(
        "INSERT INTO t (id, a, b, c) VALUES (1, 6, 7, '')", s,
        &output));
    assert(output.find("SQLSTATE 428C9") != std::string::npos);
    assert(rowCount(db, "t") == 0);

    assert(!runDml(
        "INSERT INTO t (id, a, b, c) VALUES (1, 6, 7, DEFAULT)", s));
    assert(!runDml("INSERT INTO t (id, a, b) VALUES (2, 2, 3)", s));
    assert(rowCount(db, "t") == 2);
    assert(g_engine.remove(db, "t", {"=id 2"}) == dbms::DBStatus::OK);

    // Query-time computation of VIRTUAL column.
    auto rows = g_engine.query(db, "t", {"=id 1"}, {"c"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "42");

    // Predicates and the expression projection path must observe the
    // computed value instead of the virtual column's unused storage slot.
    assert(g_engine.query(db, "t", {"=c 42"}, {"id"}) ==
           std::vector<std::string>{"1 "});
    assert(g_engine.query(db, "t", {"isnotnull c"}, {"id"}) ==
           std::vector<std::string>{"1 "});
    dbms::StorageEngine::SelectExpr cExpr;
    cExpr.displayName = "c";
    cExpr.colName = "c";
    assert(g_engine.queryExpr(db, "t", {}, {cExpr}) ==
           std::vector<std::string>{"42 "});

    dbms::StorageEngine::SelectExpr absExpr;
    absExpr.displayName = "abs_c";
    absExpr.isScalar = true;
    absExpr.funcName = "abs";
    absExpr.funcArgs = {"c"};
    assert(g_engine.queryExpr(db, "t", {}, {absExpr}) ==
           std::vector<std::string>{"42 "});

    assert(g_engine.insert(db, "t", {{"id", "2"}, {"a", "2"}, {"b", "3"}}) ==
           dbms::DBStatus::OK);
    dbms::StorageEngine::OrderBySpec exprOrder;
    exprOrder.isExpression = true;
    exprOrder.exprFunc = "add";
    exprOrder.exprArg = "c";
    exprOrder.exprArg2 = "0";
    assert(g_engine.query(db, "t", {}, {"id"}, {exprOrder}) ==
           (std::vector<std::string>{"2 ", "1 "}));

    // VIRTUAL column is recomputed after UPDATE of base columns.
    assert(g_engine.update(db, "t", {{"a", "5"}}, {"=id 1"}) == dbms::DBStatus::OK);
    rows = g_engine.query(db, "t", {"=id 1"}, {"c"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "35");

    // DML predicates use the same logical value path.
    assert(g_engine.update(db, "t", {{"a", "4"}}, {"=c 6"}) ==
           dbms::DBStatus::OK);
    rows = g_engine.query(db, "t", {"=id 2"}, {"c"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "12");

    const std::vector<dbms::StorageEngine::AggItem> aggregateItems = {
        {"sum", "c", {}, {}}, {"count", "c", {}, {}}};
    assert(g_engine.aggregate(db, "t", {}, aggregateItems) ==
           std::vector<std::string>{"47 2 "});
    const std::vector<dbms::StorageEngine::AggItem> groupedItems = {
        {"count", "*", {}, {}}};
    assert(g_engine.groupAggregate(db, "t", {}, groupedItems, {"c"}, {}) ==
           (std::vector<std::string>{"12 1 ", "35 1 "}));
    assert(g_engine.groupAggregateSets(
               db, "t", {}, groupedItems, {"c"}, {{"c"}}, {}) ==
           (std::vector<std::string>{"12 1 ", "35 1 "}));

    assert(g_engine.remove(db, "t", {"=c 12"}) == dbms::DBStatus::OK);
    assert(g_engine.query(db, "t", {"=id 2"}, {"id"}).empty());

    // Direct update of virtual generated column is rejected.
    assert(g_engine.update(db, "t", {{"c", "99"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[GENERATED] VIRTUAL OK" << std::endl;
}

static void test_virtual_scalar_function() {
    std::string db = testDbPath("gen_col_virtual_func");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20), greet VARCHAR(50) GENERATED ALWAYS AS ('Hello, ' || name) VIRTUAL)", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "World"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"greet"});
    assert(rows.size() == 1);
    assert(trimRight(rows[0]) == "Hello, World");

    cleanup(db);
    std::cout << "[GENERATED] VIRTUAL scalar OK" << std::endl;
}

static void test_virtual_null_and_empty() {
    std::string db = testDbPath("gen_col_virtual_null");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, base INT, source_text VARCHAR(10), "
        "null_value INT GENERATED ALWAYS AS (base + 1) VIRTUAL, "
        "empty_value VARCHAR(10) GENERATED ALWAYS AS ('') VIRTUAL, "
        "copied_value VARCHAR(10) GENERATED ALWAYS AS (source_text || '') VIRTUAL)", s));
    assert(g_engine.insert(
               db, "t", {{"id", "1"}, {"source_text", ""}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.query(db, "t", {}, {"null_value"}) ==
           std::vector<std::string>{"NULL "});
    assert(g_engine.query(db, "t", {"isnull null_value"}, {"id"}) ==
           std::vector<std::string>{"1 "});
    assert(g_engine.query(db, "t", {"isnotnull empty_value"}, {"id"}) ==
           std::vector<std::string>{"1 "});
    assert(g_engine.query(db, "t", {"isnotnull copied_value"}, {"id"}) ==
           std::vector<std::string>{"1 "});

    const std::vector<dbms::StorageEngine::AggItem> items = {
        {"count", "null_value", {}, {}},
        {"count", "empty_value", {}, {}},
        {"count", "copied_value", {}, {}},
        {"count", "distinct null_value", {}, {}},
        {"count", "distinct empty_value", {}, {}}};
    assert(g_engine.aggregate(db, "t", {}, items) ==
           std::vector<std::string>{"0 1 1 0 1 "});

    dbms::StorageEngine::SelectExpr nullExpr;
    nullExpr.displayName = "null_value";
    nullExpr.colName = "null_value";
    dbms::StorageEngine::SelectExpr absExpr;
    absExpr.displayName = "abs_null";
    absExpr.isScalar = true;
    absExpr.funcName = "abs";
    absExpr.funcArgs = {"null_value"};
    assert(g_engine.queryExpr(db, "t", {}, {nullExpr, absExpr}) ==
           std::vector<std::string>{"NULL NULL "});

    cleanup(db);
    std::cout << "[GENERATED] VIRTUAL NULL/empty distinction OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_stored_generated();
    test_virtual_generated();
    test_virtual_scalar_function();
    test_virtual_null_and_empty();
    std::cout << "[GENERATED_COLUMNS] all passed" << std::endl;
    return 0;
}
