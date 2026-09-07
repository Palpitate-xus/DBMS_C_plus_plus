#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "scalar_subquery_projection_cardinality";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema outer;
    outer.tablename = "scalar_outer";
    outer.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    outer.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, outer) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "scalar_outer", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    dbms::TableSchema inner;
    inner.tablename = "scalar_inner";
    inner.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    inner.append(dbms::makeIntColumn("value", false, 4, true));
    assert(g_engine.createTable(database, inner) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "scalar_inner", {{"value", "10"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "scalar_inner", {{"value", "20"}}) ==
           dbms::DBStatus::OK);

    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "scalar_value";
    expression.isScalar = true;
    expression.funcName = "subquery";
    expression.funcArgs = {"select value from scalar_inner"};

    bool rejected = false;
    try {
        (void)g_engine.queryExpr(
            database, "scalar_outer", {}, {expression});
    } catch (const std::runtime_error& error) {
        const std::string message = error.what();
        rejected = message.find(
                       "more than one row returned by a subquery used as an expression") !=
                       std::string::npos &&
                   message.find("SQLSTATE 21000") != std::string::npos;
    }
    assert(rejected);

    dbms::TableSchema emptyInner;
    emptyInner.tablename = "scalar_empty";
    emptyInner.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    emptyInner.append(dbms::makeIntColumn("value", true, 4));
    assert(g_engine.createTable(database, emptyInner) == dbms::DBStatus::OK);
    expression.funcArgs = {"select value from scalar_empty"};
    const auto emptyRows = g_engine.queryExpr(
        database, "scalar_outer", {}, {expression});
    assert(emptyRows.size() == 1);
    assert(emptyRows.front() == "NULL ");

    dbms::TableSchema textInner;
    textInner.tablename = "scalar_text";
    textInner.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    textInner.append(dbms::makeIntColumn("id", false, 4, true));
    textInner.append(dbms::makeTextColumn("value", false));
    textInner.append(dbms::makeTextColumn("fromage", false));
    assert(g_engine.createTable(database, textInner) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "scalar_text",
                           {{"id", "1"},
                            {"value", "two words"},
                            {"fromage", "cheddar"}}) ==
           dbms::DBStatus::OK);
    expression.funcArgs = {"select value from scalar_text"};
    const auto textRows = g_engine.queryExpr(
        database, "scalar_outer", {}, {expression});
    assert(textRows.size() == 1);
    assert(textRows.front() == "two words ");

    expression.funcArgs = {"SELECT value FROM scalar_text"};
    const auto uppercaseRows = g_engine.queryExpr(
        database, "scalar_outer", {}, {expression});
    assert(uppercaseRows.size() == 1);
    assert(uppercaseRows.front() == "two words ");

    expression.funcArgs = {"select fromage from scalar_text"};
    const auto keywordSubstringRows = g_engine.queryExpr(
        database, "scalar_outer", {}, {expression});
    assert(keywordSubstringRows.size() == 1);
    assert(keywordSubstringRows.front() == "cheddar ");

    expression.funcArgs = {"select id,value from scalar_text"};
    rejected = false;
    try {
        (void)g_engine.queryExpr(
            database, "scalar_outer", {}, {expression});
    } catch (const std::runtime_error& error) {
        const std::string message = error.what();
        rejected = message.find("subquery must return only one column") !=
                       std::string::npos &&
                   message.find("SQLSTATE 42601") != std::string::npos;
    }
    assert(rejected);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SCALAR SUBQUERY PROJECTION CARDINALITY] passed\n";
    return 0;
}
