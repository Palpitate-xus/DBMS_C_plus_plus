#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "scalar_subquery_self_correlation";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "self_rows";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("marker", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(database, schema.tablename,
                               {{"id", "1"}, {"marker", std::nullopt}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insertRow(database, schema.tablename,
                               {{"id", "2"}, {"marker", "second"}}) ==
           dbms::DBStatus::OK);

    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "correlated_value";
    expression.isScalar = true;
    expression.funcName = "subquery";
    expression.funcArgs = {
        "select i.id from self_rows i where i.id = self_rows.id"};
    dbms::StorageEngine::OrderBySpec order;
    order.colName = "id";
    auto rows = g_engine.queryExpr(
        database, schema.tablename, {}, {expression}, {order});
    assert((rows == std::vector<std::string>{"1 ", "2 "}));

    // The outer NULL bit must survive inner alias binding as well.
    expression.funcArgs = {
        "select i.id from self_rows AS i "
        "where i.marker = self_rows.marker"};
    rows = g_engine.queryExpr(
        database, schema.tablename, {}, {expression}, {order});
    assert((rows == std::vector<std::string>{"NULL ", "2 "}));

    expression.funcArgs = {
        "select i.marker from self_rows i where i.id = self_rows.id"};
    rows = g_engine.queryExpr(
        database, schema.tablename, {}, {expression}, {order});
    assert((rows == std::vector<std::string>{"NULL ", "second "}));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SCALAR SUBQUERY SELF CORRELATION] passed\n";
}
