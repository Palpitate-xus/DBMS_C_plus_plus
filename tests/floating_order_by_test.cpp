#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::vector<std::string> ids(std::initializer_list<const char*> values) {
    std::vector<std::string> result;
    for (const char* value : values) result.emplace_back(std::string(value) + " ");
    return result;
}

dbms::StorageEngine::OrderBySpec ascending(const std::string& column,
                                            bool nullsFirst = false) {
    dbms::StorageEngine::OrderBySpec order;
    order.colName = column;
    order.ascending = true;
    order.nullsFirst = nullsFirst;
    return order;
}

void assertQueryOrder(const std::string& database, const std::string& column,
                      const std::vector<std::string>& expected,
                      bool nullsFirst = false) {
    const std::vector<dbms::StorageEngine::OrderBySpec> order = {
        ascending(column, nullsFirst)};
    assert(g_engine.query(database, "values_to_sort", {}, {"id"}, order) ==
           expected);

    dbms::StorageEngine::SelectExpr idExpression;
    idExpression.displayName = "id";
    idExpression.colName = "id";
    assert(g_engine.queryExpr(database, "values_to_sort", {},
                              {idExpression}, order) == expected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "floating_order_by";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "values_to_sort";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeFloatColumn("single_value", true));
    schema.append(dbms::makeDoubleColumn("double_value", true));
    schema.append(dbms::makeDecimalColumn("decimal_value", true, 20, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    const std::vector<std::vector<std::string>> rows = {
        {"1", "3.5", "-2.25", "10.75"},
        {"2", "-2.25", "3.5", "-10.25"},
        {"3", "1.75", "1.75", "2.5"},
        {"4", "Infinity", "-Infinity", "2.05"},
        {"5", "-Infinity", "Infinity", "2.005"},
        {"6", "NaN", "NaN", "0.125"},
        {"7", "NULL", "NULL", "NULL"},
    };
    for (const auto& row : rows) {
        assert(g_engine.insert(
                   database, "values_to_sort",
                   {{"id", row[0]}, {"single_value", row[1]},
                    {"double_value", row[2]}, {"decimal_value", row[3]}}) ==
               dbms::DBStatus::OK);
    }

    // PostgreSQL floating ordering is numeric, with NaN after +Infinity.
    assertQueryOrder(database, "single_value",
                     ids({"5", "2", "3", "1", "4", "6", "7"}));
    assertQueryOrder(database, "double_value",
                     ids({"4", "1", "3", "2", "5", "6", "7"}));
    assertQueryOrder(database, "decimal_value",
                     ids({"2", "6", "5", "4", "3", "1", "7"}));
    assertQueryOrder(database, "single_value",
                     ids({"7", "5", "2", "3", "1", "4", "6"}), true);

    dbms::StorageEngine::OrderBySpec descending = ascending("single_value");
    descending.ascending = false;
    assert(g_engine.query(database, "values_to_sort", {}, {"id"},
                          {descending}) ==
           ids({"6", "4", "1", "3", "2", "5", "7"}));

    dbms::TableSchema numericSchema;
    numericSchema.tablename = "numeric_function_order";
    numericSchema.formatVersion = 2;
    numericSchema.append(dbms::makeDecimalColumn("v", false, 20, 4));
    assert(g_engine.createTable(database, numericSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"-1.4", "-1.6"})
        assert(g_engine.insert(database, "numeric_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    dbms::StorageEngine::OrderBySpec functionSort;
    functionSort.isExpression = true;
    functionSort.exprFunc = "round";
    functionSort.expressionSql = "round(v)";
    assert(g_engine.query(database, "numeric_function_order", {}, {"v"},
                          {functionSort}) == ids({"-1.6", "-1.4"}));
    assert(g_engine.sortByExpression(
               database, "numeric_function_order",
               ids({"-1.4", "-1.6"}), {functionSort}) ==
           ids({"-1.6", "-1.4"}));
    functionSort.exprFunc = "abs";
    functionSort.expressionSql = "abs(v)";
    functionSort.ascending = false;
    assert(g_engine.query(database, "numeric_function_order", {}, {"v"},
                          {functionSort}) == ids({"-1.6", "-1.4"}));

    dbms::TableSchema powerSchema;
    powerSchema.tablename = "power_function_order";
    powerSchema.formatVersion = 2;
    powerSchema.append(dbms::makeIntColumn("v", false, 4));
    assert(g_engine.createTable(database, powerSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"-2", "-1"})
        assert(g_engine.insert(database, "power_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    functionSort.exprFunc = "power";
    functionSort.expressionSql = "power(v, 2)";
    functionSort.ascending = true;
    assert(g_engine.query(database, "power_function_order", {}, {"v"},
                          {functionSort}) == ids({"-1", "-2"}));

    dbms::TableSchema roundingSchema;
    roundingSchema.tablename = "rounding_function_order";
    roundingSchema.formatVersion = 2;
    roundingSchema.append(dbms::makeDecimalColumn("v", false, 20, 4));
    assert(g_engine.createTable(database, roundingSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"2.2", "10.2"})
        assert(g_engine.insert(database, "rounding_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    for (const auto& name : {"floor", "ceil", "trunc", "sign"}) {
        functionSort.exprFunc = name;
        functionSort.expressionSql = std::string(name) + "(v)";
        assert(g_engine.query(database, "rounding_function_order", {},
                              {"v"}, {functionSort}) ==
               ids({"2.2", "10.2"}));
    }
    functionSort.exprFunc = "mod";
    functionSort.expressionSql = "mod(v, 3)";
    assert(g_engine.query(database, "rounding_function_order", {}, {"v"},
                          {functionSort}) == ids({"10.2", "2.2"}));
    assert(g_engine.sortByExpression(
               database, "rounding_function_order",
               ids({"2.2", "10.2"}), {functionSort}) ==
           ids({"10.2", "2.2"}));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FLOATING ORDER BY] numeric/NaN/null ordering OK"
              << std::endl;
    return 0;
}
