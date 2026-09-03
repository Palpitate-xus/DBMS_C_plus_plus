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

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FLOATING ORDER BY] numeric/NaN/null ordering OK"
              << std::endl;
    return 0;
}
