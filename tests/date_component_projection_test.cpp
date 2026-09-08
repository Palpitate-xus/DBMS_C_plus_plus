#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectComponent(const std::string& database,
                             const std::string& function,
                             const std::string& source) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = {source};

    const auto rows = g_engine.queryExpr(
        database, "date_component_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

std::string projectScalar(const std::string& database,
                          const std::string& function,
                          const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "date_component_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}
}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "date_component_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "date_component_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeDateColumn("actual_date", false));
    schema.append(dbms::makeDateColumn("null_date", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "date_component_values",
                           {{"id", "1"},
                            {"actual_date", "2024-03-04"},
                            {"null_date", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectComponent(database, "year", "actual_date") == "2024");
    assert(projectComponent(database, "month", "'2024-02-29'") == "2");
    assert(projectComponent(
               database, "day", "'2024-03-04T12:34:56.25'") == "4");
    assert(projectComponent(
               database, "year", "'2024-03-04 12:34:56+02:30'") ==
           "2024");
    assert(projectComponent(
               database, "year", "'2024-03-04 25:00:00'").empty());
    assert(projectComponent(
               database, "year", "'2024-03-04 garbage'").empty());
    assert(projectComponent(
               database, "year", "'2024-03-04 12:34:56+16:00'").empty());
    assert(projectComponent(database, "year", "null_date") == "NULL");
    assert(projectScalar(
               database, "arith", {"null_date", "+", "1"}) == "NULL");
    assert(projectScalar(
               database, "arith",
               {"actual_date", "-", "DATE '2024-01-01'"}) == "63");
    assert(projectScalar(
               database, "extract", {"year", "null_date"}) == "NULL");
    assert(projectScalar(
               database, "age",
               {"null_date", "DATE '2024-01-01'"}) == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[DATE COMPONENT PROJECTION] all passed" << std::endl;
    return 0;
}
