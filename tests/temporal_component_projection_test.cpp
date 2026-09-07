#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

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
        database, "temporal_component_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "temporal_component_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "temporal_component_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTimeColumn("null_time", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "temporal_component_values",
                           {{"id", "1"}, {"null_time", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectComponent(database, "second", "'12:34:56.25'") ==
           "56");
    assert(projectComponent(database, "second", "'12:34:56.25+02:30'") ==
           "56");
    assert(projectComponent(
               database, "hour", "'2024-01-02T12:34:56-05:30'") ==
           "12");
    assert(projectComponent(
               database, "minute", "'2024-01-02 12:34:56.25'") ==
           "34");
    assert(projectComponent(database, "second", "'12:34:56x'").empty());
    assert(projectComponent(
               database, "hour", "'2023-02-29 12:34:56'").empty());
    assert(projectComponent(
               database, "hour", "'12:34:56+16:00'").empty());
    assert(projectComponent(database, "second", "null_time") == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[TEMPORAL COMPONENT PROJECTION] all passed" << std::endl;
    return 0;
}
