#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectCoalesce(const std::string& database,
                            const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "coalesce";
    expression.isScalar = true;
    expression.funcName = "coalesce";
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "coalesce_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "coalesce_projection_null";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "coalesce_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("null_text", true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("value_text", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "coalesce_values",
                           {{"id", "1"},
                            {"null_text", "NULL"},
                            {"empty_text", ""},
                            {"value_text", "value"}}) ==
           dbms::DBStatus::OK);

    assert(projectCoalesce(database, {"null_text", "'fallback'"}) ==
           "fallback");
    assert(projectCoalesce(database, {"null_text"}) == "NULL");
    assert(projectCoalesce(database, {"null_text", "null_text"}) ==
           "NULL");
    assert(projectCoalesce(
               database, {"null_text", "empty_text", "'fallback'"})
               .empty());
    assert(projectCoalesce(database, {"empty_text", "'fallback'"})
               .empty());
    assert(projectCoalesce(database, {"null_text", "value_text"}) ==
           "value");
    assert(projectCoalesce(
               database,
               {"nullif(value_text,value_text)", "'fallback'"}) ==
           "fallback");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[COALESCE PROJECTION NULL] all passed" << std::endl;
    return 0;
}
