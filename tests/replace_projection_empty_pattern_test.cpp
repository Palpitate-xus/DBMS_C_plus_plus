#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectReplace(const std::string& database,
                           const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "replace";
    expression.isScalar = true;
    expression.funcName = "replace";
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "replace_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "replace_projection_empty_pattern";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "replace_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "replace_values",
                           {{"id", "1"},
                            {"empty_text", ""},
                            {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectReplace(database, {"'abc'", "''", "'x'"}) == "abc");
    assert(projectReplace(database, {"empty_text", "''", "'x'"}).empty());
    assert(projectReplace(database, {"'abc'", "'b'", "''"}) == "ac");
    assert(projectReplace(database, {"null_text", "'a'", "'b'"}) ==
           "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[REPLACE PROJECTION EMPTY PATTERN] all passed"
              << std::endl;
    return 0;
}
