#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectCase(const std::string& database,
                        const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "case";
    expression.isScalar = true;
    expression.funcName = "case_when";
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "case_default_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "case_when_projection_default";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "case_default_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "case_default_values", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(projectCase(database, {">id 10", "'large'"}) == "NULL");
    assert(projectCase(database,
                       {">id 10", "'large'", "<id 0", "'negative'"}) ==
           "NULL");
    assert(projectCase(database, {"=id 1", "'one'"}) == "one");
    assert(projectCase(database,
                       {">id 10", "'large'", "=id 1", "'one'"}) ==
           "one");
    assert(projectCase(database,
                       {">id 10", "'large'", "'fallback'"}) ==
           "fallback");
    assert(projectCase(database,
                       {">id 10", "'large'", "NULL"}) == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[CASE WHEN PROJECTION DEFAULT] all passed" << std::endl;
    return 0;
}
