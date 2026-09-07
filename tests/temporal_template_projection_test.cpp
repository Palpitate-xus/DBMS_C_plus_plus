#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectFunction(const std::string& database,
                            const std::string& function,
                            const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "temporal_template_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "temporal_template_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "temporal_template_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("null_text", true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "temporal_template_values",
                           {{"id", "1"},
                            {"null_text", "NULL"},
                            {"empty_text", ""}}) == dbms::DBStatus::OK);

    assert(projectFunction(database, "to_date",
                           {"'15.08.2026'", "'DD.MM.YYYY'"}) ==
           "2026-08-15");
    assert(projectFunction(
               database, "to_timestamp",
               {"'2026-08-15 14:30:05'", "'YYYY-MM-DD HH24:MI:SS'"}) ==
           "2026-08-15 14:30:05+00");
    assert(projectFunction(database, "to_date",
                           {"'2026-02-30'", "'YYYY-MM-DD'"}) == "NULL");
    assert(projectFunction(
               database, "to_timestamp",
               {"'2026-08-15 25:30:05'", "'YYYY-MM-DD HH24:MI:SS'"}) ==
           "NULL");
    assert(projectFunction(database, "to_date", {"'not-a-date'"}) ==
           "NULL");
    assert(projectFunction(database, "to_timestamp", {"empty_text"}) ==
           "NULL");
    assert(projectFunction(database, "to_date", {"null_text"}) == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[TEMPORAL TEMPLATE PROJECTION] all passed" << std::endl;
    return 0;
}
