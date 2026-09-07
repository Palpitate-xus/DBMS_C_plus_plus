#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectSlice(const std::string& database,
                         const std::string& function,
                         const std::string& source,
                         const std::string& count) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = {source, count};

    const auto rows = g_engine.queryExpr(
        database, "slice_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "left_right_projection_utf8";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "slice_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "slice_values",
                           {{"id", "1"},
                            {"empty_text", ""},
                            {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectSlice(database, "left", "'aé中z'", "3") == "aé中");
    assert(projectSlice(database, "right", "'aé中z'", "2") == "中z");
    assert(projectSlice(database, "left", "'aé中z'", "-2") == "aé");
    assert(projectSlice(database, "right", "'aé中z'", "-2") == "中z");
    assert(projectSlice(database, "left", "'abc'", "2147483647") ==
           "abc");
    assert(projectSlice(database, "right", "'abc'", "-2147483649")
               .empty());
    assert(projectSlice(database, "left", "empty_text", "2").empty());
    assert(projectSlice(database, "right", "null_text", "2") == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[LEFT RIGHT PROJECTION UTF8] all passed" << std::endl;
    return 0;
}
