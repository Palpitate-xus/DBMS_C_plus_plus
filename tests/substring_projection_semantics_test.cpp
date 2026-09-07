#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectSubstring(const std::string& database,
                             const std::string& function,
                             const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "substring_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "substring_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "substring_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("text_value", false));
    schema.append(dbms::makeIntColumn("start_pos", false, 4));
    schema.append(dbms::makeIntColumn("slice_len", false, 4));
    schema.append(dbms::makeIntColumn("null_pos", true, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "substring_values",
                           {{"id", "1"},
                            {"text_value", "aé中z"},
                            {"start_pos", "2"},
                            {"slice_len", "2"},
                            {"null_pos", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectSubstring(database, "substring",
                            {"'aé中z'", "2", "2"}) == "é中");
    assert(projectSubstring(database, "substr",
                            {"'aé中z'", "3"}) == "中z");
    assert(projectSubstring(database, "substring",
                            {"'aé中z'", "0", "2"}) == "a");
    assert(projectSubstring(database, "substring",
                            {"text_value", "start_pos", "slice_len"}) ==
           "é中");
    assert(projectSubstring(database, "substring",
                            {"text_value", "null_pos", "2"}) == "NULL");

    bool rejectedNegativeLength = false;
    try {
        (void)projectSubstring(database, "substring",
                               {"'alphabet'", "2", "-1"});
    } catch (const std::runtime_error& error) {
        rejectedNegativeLength =
            std::string(error.what()).find("SQLSTATE 22011") !=
            std::string::npos;
    }
    assert(rejectedNegativeLength);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SUBSTRING PROJECTION SEMANTICS] all passed" << std::endl;
    return 0;
}
