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

std::string projectOverlay(const std::string& database,
                           const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "overlay";
    expression.isScalar = true;
    expression.funcName = "overlay";
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "overlay_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "overlay_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "overlay_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("text_value", false));
    schema.append(dbms::makeTextColumn("replacement", false));
    schema.append(dbms::makeIntColumn("start_pos", false, 4));
    schema.append(dbms::makeIntColumn("replace_len", false, 4));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "overlay_values",
                           {{"id", "1"},
                            {"text_value", "aé中z"},
                            {"replacement", "界"},
                            {"start_pos", "2"},
                            {"replace_len", "2"},
                            {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectOverlay(database, {"'aé中z'", "'X'", "2", "2"}) ==
           "aXz");
    assert(projectOverlay(database, {"'aé中z'", "'界'", "2"}) ==
           "a界中z");
    assert(projectOverlay(database,
                          {"text_value", "replacement", "start_pos",
                           "replace_len"}) == "a界z");
    assert(projectOverlay(database, {"'abc'", "''", "2", "1"}) ==
           "ac");
    assert(projectOverlay(database, {"null_text", "'X'", "2"}) ==
           "NULL");

    bool rejectedInvalidStart = false;
    try {
        (void)projectOverlay(database, {"'abcdef'", "'X'", "0"});
    } catch (const std::runtime_error& error) {
        rejectedInvalidStart =
            std::string(error.what()).find("SQLSTATE 22011") !=
            std::string::npos;
    }
    assert(rejectedInvalidStart);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[OVERLAY PROJECTION SEMANTICS] all passed" << std::endl;
    return 0;
}
