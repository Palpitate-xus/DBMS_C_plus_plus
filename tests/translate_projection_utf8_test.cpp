#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectTranslate(const std::string& database,
                             const std::string& input,
                             const std::string& from,
                             const std::string& to) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "translate";
    expression.isScalar = true;
    expression.funcName = "translate";
    expression.funcArgs = {input, from, to};

    const auto rows = g_engine.queryExpr(
        database, "translate_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "translate_projection_utf8";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "translate_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("input_text", false));
    schema.append(dbms::makeTextColumn("from_text", false));
    schema.append(dbms::makeTextColumn("to_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "translate_values",
                           {{"id", "1"},
                            {"input_text", "xé"},
                            {"from_text", "xé"},
                            {"to_text", "中a"},
                            {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectTranslate(database, "'xé'", "'xé'", "'中a'") ==
           "中a");
    assert(projectTranslate(database, "input_text", "from_text",
                            "to_text") == "中a");
    assert(projectTranslate(database, "'aé中'", "'é中'", "'界'") ==
           "a界");
    assert(projectTranslate(database, "'aé中'", "'é中'", "''") ==
           "a");
    assert(projectTranslate(database, "'aé'", "''", "'中'") ==
           "aé");
    assert(projectTranslate(database, "null_text", "'x'", "'y'") ==
           "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[TRANSLATE PROJECTION UTF8] all passed" << std::endl;
    return 0;
}
