#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectReverse(const std::string& database,
                           const std::string& source) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "reverse";
    expression.isScalar = true;
    expression.funcName = "reverse";
    expression.funcArgs = {source};

    const auto rows = g_engine.queryExpr(
        database, "reverse_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "reverse_projection_utf8";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "reverse_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "reverse_values",
                           {{"id", "1"},
                            {"empty_text", ""},
                            {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectReverse(database, "'aé中'") == "中éa");
    assert(projectReverse(database, "'甲乙'") == "乙甲");
    assert(projectReverse(database, "empty_text").empty());
    assert(projectReverse(database, "null_text") == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[REVERSE PROJECTION UTF8] all passed" << std::endl;
    return 0;
}
