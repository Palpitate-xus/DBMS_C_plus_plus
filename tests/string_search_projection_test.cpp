#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectSearch(const std::string& database,
                          const std::string& function,
                          const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "string_search_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "string_search_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "string_search_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "string_search_values",
                           {{"id", "1"},
                            {"empty_text", ""},
                            {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectSearch(database, "position", {"''", "'abc'"}) == "1");
    assert(projectSearch(database, "position", {"'x'", "empty_text"}) ==
           "0");
    assert(projectSearch(database, "position", {"'针'", "'甲乙针'"}) ==
           "3");
    assert(projectSearch(database, "strpos", {"'甲乙针'", "'针'"}) ==
           "3");
    assert(projectSearch(database, "instr", {"'甲乙针'", "'针'"}) ==
           "3");
    assert(projectSearch(database, "strpos", {"null_text", "''"}) ==
           "NULL");
    assert(projectSearch(database, "instr", {"null_text", "''"}) ==
           "NULL");
    assert(projectSearch(database, "position", {"'x'", "null_text"}) ==
           "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[STRING SEARCH PROJECTION] all passed" << std::endl;
    return 0;
}
