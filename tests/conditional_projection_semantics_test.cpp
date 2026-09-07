#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectConditional(const std::string& database,
                               const std::string& function,
                               const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "conditional_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "conditional_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "conditional_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("literal_null", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["empty_text"] = "";
    row["literal_null"] = "NULL";
    row["null_text"] = std::nullopt;
    assert(g_engine.insertRow(database, "conditional_values", row) ==
           dbms::DBStatus::OK);

    assert(projectConditional(database, "if",
                              {"id < 0", "'yes'", "'no'"}) == "no");
    assert(projectConditional(database, "iif",
                              {"id = 1", "'yes'", "'no'"}) == "yes");
    assert(projectConditional(database, "if",
                              {"NULL", "'yes'", "'no'"}) == "no");
    assert(projectConditional(database, "if",
                              {"true", "empty_text", "'fallback'"})
               .empty());
    assert(projectConditional(database, "if",
                              {"true", "null_text", "'fallback'"}) ==
           "NULL");
    assert(projectConditional(database, "length",
                              {"if(true,literal_null,'x')"}) == "4");
    assert(projectConditional(database, "length",
                              {"iif(false,'x',null_text)"}) == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[CONDITIONAL PROJECTION SEMANTICS] all passed"
              << std::endl;
    return 0;
}
