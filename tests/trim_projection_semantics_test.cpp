#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectTrim(const std::string& database,
                        const std::string& function,
                        const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "trim_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "trim_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "trim_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("input_text", false));
    schema.append(dbms::makeTextColumn("trim_set", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "trim_values",
                           {{"id", "1"},
                            {"input_text", "丰x丰"},
                            {"trim_set", "中估"},
                            {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectTrim(database, "btrim", {"'丰x丰'", "'中估'"}) ==
           "丰x丰");
    assert(projectTrim(database, "ltrim", {"'丰x'", "'中估'"}) ==
           "丰x");
    assert(projectTrim(database, "rtrim", {"'x丰'", "'中估'"}) ==
           "x丰");
    assert(projectTrim(database, "btrim", {"input_text", "trim_set"}) ==
           "丰x丰");
    assert(projectTrim(database, "btrim", {"' x '", "''"}) == " x ");
    assert(projectTrim(database, "ltrim", {"'  x  '"}) == "x  ");
    assert(projectTrim(database, "rtrim", {"'  x  '"}) == "  x");
    assert(projectTrim(database, "btrim", {"'\tx\t'"}) == "\tx\t");
    assert(projectTrim(database, "trim",
                       {"leading 'é' from 'ééxé'"}) == "xé");
    assert(projectTrim(database, "trim",
                       {"trailing 'é' from 'ééxé'"}) == "ééx");
    assert(projectTrim(database, "btrim", {"input_text", "null_text"}) ==
           "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[TRIM PROJECTION SEMANTICS] all passed" << std::endl;
    return 0;
}
