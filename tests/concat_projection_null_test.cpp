#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectConcat(const std::string& database,
                          const std::string& function,
                          const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "concat_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "concat_projection_null";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "concat_values";
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
    assert(g_engine.insertRow(database, "concat_values", row) ==
           dbms::DBStatus::OK);

    assert(projectConcat(database, "concat_ws",
                         {"','", "'a'", "''", "'b'"}) == "a,,b");
    assert(projectConcat(database, "concat_ws",
                         {"','", "'a'", "empty_text", "'b'"}) ==
           "a,,b");
    assert(projectConcat(database, "concat_ws",
                         {"','", "'a'", "null_text", "'b'"}) ==
           "a,b");
    assert(projectConcat(database, "concat_ws",
                         {"','", "literal_null", "'b'"}) == "NULL,b");
    assert(projectConcat(database, "concat", {"null_text", "'a'"}) ==
           "a");
    assert(projectConcat(database, "concat", {"literal_null", "'a'"}) ==
           "NULLa");
    assert(projectConcat(database, "concat_ws",
                         {"null_text", "'a'", "'b'"}) == "NULL");
    assert(projectConcat(database, "length",
                         {"concat_ws(null_text,'a','b')"}) == "NULL");
    assert(projectConcat(database, "length",
                         {"concat_ws(',',literal_null)"}) == "4");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[CONCAT PROJECTION NULL] all passed" << std::endl;
    return 0;
}
