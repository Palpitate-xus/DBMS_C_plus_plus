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
        database, "null_text_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "strict_projection_null_text";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "null_text_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("literal_null", false));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["literal_null"] = "NULL";
    row["empty_text"] = "";
    row["null_text"] = std::nullopt;
    assert(g_engine.insertRow(database, "null_text_values", row) ==
           dbms::DBStatus::OK);

    assert(projectFunction(database, "length", {"'NULL'"}) == "4");
    assert(projectFunction(database, "length", {"literal_null"}) == "4");
    assert(projectFunction(database, "reverse", {"'NULL'"}) == "LLUN");
    assert(projectFunction(database, "reverse", {"literal_null"}) ==
           "LLUN");
    assert(projectFunction(database, "replace",
                           {"'NULL'", "'N'", "'n'"}) == "nULL");
    assert(projectFunction(database, "length", {"upper('null')"}) ==
           "4");
    assert(projectFunction(database, "length", {"empty_text"}) == "0");
    assert(projectFunction(database, "length", {"null_text"}) == "NULL");
    assert(projectFunction(database, "length", {"upper(null_text)"}) ==
           "NULL");
    assert(projectFunction(database, "length", {"NULL"}) == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[STRICT PROJECTION NULL TEXT] all passed" << std::endl;
    return 0;
}
