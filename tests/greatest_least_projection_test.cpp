#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectChoice(const std::string& database,
                          const std::string& function,
                          const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "choice_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "greatest_least_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "choice_values";
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
    assert(g_engine.insertRow(database, "choice_values", row) ==
           dbms::DBStatus::OK);

    assert(projectChoice(database, "least", {"empty_text", "'z'"})
               .empty());
    assert(projectChoice(database, "greatest", {"empty_text", "'z'"}) ==
           "z");
    assert(projectChoice(database, "least", {"'b'", "''", "'a'"})
               .empty());
    assert(projectChoice(database, "greatest",
                         {"9007199254740992", "9007199254740993"}) ==
           "9007199254740993");
    assert(projectChoice(database, "least",
                         {"9007199254740993", "9007199254740992"}) ==
           "9007199254740992");
    assert(projectChoice(database, "greatest",
                         {"1.0000000000000001", "1.0000000000000002"}) ==
           "1.0000000000000002");
    assert(projectChoice(database, "greatest", {"NULL", "2", "1"}) ==
           "2");
    assert(projectChoice(database, "least", {"null_text", "2", "1"}) ==
           "1");
    assert(projectChoice(database, "least", {"NULL", "null_text"}) ==
           "NULL");
    assert(projectChoice(database, "least", {"literal_null", "'ZZZ'"}) ==
           "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[GREATEST LEAST PROJECTION] all passed" << std::endl;
    return 0;
}
