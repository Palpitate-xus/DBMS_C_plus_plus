#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectSplitPart(const std::string& database,
                             const std::string& input,
                             const std::string& delimiter,
                             const std::string& field) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "split_part";
    expression.isScalar = true;
    expression.funcName = "split_part";
    expression.funcArgs = {input, delimiter, field};

    const auto rows = g_engine.queryExpr(
        database, "split_part_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

void expectSqlState(const std::string& database,
                    const std::string& field,
                    const std::string& sqlState) {
    bool rejected = false;
    try {
        (void)projectSplitPart(database, "'a,b,c'", "','", field);
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find(
            "SQLSTATE " + sqlState) != std::string::npos;
    }
    assert(rejected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "split_part_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "split_part_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "split_part_values",
                           {{"id", "1"}, {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectSplitPart(database, "'a,b,c'", "','", "2") == "b");
    assert(projectSplitPart(database, "'abc'", "''", "1") == "abc");
    assert(projectSplitPart(database, "'abc'", "''", "2").empty());
    assert(projectSplitPart(database, "'a,b,c'", "','", "-1") == "c");
    assert(projectSplitPart(database, "'a,b,c'", "','", "-2") == "b");
    assert(projectSplitPart(database, "'a,b,c'", "','", "-4").empty());
    assert(projectSplitPart(database, "null_text", "','", "1") ==
           "NULL");

    expectSqlState(database, "0", "22023");
    expectSqlState(database, "'2x'", "22P02");
    expectSqlState(database, "2147483648", "22003");
    expectSqlState(database, "-2147483649", "22003");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SPLIT PART PROJECTION] all passed" << std::endl;
    return 0;
}
