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

std::string projectPad(const std::string& database,
                       const std::string& function,
                       const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "pad_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

void expectSqlState(const std::string& database,
                    const std::string& function,
                    const std::string& length,
                    const std::string& sqlState) {
    bool rejected = false;
    try {
        (void)projectPad(
            database, function, {"'x'", length, "'a'"});
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find(
            "SQLSTATE " + sqlState) != std::string::npos;
    }
    assert(rejected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "pad_projection_bounds";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "pad_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "pad_values",
                           {{"id", "1"}, {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectPad(database, "lpad", {"'abc'", "-1", "'x'"}).empty());
    assert(projectPad(database, "rpad", {"'abc'", "0", "'x'"}).empty());
    assert(projectPad(database, "lpad", {"'abc'", "5", "''"}) == "abc");
    assert(projectPad(database, "rpad", {"'abc'", "5", "''"}) == "abc");
    assert(projectPad(database, "lpad", {"'é'", "3", "'界'"}) ==
           "界界é");
    assert(projectPad(database, "rpad", {"'é'", "3", "'界'"}) ==
           "é界界");
    assert(projectPad(database, "lpad", {"null_text", "3", "'x'"}) ==
           "NULL");
    expectSqlState(database, "lpad", "'2x'", "22P02");
    expectSqlState(database, "rpad", "2147483648", "22003");
    expectSqlState(database, "lpad", "1073741824", "54000");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[PAD PROJECTION BOUNDS] all passed" << std::endl;
    return 0;
}
