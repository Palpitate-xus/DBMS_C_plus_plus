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

std::string projectMath(const std::string& database,
                        const std::string& function,
                        const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;
    const auto rows = g_engine.queryExpr(
        database, "math_error_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

void expectSqlState(const std::string& database,
                    const std::string& function,
                    const std::vector<std::string>& arguments,
                    const std::string& sqlState) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    bool rejected = false;
    std::string observed;
    try {
        (void)g_engine.queryExpr(
            database, "math_error_values", {}, {expression});
    } catch (const std::runtime_error& error) {
        observed = error.what();
        rejected = std::string(error.what()).find(
            "SQLSTATE " + sqlState) != std::string::npos;
    }
    if (!rejected) {
        std::cerr << "expected " << function << " to report SQLSTATE "
                  << sqlState << ", observed "
                  << (observed.empty() ? "no error" : observed) << std::endl;
    }
    assert(rejected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "math_projection_error";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "math_error_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "math_error_values", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(projectMath(database, "sqrt", {"9"}) == "3");
    assert(projectMath(database, "mod", {"5", "2"}) == "1");
    assert(projectMath(database, "width_bucket", {"5", "0", "10", "5"}) ==
           "3");

    expectSqlState(database, "sqrt", {"-1"}, "2201F");
    expectSqlState(database, "ln", {"0"}, "2201E");
    expectSqlState(database, "mod", {"5", "0"}, "22012");
    expectSqlState(database, "width_bucket", {"1", "0", "10", "0"},
                   "2201G");
    expectSqlState(database, "width_bucket",
                   {"1", "0", "10", "'5x'"}, "22P02");
    expectSqlState(database, "width_bucket",
                   {"1", "0", "10", "2147483648"}, "22003");
    expectSqlState(database, "power", {"0", "-1"}, "2201F");
    expectSqlState(database, "trunc",
                   {"1.2", "999999999999999999999"}, "22003");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[MATH PROJECTION ERROR] all passed" << std::endl;
    return 0;
}
