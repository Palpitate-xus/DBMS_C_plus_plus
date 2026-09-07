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

std::string projectArrayFunction(
    const std::string& database,
    const std::string& function,
    const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    const auto rows = g_engine.queryExpr(
        database, "array_inspection_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "array_inspection_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "array_inspection_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("flat_array", false));
    schema.append(dbms::makeTextColumn("matrix_array", false));
    schema.append(dbms::makeTextColumn("null_element_array", false));
    schema.append(dbms::makeTextColumn("empty_array", false));
    schema.append(dbms::makeTextColumn("null_array", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["flat_array"] = "{10,20,30}";
    row["matrix_array"] = "{{1,2},{3,4},{5,6}}";
    row["null_element_array"] = "{10,NULL,30}";
    row["empty_array"] = "{}";
    row["null_array"] = std::nullopt;
    assert(g_engine.insertRow(database, "array_inspection_values", row) ==
           dbms::DBStatus::OK);

    assert(projectArrayFunction(database, "array_dims", {"flat_array"}) ==
           "[1:3]");
    assert(projectArrayFunction(database, "array_dims", {"matrix_array"}) ==
           "[1:3][1:2]");
    assert(projectArrayFunction(database, "cardinality", {"matrix_array"}) ==
           "6");
    assert(projectArrayFunction(database, "cardinality", {"empty_array"}) ==
           "0");
    assert(projectArrayFunction(database, "cardinality", {"null_array"}) ==
           "NULL");
    assert(projectArrayFunction(
               database, "array_position", {"flat_array", "20"}) == "2");
    assert(projectArrayFunction(
               database, "array_position", {"flat_array", "99"}) ==
           "NULL");
    assert(projectArrayFunction(
               database, "array_position", {"null_element_array", "NULL"}) ==
           "2");
    assert(projectArrayFunction(
               database, "array_position", {"flat_array", "10", "-2"}) ==
           "1");

    bool rejectedStartRange = false;
    try {
        (void)projectArrayFunction(
            database, "array_position",
            {"flat_array", "10", "2147483648"});
    } catch (const std::runtime_error& error) {
        rejectedStartRange = std::string(error.what()).find(
            "SQLSTATE 22003") != std::string::npos;
    }
    assert(rejectedStartRange);

    bool rejectedMultidimensional = false;
    try {
        (void)projectArrayFunction(
            database, "array_position", {"matrix_array", "1"});
    } catch (const std::runtime_error& error) {
        rejectedMultidimensional = std::string(error.what()).find(
            "SQLSTATE 0A000") != std::string::npos;
    }
    assert(rejectedMultidimensional);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[ARRAY INSPECTION PROJECTION] all passed" << std::endl;
    return 0;
}
