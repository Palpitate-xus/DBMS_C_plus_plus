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

dbms::Column integerArrayColumn(const std::string& name, bool nullable = false) {
    dbms::Column column;
    column.dataName = name;
    column.isNull = nullable;
    assert(dbms::TypeRegistry::instance().resolveColumnType(
               column, "integer", {}, true).empty());
    assert(column.dataType == "integer[]" && column.isArray &&
           column.isVariableLength && column.dsize == 4);
    return column;
}

std::string projectArrayFunction(
    const std::string& database,
    const std::string& function,
    const std::vector<std::string>& arguments) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = arguments;

    std::vector<std::string> rows;
    try {
        rows = g_engine.queryExpr(database, "array_inspection_values", {}, {expression});
    } catch (const std::runtime_error& error) {
        std::cerr << "ARRAY_PROJECTION " << function << '(';
        for (const auto& argument : arguments) std::cerr << argument << ',';
        std::cerr << "): " << error.what() << '\n';
        throw;
    }
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
    schema.append(integerArrayColumn("flat_array"));
    schema.append(integerArrayColumn("matrix_array"));
    schema.append(integerArrayColumn("null_element_array"));
    schema.append(integerArrayColumn("empty_array"));
    schema.append(integerArrayColumn("null_array", true));
    schema.append(dbms::makeTextColumn("text_array", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    const auto stored = g_engine.getTableSchema(database, "array_inspection_values");
    for (size_t i = 1; i <= 5; ++i)
        assert(stored.cols[i].isArray && stored.cols[i].dataType == "integer");
    assert(!stored.cols[6].isArray && !stored.cols[7].isArray);

    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["flat_array"] = "{10,20,30}";
    row["matrix_array"] = "{{1,2},{3,4},{5,6}}";
    row["null_element_array"] = "{10,NULL,30}";
    row["empty_array"] = "{}";
    row["null_array"] = std::nullopt;
    row["text_array"] = "{10,20,30}";
    row["null_text"] = std::nullopt;
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
    assert(projectArrayFunction(
               database, "array_position", {"empty_array", "NULL"}) ==
           "NULL");
    assert(projectArrayFunction(
               database, "array_position", {"null_array", "20"}) ==
           "NULL");
    assert(projectArrayFunction(database, "array_length", {"flat_array", "1"}) == "3");
    assert(projectArrayFunction(database, "expreval", {"array_position(flat_array,20)"}) == "2");
    assert(projectArrayFunction(database, "expreval", {"array_position(null_array,20)"}) == "NULL");
    assert(projectArrayFunction(database, "expreval", {"pg_typeof(flat_array)"}) == "integer[]");

    // Runtime SQL row declarations are not the legacy raw-literal array
    // carrier API. Both brace-looking and NULL TEXT columns stay TEXT.
    for (const auto& column : {"text_array", "null_text"}) {
        for (const auto& needle : {"20", "'20'::text"}) {
            bool rejectedText = false;
            try {
                (void)projectArrayFunction(database, "array_position", {column, needle});
            } catch (const std::runtime_error& error) {
                rejectedText = std::string(error.what()).find(
                    "SQLSTATE 42883") != std::string::npos;
            }
            assert(rejectedText);
        }
    }

    // SQL expression arguments retain their declarations: this bare literal
    // is BIGINT and has no matching three-argument signature. An explicit
    // INTEGER conversion separately tests the original 22003 codec guard.
    bool rejectedStartSignature = false;
    try {
        (void)projectArrayFunction(database, "array_position",
                                   {"flat_array", "10", "2147483648"});
    } catch (const std::runtime_error& error) {
        rejectedStartSignature = std::string(error.what()).find(
            "SQLSTATE 42883") != std::string::npos;
    }
    assert(rejectedStartSignature);

    bool rejectedStartRange = false;
    try {
        (void)projectArrayFunction(
            database, "array_position",
            {"flat_array", "10", "2147483648::integer"});
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
