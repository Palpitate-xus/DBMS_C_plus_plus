#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectArrayLength(const std::string& database,
                               const std::string& array,
                               const std::string& dimension) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "array_length";
    expression.isScalar = true;
    expression.funcName = "array_length";
    expression.funcArgs = {array, dimension};

    const auto rows = g_engine.queryExpr(
        database, "array_length_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

void expectSqlState(const std::string& database,
                    const std::string& dimension,
                    const std::string& sqlState) {
    bool rejected = false;
    try {
        (void)projectArrayLength(database, "flat_array", dimension);
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find(
            "SQLSTATE " + sqlState) != std::string::npos;
    }
    assert(rejected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "array_length_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "array_length_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("flat_array", false));
    schema.append(dbms::makeTextColumn("matrix_array", false));
    schema.append(dbms::makeTextColumn("quoted_array", false));
    schema.append(dbms::makeTextColumn("empty_array", false));
    schema.append(dbms::makeTextColumn("null_array", true));
    schema.append(dbms::makeIntColumn("dimension", false, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["flat_array"] = "{1,2,3}";
    row["matrix_array"] = "{{1,2},{3,4},{5,6}}";
    row["quoted_array"] = "{\"a,b\",c}";
    row["empty_array"] = "{}";
    row["null_array"] = std::nullopt;
    row["dimension"] = "2";
    assert(g_engine.insertRow(database, "array_length_values", row) ==
           dbms::DBStatus::OK);

    assert(projectArrayLength(database, "flat_array", "1") == "3");
    assert(projectArrayLength(database, "matrix_array", "1") == "3");
    assert(projectArrayLength(database, "matrix_array", "2") == "2");
    assert(projectArrayLength(database, "matrix_array", "dimension") ==
           "2");
    assert(projectArrayLength(database, "matrix_array", "3") == "NULL");
    assert(projectArrayLength(database, "quoted_array", "1") == "2");
    assert(projectArrayLength(database, "empty_array", "1") == "NULL");
    assert(projectArrayLength(database, "null_array", "1") == "NULL");
    assert(projectArrayLength(database, "flat_array", "0") == "NULL");
    expectSqlState(database, "'2x'", "22P02");
    expectSqlState(database, "2147483648", "22003");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[ARRAY LENGTH PROJECTION] all passed" << std::endl;
    return 0;
}
