#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectCast(const std::string& database,
                        const std::string& column,
                        const std::string& targetType) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = column;
    expression.isScalar = true;
    expression.funcName = "cast";
    expression.funcArgs = {column, targetType};

    const auto rows = g_engine.queryExpr(
        database, "cast_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

void expectCastError(const std::string& database,
                     const std::string& column,
                     const std::string& targetType,
                     const std::string& sqlstate) {
    bool rejected = false;
    try {
        (void)projectCast(database, column, targetType);
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find(
                       "SQLSTATE " + sqlstate) != std::string::npos;
    }
    assert(rejected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "cast_projection_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "cast_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("valid_text", false));
    schema.append(dbms::makeTextColumn("decimal_text", false));
    schema.append(dbms::makeTextColumn("trailing_text", false));
    schema.append(dbms::makeTextColumn("invalid_text", false));
    schema.append(dbms::makeTextColumn("overflow_text", false));
    schema.append(dbms::makeTextColumn("valid_float_text", false));
    schema.append(dbms::makeTextColumn("float_overflow_text", false));
    schema.append(dbms::makeTextColumn("invalid_date_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    schema.append(dbms::makeDecimalColumn("numeric_value", false, 20, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "cast_values",
               {{"id", "1"},
                {"valid_text", "42"},
                {"decimal_text", "3.7"},
                {"trailing_text", "12x"},
                {"invalid_text", "abc"},
                {"overflow_text", "2147483648"},
                {"valid_float_text", "1.25"},
                {"float_overflow_text", "1e1000"},
                {"invalid_date_text", "2023-02-29"},
                {"null_text", "NULL"},
                {"numeric_value", "3.7"}}) == dbms::DBStatus::OK);

    assert(projectCast(database, "valid_text", "integer") == "42");
    assert(projectCast(database, "numeric_value", "integer") == "4");
    assert(projectCast(database, "(numeric_value + 0.8)", "integer") ==
           "5");
    assert(projectCast(database, "numeric_value", "numeric(4,2)") ==
           "3.70");
    assert(projectCast(database, "valid_float_text", "double") == "1.25");
    assert(projectCast(database, "valid_text", "char") == "4");
    assert(projectCast(database, "null_text", "integer") == "NULL");
    expectCastError(database, "decimal_text", "integer", "22P02");
    expectCastError(database, "trailing_text", "integer", "22P02");
    expectCastError(database, "invalid_text", "integer", "22P02");
    expectCastError(database, "overflow_text", "integer", "22003");
    expectCastError(database, "trailing_text", "double", "22P02");
    expectCastError(database, "float_overflow_text", "double", "22003");
    expectCastError(database, "invalid_text", "numeric", "22P02");
    expectCastError(database, "invalid_date_text", "date", "22008");
    expectCastError(database, "'128'", "tinyint", "22003");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[CAST PROJECTION VALIDATION] all passed" << std::endl;
    return 0;
}
