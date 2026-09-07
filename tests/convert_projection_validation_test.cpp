#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectConvert(const std::string& database,
                           const std::string& source,
                           const std::string& targetType) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = source;
    expression.isScalar = true;
    expression.funcName = "convert";
    expression.funcArgs = {source, targetType};

    const auto rows = g_engine.queryExpr(
        database, "convert_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

void expectConvertError(const std::string& database,
                        const std::string& source,
                        const std::string& targetType,
                        const std::string& sqlstate) {
    bool rejected = false;
    try {
        (void)projectConvert(database, source, targetType);
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find(
                       "SQLSTATE " + sqlstate) != std::string::npos;
    }
    assert(rejected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "convert_projection_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "convert_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("valid_text", false));
    schema.append(dbms::makeTextColumn("trailing_text", false));
    schema.append(dbms::makeTextColumn("invalid_text", false));
    schema.append(dbms::makeTextColumn("overflow_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    schema.append(dbms::makeDecimalColumn("numeric_value", false, 20, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "convert_values",
               {{"id", "1"},
                {"valid_text", "42"},
                {"trailing_text", "12x"},
                {"invalid_text", "abc"},
                {"overflow_text", "2147483648"},
                {"null_text", "NULL"},
                {"numeric_value", "3.7"}}) == dbms::DBStatus::OK);

    assert(projectConvert(database, "valid_text", "integer") == "42");
    assert(projectConvert(database, "numeric_value", "integer") == "4");
    assert(projectConvert(database, "null_text", "integer") == "NULL");
    expectConvertError(database, "trailing_text", "integer", "22P02");
    expectConvertError(database, "invalid_text", "double", "22P02");
    expectConvertError(database, "overflow_text", "integer", "22003");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[CONVERT PROJECTION VALIDATION] all passed" << std::endl;
    return 0;
}
