#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectNullIf(const std::string& database,
                          const std::string& left,
                          const std::string& right) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "nullif";
    expression.isScalar = true;
    expression.funcName = "nullif";
    expression.funcArgs = {left, right};

    const auto rows = g_engine.queryExpr(
        database, "nullif_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "nullif_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "nullif_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeIntColumn("integer_value", false, 2));
    schema.append(dbms::makeDecimalColumn("numeric_value", false, 20, 4));
    schema.append(dbms::makeTextColumn("value_text", false));
    schema.append(dbms::makeTextColumn("same_text", false));
    schema.append(dbms::makeTextColumn("other_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    schema.append(dbms::makeTextColumn("empty_left", false));
    schema.append(dbms::makeTextColumn("empty_right", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "nullif_values",
               {{"id", "1"},
                {"integer_value", "1"},
                {"numeric_value", "1.0"},
                {"value_text", "value"},
                {"same_text", "value"},
                {"other_text", "other"},
                {"null_text", "NULL"},
                {"empty_left", ""},
                {"empty_right", ""}}) == dbms::DBStatus::OK);

    assert(projectNullIf(database, "value_text", "same_text") == "NULL");
    assert(projectNullIf(database, "value_text", "other_text") == "value");
    assert(projectNullIf(database, "null_text", "value_text") == "NULL");
    assert(projectNullIf(database, "value_text", "null_text") == "value");
    assert(projectNullIf(database, "empty_left", "empty_right") == "NULL");
    assert(projectNullIf(database, "empty_left", "value_text").empty());
    assert(projectNullIf(database, "integer_value", "numeric_value") ==
           "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[NULLIF PROJECTION SEMANTICS] all passed" << std::endl;
    return 0;
}
