#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string projectLength(const std::string& database,
                          const std::string& function,
                          const std::string& argument) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = function;
    expression.isScalar = true;
    expression.funcName = function;
    expression.funcArgs = {argument};

    const auto rows = g_engine.queryExpr(
        database, "length_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "length_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "length_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("text_value", false));
    schema.append(dbms::makeVarBinaryColumn("binary_value", false, 32));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "length_values",
                           {{"id", "1"},
                            {"text_value", "aé中"},
                            {"binary_value", "aé中"},
                            {"empty_text", ""},
                            {"null_text", "NULL"}}) ==
           dbms::DBStatus::OK);

    assert(projectLength(database, "length", "'aé中'") == "3");
    assert(projectLength(database, "length", "text_value") == "3");
    assert(projectLength(database, "char_length", "text_value") == "3");
    assert(projectLength(database, "character_length", "text_value") ==
           "3");
    assert(projectLength(database, "octet_length", "text_value") == "6");
    assert(projectLength(database, "bit_length", "text_value") == "48");
    assert(projectLength(database, "length", "binary_value") == "6");
    assert(projectLength(database, "length", "empty_text") == "0");
    assert(projectLength(database, "length", "null_text") == "NULL");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[LENGTH PROJECTION SEMANTICS] all passed" << std::endl;
    return 0;
}
