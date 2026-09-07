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

std::string projectRepeat(const std::string& database,
                          const std::string& value,
                          const std::string& count) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = "repeat";
    expression.isScalar = true;
    expression.funcName = "repeat";
    expression.funcArgs = {value, count};

    const auto rows = g_engine.queryExpr(
        database, "repeat_projection_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string result = rows.front();
    result.pop_back();
    return result;
}

void expectSqlState(const std::string& database,
                    const std::string& count,
                    const std::string& sqlState) {
    bool rejected = false;
    try {
        (void)projectRepeat(database, "'x'", count);
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find(
            "SQLSTATE " + sqlState) != std::string::npos;
    }
    assert(rejected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "repeat_projection_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "repeat_projection_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTextColumn("text_value", false));
    schema.append(dbms::makeTextColumn("empty_text", false));
    schema.append(dbms::makeTextColumn("literal_null", false));
    schema.append(dbms::makeTextColumn("null_text", true));
    schema.append(dbms::makeIntColumn("repeat_count", false, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    dbms::StorageEngine::SqlRow row;
    row["id"] = "1";
    row["text_value"] = "é";
    row["empty_text"] = "";
    row["literal_null"] = "NULL";
    row["null_text"] = std::nullopt;
    row["repeat_count"] = "3";
    assert(g_engine.insertRow(database, "repeat_projection_values", row) ==
           dbms::DBStatus::OK);

    assert(projectRepeat(database, "text_value", "repeat_count") ==
           "ééé");
    assert(projectRepeat(database, "empty_text", "3").empty());
    assert(projectRepeat(database, "literal_null", "2") == "NULLNULL");
    assert(projectRepeat(database, "null_text", "2") == "NULL");
    assert(projectRepeat(database, "'ab'", "0").empty());
    assert(projectRepeat(database, "'ab'", "-2").empty());
    expectSqlState(database, "'2x'", "22P02");
    expectSqlState(database, "'2147483648'", "22003");
    expectSqlState(database, "1073741824", "54000");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[REPEAT PROJECTION SEMANTICS] all passed" << std::endl;
    return 0;
}
