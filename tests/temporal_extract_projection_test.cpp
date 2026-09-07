#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

static std::string projectExtract(const std::string& database,
                                  const std::string& field,
                                  const std::string& column) {
    dbms::StorageEngine::SelectExpr expression;
    expression.displayName = field;
    expression.isScalar = true;
    expression.funcName = "extract";
    expression.funcArgs = {field, column};
    const auto rows = g_engine.queryExpr(
        database, "temporal_values", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();
    return value;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "temporal_extract_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "temporal_values";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTimestampColumn("ts", false));
    schema.append(dbms::makeTimestamptzColumn("tz", false));
    schema.append(dbms::makeTimeColumn("tm", false));
    schema.append(dbms::makeIntervalColumn("iv", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "temporal_values",
               {{"id", "1"},
                {"ts", "2024-01-01 12:34:56"},
                {"tz", "2024-01-01 00:30:00+02"},
                {"tm", "12:34:56"},
                {"iv", "1 year 2 mons 3 days 04:05:06.25"}}) ==
           dbms::DBStatus::OK);

    assert(projectExtract(database, "milliseconds", "ts") ==
           "56000.000");
    assert(projectExtract(database, "week", "ts") == "1");
    assert(projectExtract(database, "hour", "tz") == "22");
    assert(projectExtract(database, "timezone", "tz") == "0");
    assert(projectExtract(database, "hour", "tm") == "12");
    assert(projectExtract(database, "epoch", "tm") == "45296.000000");
    assert(projectExtract(database, "year", "iv") == "1");
    assert(projectExtract(database, "second", "iv") == "6.250000");

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[TEMPORAL EXTRACT PROJECTION] all passed" << std::endl;
    return 0;
}
