#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

static std::string utcTimestampNow() {
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
    ::gmtime_r(&now, &utc);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &utc);
    return std::string(buffer) + "+00";
}

static std::string projectedValue(
    const std::string& database,
    const dbms::StorageEngine::SelectExpr& expression) {
    const auto rows = g_engine.queryExpr(
        database, "clock_source", {}, {expression});
    assert(rows.size() == 1 && !rows.front().empty());
    std::string value = rows.front();
    value.pop_back();  // queryExpr's column separator
    return value;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "current_time_projection";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "clock_source";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "clock_source", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    dbms::StorageEngine::SelectExpr nowExpression;
    nowExpression.displayName = "now";
    nowExpression.isScalar = true;
    nowExpression.funcName = "now";

    const std::string before = utcTimestampNow();
    const std::string nowValue = projectedValue(database, nowExpression);
    const std::string after = utcTimestampNow();
    assert(nowValue == before || nowValue == after);

    dbms::StorageEngine::SelectExpr currentTimestamp = nowExpression;
    currentTimestamp.displayName = "current_timestamp";
    currentTimestamp.funcName = "current_timestamp";
    const std::string timestampValue =
        projectedValue(database, currentTimestamp);
    assert(timestampValue == before || timestampValue == after ||
           timestampValue == utcTimestampNow());

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[CURRENT TIME PROJECTION] runtime clock OK" << std::endl;
    return 0;
}
