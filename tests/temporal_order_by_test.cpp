#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::vector<std::string> ids(std::initializer_list<const char*> values) {
    std::vector<std::string> result;
    for (const char* value : values) {
        result.emplace_back(std::string(value) + " ");
    }
    return result;
}

dbms::StorageEngine::OrderBySpec ascending(const std::string& column,
                                            bool nullsFirst = false) {
    dbms::StorageEngine::OrderBySpec order;
    order.colName = column;
    order.ascending = true;
    order.nullsFirst = nullsFirst;
    return order;
}

void assertQueryOrder(const std::string& database, const std::string& column,
                      const std::vector<std::string>& expected,
                      bool nullsFirst = false) {
    const std::vector<dbms::StorageEngine::OrderBySpec> order = {
        ascending(column, nullsFirst)};
    assert(g_engine.query(database, "events", {}, {"id"}, order) ==
           expected);

    dbms::StorageEngine::SelectExpr idExpression;
    idExpression.displayName = "id";
    idExpression.colName = "id";
    assert(g_engine.queryExpr(database, "events", {}, {idExpression}, order) ==
           expected);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "temporal_order_by";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "events";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeTimeColumn("time_value", true));
    schema.append(dbms::makeTimestampColumn("timestamp_value", true));
    schema.append(dbms::makeTimestamptzColumn("timestamptz_value", true));
    schema.append(dbms::makeDateTimeColumn("datetime_value", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    const std::vector<std::vector<std::string>> rows = {
        {"1", "09:45:00", "2024-02-01 09:45:00",
         "2024-02-01 09:45:00+08:00", "2024-02-01 09:45:00"},
        {"2", "09:05:00", "2024-02-01 09:05:00",
         "2024-02-01 01:05:00+00:00", "2024-02-01 09:05:00"},
        {"3", "02:30:00", "2023-12-31 23:59:00",
         "2024-01-31 23:30:00-02:00", "2023-12-31 23:59:00"},
        {"4", "NULL", "NULL", "NULL", "NULL"},
    };
    for (const auto& row : rows) {
        assert(g_engine.insert(
                   database, "events",
                   {{"id", row[0]}, {"time_value", row[1]},
                    {"timestamp_value", row[2]},
                    {"timestamptz_value", row[3]},
                    {"datetime_value", row[4]}}) == dbms::DBStatus::OK);
    }

    assertQueryOrder(database, "time_value", ids({"3", "2", "1", "4"}));
    assertQueryOrder(database, "timestamp_value",
                     ids({"3", "2", "1", "4"}));
    assertQueryOrder(database, "datetime_value",
                     ids({"3", "2", "1", "4"}));
    // TIMESTAMPTZ values compare by their normalized UTC instants.
    assertQueryOrder(database, "timestamptz_value",
                     ids({"2", "3", "1", "4"}));
    assertQueryOrder(database, "time_value",
                     ids({"4", "3", "2", "1"}), true);

    dbms::StorageEngine::OrderBySpec byMonth;
    byMonth.isExpression = true;
    byMonth.exprFunc = "date_trunc";
    byMonth.expressionSql =
        "date_trunc('month', timestamp_value)";
    assert(g_engine.query(database, "events", {}, {"id"}, {byMonth}) ==
           ids({"3", "1", "2", "4"}));
    byMonth.exprFunc = "extract";
    byMonth.expressionSql = "extract(year from timestamp_value)";
    assert(g_engine.query(database, "events", {}, {"id"}, {byMonth}) ==
           ids({"3", "1", "2", "4"}));
    byMonth.exprFunc = "date_part";
    byMonth.expressionSql = "date_part('year', timestamp_value)";
    assert(g_engine.query(database, "events", {}, {"id"}, {byMonth}) ==
           ids({"3", "1", "2", "4"}));

    dbms::TableSchema formattedSchema;
    formattedSchema.tablename = "formatted_dates";
    formattedSchema.formatVersion = 2;
    formattedSchema.append(dbms::makeTextColumn("v", false));
    assert(g_engine.createTable(database, formattedSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"01/01/2021", "01/12/2020"})
        assert(g_engine.insert(database, "formatted_dates", {{"v", value}}) ==
               dbms::DBStatus::OK);
    byMonth.exprFunc = "to_date";
    byMonth.expressionSql = "to_date(v, 'DD/MM/YYYY')";
    assert(g_engine.query(database, "formatted_dates", {}, {"v"}, {byMonth}) ==
           ids({"01/12/2020", "01/01/2021"}));
    assert(g_engine.sortByExpression(
               database, "formatted_dates",
               ids({"01/01/2021", "01/12/2020"}), {byMonth}) ==
           ids({"01/12/2020", "01/01/2021"}));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[TEMPORAL ORDER BY] chronological/timezone/null ordering OK"
              << std::endl;
    return 0;
}
