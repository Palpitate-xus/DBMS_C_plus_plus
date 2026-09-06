#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    // Formatting must obey the same four-digit civil-date domain accepted
    // by the parser. Huge finite timestamp values previously wrapped the
    // computed year through int and/or returned a silently truncated date.
    assert(str(Date(9999, 12, 31)) == "9999-12-31");
    assert(str(Date(std::numeric_limits<int>::max(), 1, 1)).empty());
    assert(str(dateAddMonths(Date(2024, 1, 31), 1)) == "2024-02-29");
    assert(str(dateAddMonths(
                   Date(2024, 1, 31),
                   std::numeric_limits<int64_t>::max())).empty());
    assert(str(dateAddYears(
                   Date(2024, 1, 31),
                   std::numeric_limits<int64_t>::max())).empty());
    assert(formatTimestampSeconds(
               parseTimestampToSeconds("9999-12-31 23:59:59")) ==
           "9999-12-31 23:59:59");
    assert(formatTimestampSeconds(
               std::numeric_limits<int64_t>::max() - 1).empty());
    assert(formatTimestampWithTz(
               std::numeric_limits<int64_t>::max() - 1, 60).empty());
    assert(formatTimestampWithTz(
               parseTimestampToSeconds("2024-01-01 00:00:00"),
               std::numeric_limits<int>::min()).empty());

    const std::string testName = "temporal_literal_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "events";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeDateColumn("date_value", true));
    schema.append(dbms::makeTimeColumn("time_value", true));
    schema.append(dbms::makeTimestampColumn("timestamp_value", true));
    schema.append(dbms::makeTimestamptzColumn("timestamptz_value", true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    assert(g_engine.insert(
               database, "events",
               {{"id", "1"},
                {"date_value", "2024-02-29"},
                {"time_value", "23:59:59"},
                {"timestamp_value", "2024-02-29 23:59:59"},
                {"timestamptz_value", "2024-02-29 23:59:59+15:59"}}) ==
           dbms::DBStatus::OK);

    const std::vector<std::pair<std::string, std::string>> invalidValues = {
        {"date_value", "2024-00-01"},
        {"date_value", "2024-01-00"},
        {"date_value", "2023-02-29"},
        {"date_value", "2024-aa-01"},
        {"time_value", "24:00:00"},
        {"time_value", "12:60:00"},
        {"time_value", "12:00:60"},
        {"time_value", "0;:00:00"},
        {"timestamp_value", "2024-01-01 garbage"},
        {"timestamp_value", "2024-01-01 24:00:00"},
        {"timestamp_value", "2024-01-01 12:60:00"},
        {"timestamp_value", "2024-01-01 12:00:60"},
        {"timestamp_value", "2024-01-01 0;:00:00"},
        {"timestamptz_value", "2024-01-01 12:00:00+16:00"},
        {"timestamptz_value", "2024-01-01 12:00:00+08:60"},
        {"timestamptz_value", "2024-01-01 12:00:00+ab"},
        {"timestamptz_value", "2024-01-01 12:00:00+08:00Z"},
    };

    int id = 2;
    for (const auto& [column, value] : invalidValues) {
        assert(g_engine.insert(database, "events",
                               {{"id", std::to_string(id++)},
                                {column, value}}) ==
               dbms::DBStatus::INVALID_VALUE);
        assert(g_engine.update(database, "events", {{column, value}},
                               {"=id 1"}) ==
               dbms::DBStatus::INVALID_VALUE);
    }

    assert(g_engine.query(database, "events", {}, {"id"}).size() == 1);
    assert(g_engine.query(database, "events", {"=id 1"}, {"date_value"}) ==
           std::vector<std::string>{"2024-02-29 "});
    assert(g_engine.query(database, "events", {"=id 1"}, {"time_value"}) ==
           std::vector<std::string>{"23:59:59 "});
    assert(g_engine.query(database, "events", {"=id 1"},
                          {"timestamp_value"}) ==
           std::vector<std::string>{"2024-02-29 23:59:59 "});

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[TEMPORAL LITERAL VALIDATION] strict bounds and digits OK"
              << std::endl;
    return 0;
}
