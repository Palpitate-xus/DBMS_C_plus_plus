#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void expectValue(const std::string& database, const std::string& column,
                 const std::string& value) {
    const auto rows =
        g_engine.query(database, "typed_values", {"=id 1"}, {column});
    assert(rows.size() == 1);
    assert(rows.front() == value + " ");
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "update_type_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "typed_values";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeDateColumn("date_value", false));
    schema.append(dbms::makeTimestampColumn("timestamp_value", false));
    schema.append(dbms::makeTimestamptzColumn("timestamptz_value", false));
    schema.append(dbms::makeDateTimeColumn("datetime_value", false));
    schema.append(dbms::makeTimeColumn("time_value", false));
    schema.append(dbms::makeFloatColumn("float_value", false));
    schema.append(dbms::makeDoubleColumn("double_value", false));
    schema.append(dbms::makeDecimalColumn("numeric_value", false, 20, 4));
    schema.append(dbms::makeJsonColumn("json_value", false));
    schema.append(dbms::makeJsonbColumn("jsonb_value", false));
    dbms::Column enumValue =
        dbms::makeVarCharColumn("enum_value", false, 16);
    enumValue.enumValues = {"red", "green"};
    schema.append(enumValue);
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    assert(g_engine.insert(
               database, "typed_values",
               {{"id", "1"},
                {"date_value", "2024-01-02"},
                {"timestamp_value", "2024-01-02 03:04:05"},
                {"timestamptz_value", "2024-01-02 03:04:05+00:00"},
                {"datetime_value", "2024-01-02 03:04:05"},
                {"time_value", "03:04:05"},
                {"float_value", "1.25"},
                {"double_value", "2.5"},
                {"numeric_value", "3.7500"},
                {"json_value", "{\"old\":true}"},
                {"jsonb_value", "[1,2]"},
                {"enum_value", "red"}}) == dbms::DBStatus::OK);

    assert(g_engine.update(
               database, "typed_values",
               {{"date_value", "2025-02-03"},
                {"timestamp_value", "2025-02-03 04:05:06"},
                {"timestamptz_value", "2025-02-03 04:05:06+00:00"},
                {"datetime_value", "2025-02-03 04:05:06"},
                {"time_value", "04:05:06"},
                {"float_value", "4.5"},
                {"double_value", "5.5"},
                {"numeric_value", "6.2500"},
                {"json_value", "{\"new\":true}"},
                {"jsonb_value", "[3,4]"},
                {"enum_value", "green"}},
               {"=id 1"}) == dbms::DBStatus::OK);

    expectValue(database, "date_value", "2025-02-03");
    expectValue(database, "timestamp_value", "2025-02-03 04:05:06");
    expectValue(database, "timestamptz_value", "2025-02-03 04:05:06+00");
    expectValue(database, "datetime_value", "2025-02-03 04:05:06");
    expectValue(database, "time_value", "04:05:06");
    expectValue(database, "float_value", "4.5");
    expectValue(database, "double_value", "5.5");
    expectValue(database, "numeric_value", "6.2500");
    expectValue(database, "json_value", "{\"new\":true}");
    expectValue(database, "jsonb_value", "[3,4]");
    expectValue(database, "enum_value", "green");

    const auto expectRejected = [&](const std::string& column,
                                    const std::string& invalid,
                                    const std::string& retained) {
        assert(g_engine.update(database, "typed_values", {{column, invalid}},
                               {"=id 1"}) ==
               dbms::DBStatus::INVALID_VALUE);
        expectValue(database, column, retained);
    };
    expectRejected("date_value", "not-a-date", "2025-02-03");
    expectRejected("timestamp_value", "not-a-timestamp",
                   "2025-02-03 04:05:06");
    expectRejected("time_value", "99:00:00", "04:05:06");
    expectRejected("float_value", "not-a-float", "4.5");
    expectRejected("float_value", "4.5junk", "4.5");
    expectRejected("double_value", "not-a-double", "5.5");
    expectRejected("double_value", "5.5junk", "5.5");
    expectRejected("numeric_value", "not-a-number", "6.2500");
    expectRejected("json_value", "{broken}", "{\"new\":true}");
    expectRejected("jsonb_value", "[broken]", "[3,4]");
    expectRejected("enum_value", "blue", "green");

    // Type errors belong to the statement itself and must be reported even
    // when its predicate happens to match no rows.
    assert(g_engine.update(database, "typed_values",
                           {{"numeric_value", "still-not-a-number"}},
                           {"=id 999"}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.update(database, "typed_values",
                           {{"enum_value", "blue"}}, {"=id 999"}) ==
           dbms::DBStatus::INVALID_VALUE);

    // INSERT uses the same conversion code but a different row-building
    // path for tables without variable-length columns.  Numeric prefixes
    // must not be accepted merely because std::stof/std::stod consumed a
    // valid prefix, while PostgreSQL's supported non-finite values remain
    // valid floating-point inputs.
    dbms::TableSchema floatingSchema;
    floatingSchema.tablename = "floating_values";
    floatingSchema.formatVersion = 2;
    floatingSchema.append(dbms::makeIntColumn("id", false, 4, true));
    floatingSchema.append(dbms::makeFloatColumn("float_value", false));
    floatingSchema.append(dbms::makeDoubleColumn("double_value", false));
    assert(g_engine.createTable(database, floatingSchema) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "floating_values",
                           {{"id", "1"}, {"float_value", "1.25"},
                            {"double_value", "2.5"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "floating_values",
                           {{"id", "2"}, {"float_value", "1.25junk"},
                            {"double_value", "2.5"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "floating_values",
                           {{"id", "3"}, {"float_value", "1.25"},
                            {"double_value", "2.5junk"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.query(database, "floating_values", {}, {"id"}).size() ==
           1);
    assert(g_engine.insert(database, "floating_values",
                           {{"id", "4"}, {"float_value", "Infinity"},
                            {"double_value", "NaN"}}) ==
           dbms::DBStatus::OK);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[UPDATE TYPE VALIDATION] parity with inserts OK"
              << std::endl;
    return 0;
}
