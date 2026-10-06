#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    int failures = 0;
    auto check = [&](const std::string& expression, const std::string& expected,
                     const std::string& type, bool isNull = false) {
        const auto result = dbms::ExprHelper::evalStringWithNulls(
            expression, {{"tz", "UTC"}, {"nothing", ""}}, {"nothing"},
            {{"tz", "text"}, {"nothing", "text"}});
        if (!result.ok || result.isNull != isNull ||
            (!isNull && result.value != expected) ||
            dbms::ExprHelper::canonicalResultTypeName(result.typeName) != type) {
            std::cerr << expression << " got " << result.ok << '/' << result.isNull
                << '/' << result.value << '/' << result.typeName << '/' << result.error << '\n';
            ++failures;
        }
    };
    const std::string stamp = "TIMESTAMP '2026-10-06 00:00:00'";
    check(stamp + " AT TIME ZONE 'UTC'", "2026-10-06 00:00:00+00", "timestamptz");
    check(stamp + " AT TIME ZONE tz", "2026-10-06 00:00:00+00", "timestamptz");
    check(stamp + " AT TIME ZONE lower(tz)", "2026-10-06 00:00:00+00", "timestamptz");
    check(stamp + " AT TIME ZONE ('U'||'TC')", "2026-10-06 00:00:00+00", "timestamptz");
    check(stamp + " AT TIME ZONE CASE WHEN TRUE THEN tz ELSE 'bad' END", "2026-10-06 00:00:00+00", "timestamptz");
    check(stamp + " AT TIME ZONE coalesce(nothing,tz)", "2026-10-06 00:00:00+00", "timestamptz");
    check(stamp + " AT TIME ZONE nothing", "", "timestamptz", true);
    check("CAST(NULL AS TIMESTAMP) AT TIME ZONE 'UTC'", "", "timestamptz", true);
    check("CAST(NULL AS TIMESTAMPTZ) AT TIME ZONE 'UTC'", "", "timestamp", true);
    check(stamp + " AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'", "2026-10-05 20:00:00", "timestamp");
    check("('2026-10-06 00:00:00'::timestamp AT TIME ZONE 'UTC')::text", "2026-10-06 00:00:00+00", "text");
    check(stamp + " AT TIME ZONE 'UTC' + INTERVAL '1 hour'", "2026-10-06 01:00:00+00", "timestamptz");
    check(stamp + " AT TIME ZONE 'UTC' || ':done'", "2026-10-06 00:00:00+00:done", "text");
    check(stamp + " AT TIME ZONE INTERVAL '02:00'", "2026-10-05 22:00:00+00", "timestamptz");
    check(stamp + " AT TIME ZONE INTERVAL '00:00:30'", "2026-10-05 23:59:30+00", "timestamptz");
    check("timezone('UTC','2026-10-06 00:00:00')", "2026-10-06 00:00:00", "timestamp");
    for (const auto& item : std::vector<std::pair<std::string, std::string>>{
        {stamp + " AT TIME ZONE 'NoSuchZone'", "22023"},
        {stamp + " AT TIME ZONE 'U'||'TC'", "22023"},
        {stamp + " AT TIME ZONE 7", "42883"},
        {stamp + " AT TIME ZONE CAST(NULL AS INT)", "42883"},
        {"CAST(NULL AS INT) AT TIME ZONE tz", "42883"},
        {"CAST('2026-10-06 00:00:00' AS TEXT) AT TIME ZONE tz", "42883"},
        {stamp + " AT TIME ZONE INTERVAL '1 day'", "22023"}
    }) {
        // These are type/error controls with a declared text zone, not tests
        // of an unresolved column. PostgreSQL rejects a missing tz first.
        const auto result = dbms::ExprHelper::evalStringWithNulls(
            item.first, {{"tz", "UTC"}}, {}, {{"tz", "text"}});
        if (result.ok || result.error.find("SQLSTATE " + item.second) == std::string::npos) {
            std::cerr << item.first << " expected " << item.second << " got " << result.error << '\n';
            ++failures;
        }
    }
    for (const std::string expression : {
             "CAST(NULL AS INT) AT TIME ZONE tz",
             "CAST('2026-10-06 00:00:00' AS TEXT) AT TIME ZONE tz"}) {
        const auto result = dbms::ExprHelper::evalString(expression, {});
        if (result.ok || result.error.find("SQLSTATE 42703") == std::string::npos) {
            std::cerr << expression << " expected missing-column 42703 got " << result.error << '\n';
            ++failures;
        }
    }
    if (failures) return 1;
    std::cout << "[PLPGSQL TIMEZONE VALUE OPERAND] passed\n";
}
