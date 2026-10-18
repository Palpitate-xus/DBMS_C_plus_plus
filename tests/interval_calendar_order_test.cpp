// Calendar fields are applied months, days, then microseconds.
#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

namespace {
int checks = 0, failures = 0;

void expect(const std::string& expression, const std::string& value,
            const std::string& type, bool isNull = false) {
    const auto result = dbms::ExprHelper::evalString(expression, {});
    ++checks;
    const bool good = result.ok && result.isNull == isNull &&
        (isNull || (result.value == value && result.typeName == type));
    if (!good) {
        ++failures;
        std::cerr << "CALENDAR_NATIVE_FAILURE " << checks << ' ' << expression
                  << " actual=" << result.value << " type=" << result.typeName
                  << " null=" << result.isNull << " state=" << result.sqlState
                  << " expected=" << value << " type=" << type << '\n';
    }
}

int monthDays(int year, int month) {
    static constexpr int lengths[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    return lengths[month - 1] + (month == 2 &&
        (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)));
}

// Independent test oracle: clamp the destination month, then let libc's UTC
// calendar normalize the day displacement. No production shift/codec is used.
std::string shifted(int year, int month, int day, int months, int days,
                    bool dateOnly, bool zoned) {
    const int index = year * 12 + month - 1 + months;
    year = index / 12;
    month = index % 12 + 1;
    day = std::min(day, monthDays(year, month));
    std::tm point{};
    point.tm_year = year - 1900;
    point.tm_mon = month - 1;
    point.tm_mday = day + days;
    if (!dateOnly) {
        point.tm_hour = 12;
        point.tm_min = 34;
        point.tm_sec = 56;
    }
    const std::time_t seconds = timegm(&point);
    std::tm utc{};
    gmtime_r(&seconds, &utc);
    char buffer[64];
    std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d %02d:%02d:%02d",
                  utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday,
                  utc.tm_hour, utc.tm_min, utc.tm_sec);
    return std::string(buffer) + (dateOnly ? "" : ".000001") +
           (zoned ? "+00" : "");
}

std::string input(int year, int month, int day, const std::string& kind) {
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year, month, day);
    return kind + " '" + buffer +
        (kind == "date" ? "" : " 12:34:56.000001") +
        (kind == "timestamptz" ? "+00" : "") + "'";
}
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::vector<std::tuple<int,int,int>> dates = {
        {2023,1,30}, {2024,1,30}, {2024,3,31}, {1900,1,30},
        {2000,1,30}, {2100,1,30}, {2024,12,30}, {2024,8,31},
        {2024,2,29}
    };
    for (const auto& [year,month,day] : dates) {
        for (const int months : {-13,-1,0,1,13}) {
            for (const int days : {-31,-1,0,1,31}) {
                const std::string interval = "INTERVAL '" +
                    std::to_string(months) + " months " +
                    std::to_string(days) + " days'";
                for (const std::string kind : {"date","timestamp","timestamptz"}) {
                    const bool dateOnly = kind == "date";
                    const bool zoned = kind == "timestamptz";
                    const auto source = input(year,month,day,kind);
                    const std::string type = zoned ? "timestamptz" : "timestamp";
                    const auto added = shifted(year,month,day,months,days,dateOnly,zoned);
                    expect(source + "+" + interval, added, type);
                    expect(source + "-" + interval,
                           shifted(year,month,day,-months,-days,dateOnly,zoned), type);
                    expect(interval + "+" + source, added, type);
                }
            }
        }
    }
    for (const auto& [expression,value] :
         std::vector<std::pair<std::string,std::string>>{
        {"TIMESTAMP '2024-01-30 23:59:59.999999'+INTERVAL '1 month 1 day 1 microsecond'",
         "2024-03-02 00:00:00"},
        {"TIMESTAMP '2024-03-31'-INTERVAL '1 month 1 day 1 microsecond'",
         "2024-02-27 23:59:59.999999"},
        {"TIMESTAMP '2024-01-30'+INTERVAL '1 month 1 day -1 microsecond'",
         "2024-02-29 23:59:59.999999"},
        {"TIMESTAMP '2023-01-30'+INTERVAL '1 month 1 day 24 hours'",
         "2023-03-02 00:00:00"},
        {"TIMESTAMP '2024-01-30 24:00:00'+INTERVAL '1 month -1 day'",
         "2024-02-28 00:00:00"},
        {"TIMESTAMP '2024-01-30'+INTERVAL '1 month 1 day'-INTERVAL '1 month 1 day'",
         "2024-01-31 00:00:00"},
        {"TIMESTAMP '2024-01-30'+(INTERVAL '1 month'+INTERVAL '1 day')",
         "2024-03-01 00:00:00"},
        {"TIMESTAMP '2024-01-30'-(INTERVAL '-1 month'-INTERVAL '1 day')",
         "2024-03-01 00:00:00"}
    }) expect(expression,value,"timestamp");

    for (const std::string kind : {"timestamp","timestamptz"}) {
        expect("CAST(NULL AS "+kind+")+INTERVAL '1 month 1 day'","",kind,true);
        expect("CAST(NULL AS "+kind+")-INTERVAL '1 month 1 day'","",kind,true);
        expect(input(2024,1,30,kind)+"+CAST(NULL AS interval)","",kind,true);
    }
    // Preserve the project's pre-existing finite civil domain behavior.
    for (const std::string expression : {
        "TIMESTAMP '9999-12-31 23:59:59'+INTERVAL '1 second'",
        "TIMESTAMP '0001-01-01'-INTERVAL '1 day'",
        "TIMESTAMP '9999-12-31'+INTERVAL '1 month'",
        "TIMESTAMP '0001-01-01'-INTERVAL '1 month'"
    }) expect(expression,"","timestamp",true);

    constexpr int expected = 2043;
    std::cout << "CALENDAR_NATIVE_COMPLETE " << checks << " EXPECTED "
              << expected << " FAILURES " << failures << '\n';
    return checks == expected && failures == 0 ? 0 : 1;
}
