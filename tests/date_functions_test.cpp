// ============================================================================
// Date/time function library test — Phase 4 Wave 4.19c
// Exercises the expanded date/time functions in ExprEvaluator: extract /
// date_part (year..second, quarter, dow/isodow, doy, epoch, century, ...),
// make_date / make_time / make_timestamp, date_trunc, and the reference
// "current" timestamp family.
// ============================================================================

#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "common/DbError.h"
#include "parser/ast.h"
#include <cassert>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

static dbms::ExprValue callFn(dbms::ExprEvaluator& eval, const std::string& name,
                              const std::vector<dbms::ExprValue>& args) {
    dbms::FunctionCallExpr call;
    call.funcName = name;
    for (const auto& a : args) {
        auto lit = std::make_unique<dbms::LiteralExpr>();
        lit->value = a.isNull ? "null" : a.value;
        lit->typeName = a.isNull ? "null" : a.typeName;
        call.args.push_back(std::move(lit));
    }
    dbms::RowContext ctx;
    return eval.eval(&call, ctx);
}

static dbms::ExprValue F(const std::string& v) { return dbms::ExprValue("text", v, false); }
static dbms::ExprValue D(const std::string& v) { return dbms::ExprValue("date", v, false); }
static dbms::ExprValue TS(const std::string& v) { return dbms::ExprValue("timestamp", v, false); }
static dbms::ExprValue TSTZ(const std::string& v) { return dbms::ExprValue("timestamptz", v, false); }
static dbms::ExprValue TIME(const std::string& v) { return dbms::ExprValue("time", v, false); }
static dbms::ExprValue TIMETZ(const std::string& v) { return dbms::ExprValue("timetz", v, false); }
static dbms::ExprValue IV(const std::string& v) { return dbms::ExprValue("interval", v, false); }
static dbms::ExprValue I(int64_t v) { return dbms::ExprValue("integer", std::to_string(v), false); }
static dbms::ExprValue NullDate() { return dbms::ExprValue("date", "", true); }

static std::string utcNow(const char* format) {
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
    ::gmtime_r(&now, &utc);
    char buffer[40];
    std::strftime(buffer, sizeof(buffer), format, &utc);
    return buffer;
}

static void test_extract_date_part() {
    dbms::ExprEvaluator eval;
    auto expectExtractError = [&](const std::string& field,
                                  const dbms::ExprValue& source,
                                  const std::string& sqlstate) {
        bool rejected = false;
        try {
            (void)callFn(eval, "extract", {F(field), source});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find(
                           "SQLSTATE " + sqlstate) != std::string::npos;
        }
        assert(rejected);
    };
    auto ts = TS("2026-06-26 14:35:09");
    assert(callFn(eval, "extract", {F("year"), ts}).value == "2026");
    assert(callFn(eval, "extract", {F("month"), ts}).value == "6");
    assert(callFn(eval, "extract", {F("day"), ts}).value == "26");
    assert(callFn(eval, "extract", {F("hour"), ts}).value == "14");
    assert(callFn(eval, "extract", {F("minute"), ts}).value == "35");
    assert(callFn(eval, "extract", {F("second"), ts}).value == "9");
    assert(callFn(eval, "extract", {F("milliseconds"), ts}).value ==
           "9000.000");
    assert(callFn(eval, "extract", {F("microseconds"), ts}).value ==
           "9000000");
    auto fractionalTs = TS("2024-01-01 12:34:56.123456");
    assert(callFn(eval, "extract", {F("second"), fractionalTs}).value ==
           "56.123456");
    assert(callFn(eval, "extract", {F("milliseconds"), fractionalTs}).value ==
           "56123.456");
    assert(callFn(eval, "extract", {F("microseconds"), fractionalTs}).value ==
           "56123456");
    assert(callFn(eval, "date_part", {F("second"), fractionalTs}).value ==
           "56.123456");
    assert(callFn(eval, "extract",
                  {F("microseconds"),
                   TS("2024-01-01 00:00:00.1234565")}).value ==
           "123456");
    assert(callFn(eval, "extract",
                  {F("microseconds"),
                   TS("2024-01-01 00:00:00.1234575")}).value ==
           "123458");
    assert(callFn(eval, "extract", {F("quarter"), ts}).value == "2");
    // date_part is an alias of extract.
    assert(callFn(eval, "date_part", {F("year"), ts}).value == "2026");
    assert(callFn(eval, "extract",
                  {F("dow"), TS("2026-13-01 14:35:09")}).isNull);
    assert(callFn(eval, "extract",
                  {F("month"), TS("2026-02-30 14:35:09")}).isNull);
    assert(callFn(eval, "extract",
                  {F("hour"), TS("2026-06-26 24:00:00")}).isNull);
    assert(callFn(eval, "date_part",
                  {F("year"), TS("not-a-timestamp")}).isNull);
    assert(callFn(eval, "extract",
                  {F("year"), TS("infinity")}).value == "Infinity");
    assert(callFn(eval, "extract",
                  {F("month"), TS("infinity")}).isNull);
    expectExtractError("hour", D("2026-06-26"), "0A000");
    expectExtractError("timezone", ts, "0A000");
    expectExtractError("not_a_unit", ts, "22023");
    assert(callFn(eval, "extract",
                  {F("timezone"), TSTZ("2026-06-26 14:35:09+00")}).value ==
           "0");
    assert(callFn(eval, "extract",
                  {F("timezone_hour"),
                   TSTZ("2026-06-26 14:35:09+00")}).value == "0");
    auto offsetTimestamp = TSTZ("2024-01-01 00:30:00+02");
    assert(callFn(eval, "extract",
                  {F("year"), offsetTimestamp}).value == "2023");
    assert(callFn(eval, "extract",
                  {F("month"), offsetTimestamp}).value == "12");
    assert(callFn(eval, "extract",
                  {F("day"), offsetTimestamp}).value == "31");
    assert(callFn(eval, "extract",
                  {F("hour"), offsetTimestamp}).value == "22");
    assert(callFn(eval, "extract",
                  {F("dow"), offsetTimestamp}).value == "0");
    assert(callFn(eval, "extract",
                  {F("week"), offsetTimestamp}).value == "52");
    assert(callFn(eval, "extract",
                  {F("isoyear"), offsetTimestamp}).value == "2023");

    auto time = TIME("12:34:56.123456");
    assert(callFn(eval, "extract", {F("hour"), time}).value == "12");
    assert(callFn(eval, "extract", {F("minute"), time}).value == "34");
    assert(callFn(eval, "extract", {F("second"), time}).value ==
           "56.123456");
    assert(callFn(eval, "extract", {F("milliseconds"), time}).value ==
           "56123.456");
    assert(callFn(eval, "extract", {F("microseconds"), time}).value ==
           "56123456");
    assert(callFn(eval, "extract", {F("epoch"), time}).value ==
           "45296.123456");
    assert(callFn(eval, "date_part", {F("second"), time}).value ==
           "56.123456");
    assert(callFn(eval, "extract",
                  {F("epoch"), TIME("24:00:00")}).value ==
           "86400.000000");
    expectExtractError("year", time, "0A000");
    expectExtractError("timezone", time, "0A000");

    auto zonedTime = TIMETZ("12:34:56.123456+05:30");
    assert(callFn(eval, "extract", {F("hour"), zonedTime}).value == "12");
    assert(callFn(eval, "extract", {F("timezone"), zonedTime}).value ==
           "19800");
    assert(callFn(eval, "extract",
                  {F("timezone_hour"), zonedTime}).value == "5");
    assert(callFn(eval, "extract",
                  {F("timezone_minute"), zonedTime}).value == "30");
    assert(callFn(eval, "extract", {F("epoch"), zonedTime}).value ==
           "25496.123456");
    auto negativeZonedTime = TIMETZ("12:34:56.123456-05:30");
    assert(callFn(eval, "extract",
                  {F("timezone_hour"), negativeZonedTime}).value == "-5");
    assert(callFn(eval, "extract",
                  {F("timezone_minute"), negativeZonedTime}).value ==
           "-30");
    assert(callFn(eval, "extract",
                  {F("epoch"), negativeZonedTime}).value ==
           "65096.123456");

    auto interval = IV("1 year 2 mons 3 days 04:05:06.25");
    assert(callFn(eval, "extract", {F("year"), interval}).value == "1");
    assert(callFn(eval, "extract", {F("month"), interval}).value == "2");
    assert(callFn(eval, "extract", {F("day"), interval}).value == "3");
    assert(callFn(eval, "extract", {F("hour"), interval}).value == "4");
    assert(callFn(eval, "extract", {F("minute"), interval}).value == "5");
    assert(callFn(eval, "extract", {F("second"), interval}).value ==
           "6.250000");
    assert(callFn(eval, "extract", {F("milliseconds"), interval}).value ==
           "6250.000");
    assert(callFn(eval, "extract", {F("microseconds"), interval}).value ==
           "6250000");
    assert(callFn(eval, "extract",
                  {F("second"), IV("00:00:00.1234565")}).value ==
           "0.123456");
    assert(callFn(eval, "extract",
                  {F("second"), IV("00:00:00.1234575")}).value ==
           "0.123458");
    assert(callFn(eval, "extract",
                  {F("second"), IV("00:00:00.9999999")}).value == "1");
    assert(callFn(eval, "extract",
                  {F("second"), IV("0.1234567 seconds")}).value ==
           "0.123457");
    assert(callFn(eval, "extract",
                  {F("second"), IV("-0.1234567 seconds")}).value ==
           "-0.123457");
    assert(callFn(eval, "extract",
                  {F("quarter"), IV("14 months")}).value == "1");
    assert(callFn(eval, "extract",
                  {F("decade"), IV("25 years")}).value == "2");
    assert(callFn(eval, "extract",
                  {F("century"), IV("2345 years")}).value == "23");
    assert(callFn(eval, "extract",
                  {F("millennium"), IV("2345 years")}).value == "2");
    expectExtractError("dow", interval, "0A000");
    expectExtractError("not_a_unit", interval, "22023");
    std::cout << "[DATEFN] extract/date_part OK" << std::endl;
}

static void test_dow_doy_century() {
    dbms::ExprEvaluator eval;
    // 2026-06-26 is a Friday.
    assert(callFn(eval, "extract", {F("dow"), F("2026-06-26")}).value == "5");    // 0=Sun..6=Sat
    assert(callFn(eval, "extract", {F("isodow"), F("2026-06-26")}).value == "5"); // 1=Mon..7=Sun
    // 2024-01-01 is a Monday.
    assert(callFn(eval, "extract", {F("dow"), F("2024-01-01")}).value == "1");
    // day-of-year: Jan 1 -> 1, Feb 1 -> 32 (2026 not a leap year).
    assert(callFn(eval, "extract", {F("doy"), F("2026-01-01")}).value == "1");
    assert(callFn(eval, "extract", {F("doy"), F("2026-02-01")}).value == "32");
    assert(callFn(eval, "extract", {F("week"), F("2021-01-01")}).value ==
           "53");
    assert(callFn(eval, "extract",
                  {F("isoyear"), F("2021-01-01")}).value == "2020");
    assert(callFn(eval, "extract", {F("week"), F("2021-01-04")}).value ==
           "1");
    assert(callFn(eval, "extract",
                  {F("isoyear"), F("2021-01-04")}).value == "2021");
    assert(callFn(eval, "extract",
                  {F("julian"), TS("2000-01-01 00:00:00")}).value ==
           "2451545.000000");
    assert(callFn(eval, "extract",
                  {F("julian"), TS("2000-01-01 12:00:00")}).value ==
           "2451545.500000");
    assert(callFn(eval, "extract",
                  {F("julian"),
                   TS("2000-01-01 00:00:00.5")}).value ==
           "2451545.000006");
    // century / millennium.
    assert(callFn(eval, "extract", {F("century"), F("2026-06-26")}).value == "21");
    assert(callFn(eval, "extract", {F("decade"), F("2026-06-26")}).value == "202");
    std::cout << "[DATEFN] dow/doy/century OK" << std::endl;
}

static void test_epoch() {
    dbms::ExprEvaluator eval;
    // PG renders date_part/extract('epoch', ts) as numeric with scale 6.
    assert(callFn(eval, "extract", {F("epoch"), TS("1970-01-01 00:00:00")}).value == "0.000000");
    // One day later -> 86400 seconds.
    assert(callFn(eval, "extract", {F("epoch"), TS("1970-01-02 00:00:00")}).value == "86400.000000");
    // A specific later instant.
    assert(callFn(eval, "extract", {F("epoch"), TS("1970-01-01 01:00:00")}).value == "3600.000000");
    assert(callFn(eval, "extract",
                  {F("epoch"),
                   TS("1970-01-01 00:00:00.123456")}).value ==
           "0.123456");
    assert(callFn(eval, "extract",
                  {F("epoch"), TS("infinity")}).value == "Infinity");
    assert(callFn(eval, "extract",
                  {F("epoch"), TS("-infinity")}).value == "-Infinity");
    assert(callFn(eval, "extract", {F("epoch"), IV("3 days")}).value ==
           "259200.000000");
    for (const auto& input : std::vector<std::pair<std::string, std::string>>{
             {"9223372036854775807 months", "22015"},
             {std::string(400, '9') + " months", "22007"}}) {
        bool rejected = false;
        try { (void)callFn(eval, "extract", {F("epoch"), IV(input.first)}); }
        catch (const dbms::DbError& error) { rejected = error.sqlState() == input.second; }
        assert(rejected);
    }
    std::cout << "[DATEFN] epoch OK" << std::endl;
}

static void test_make() {
    dbms::ExprEvaluator eval;
    auto expectFieldError = [&](const std::string& function,
                                const std::vector<dbms::ExprValue>& args) {
        bool rejected = false;
        try {
            (void)callFn(eval, function, args);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22008") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    assert(callFn(eval, "make_date", {I(2026), I(6), I(26)}).value == "2026-06-26");
    assert(callFn(eval, "make_time", {I(14), I(5), I(9)}).value == "14:05:09");
    assert(callFn(eval, "make_timestamp", {I(2026), I(6), I(26), I(14), I(5), I(9)}).value
           == "2026-06-26 14:05:09");
    assert(callFn(eval, "make_time",
                  {I(14), I(5),
                   dbms::ExprValue("numeric", "9.25", false)}).value ==
           "14:05:09.25");
    assert(callFn(eval, "make_timestamp",
                  {I(2026), I(6), I(26), I(14), I(5),
                   dbms::ExprValue("numeric", "9.125", false)}).value ==
           "2026-06-26 14:05:09.125");
    expectFieldError("make_date", {I(2026), I(13), I(1)});
    expectFieldError("make_date", {I(2023), I(2), I(29)});
    assert(callFn(eval, "make_date", {I(2024), I(2), I(29)}).value ==
           "2024-02-29");
    expectFieldError(
        "make_date",
        {I(std::numeric_limits<int64_t>::max()), I(1), I(1)});

    assert(callFn(eval, "make_time", {I(12), I(0), I(60)}).value ==
           "12:01:00");
    assert(callFn(eval, "make_time", {I(23), I(59), I(60)}).value ==
           "24:00:00");
    assert(callFn(eval, "make_time", {I(24), I(0), I(0)}).value ==
           "24:00:00");
    assert(callFn(eval, "make_timestamp",
                  {I(2024), I(2), I(28), I(23), I(59), I(60)}).value ==
           "2024-02-29 00:00:00");
    const auto rolledTimestamp = callFn(
        eval, "make_timestamp",
        {I(2023), I(12), I(31), I(24), I(0), I(0)});
    assert(rolledTimestamp.value == "2024-01-01 00:00:00");

    expectFieldError("make_time", {I(4294967296LL), I(0), I(0)});
    expectFieldError("make_time", {I(24), I(1), I(0)});
    expectFieldError(
        "make_time",
        {I(23), I(59), dbms::ExprValue("numeric", "60.1", false)});
    expectFieldError("make_timestamp",
                     {I(2026), I(2), I(30), I(0), I(0), I(0)});
    expectFieldError(
        "make_timestamp",
        {I(std::numeric_limits<int64_t>::max()), I(1), I(1),
         I(0), I(0), I(0)});
    std::cout << "[DATEFN] make_* OK" << std::endl;
}

static void test_date_trunc() {
    dbms::ExprEvaluator eval;
    auto expectDateTruncError = [&](const std::string& field,
                                    const dbms::ExprValue& source,
                                    const std::string& sqlstate) {
        bool rejected = false;
        try {
            (void)callFn(eval, "date_trunc", {F(field), source});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find(
                           "SQLSTATE " + sqlstate) != std::string::npos;
        }
        assert(rejected);
    };
    auto ts = TS("2026-06-26 14:35:09");
    const auto truncatedYear =
        callFn(eval, "date_trunc", {F("year"), ts});
    assert(truncatedYear.value == "2026-01-01 00:00:00");
    assert(truncatedYear.typeName == "timestamp");
    assert(callFn(eval, "date_trunc", {F("millennium"), ts}).value ==
           "2001-01-01 00:00:00");
    assert(callFn(eval, "date_trunc", {F("century"), ts}).value ==
           "2001-01-01 00:00:00");
    assert(callFn(eval, "date_trunc", {F("decade"), ts}).value ==
           "2020-01-01 00:00:00");
    assert(callFn(eval, "date_trunc",
                  {F("decade"), TS("0005-06-01 00:00:00")}).isNull);
    assert(callFn(eval, "date_trunc", {F("month"), ts}).value == "2026-06-01 00:00:00");
    assert(callFn(eval, "date_trunc", {F("week"), ts}).value ==
           "2026-06-22 00:00:00");
    assert(callFn(eval, "date_trunc",
                  {F("week"), TS("2024-03-01 12:00:00")}).value ==
           "2024-02-26 00:00:00");
    assert(callFn(eval, "date_trunc",
                  {F("week"), TS("2023-01-01 12:00:00")}).value ==
           "2022-12-26 00:00:00");
    assert(callFn(eval, "date_trunc", {F("day"), ts}).value == "2026-06-26 00:00:00");
    assert(callFn(eval, "date_trunc", {F("hour"), ts}).value == "2026-06-26 14:00:00");
    assert(callFn(eval, "date_trunc", {F("minute"), ts}).value == "2026-06-26 14:35:00");
    assert(callFn(eval, "date_trunc", {F("milliseconds"), ts}).value ==
           "2026-06-26 14:35:09");
    assert(callFn(eval, "date_trunc", {F("microseconds"), ts}).value ==
           "2026-06-26 14:35:09");
    auto fractionalTs = TS("2024-01-01 12:34:56.123456");
    assert(callFn(eval, "date_trunc",
                  {F("minute"), fractionalTs}).value ==
           "2024-01-01 12:34:00");
    assert(callFn(eval, "date_trunc",
                  {F("second"), fractionalTs}).value ==
           "2024-01-01 12:34:56");
    assert(callFn(eval, "date_trunc",
                  {F("milliseconds"), fractionalTs}).value ==
           "2024-01-01 12:34:56.123");
    assert(callFn(eval, "date_trunc",
                  {F("microseconds"), fractionalTs}).value ==
           "2024-01-01 12:34:56.123456");
    const auto truncatedDate =
        callFn(eval, "date_trunc", {F("day"), D("2024-01-01")});
    assert(truncatedDate.value == "2024-01-01 00:00:00+00");
    assert(truncatedDate.typeName == "timestamptz");
    const auto truncatedZoned = callFn(
        eval, "date_trunc",
        {F("day"), TSTZ("2024-01-01 00:30:00+02")});
    assert(truncatedZoned.value == "2023-12-31 00:00:00+00");
    assert(truncatedZoned.typeName == "timestamptz");
    const auto truncatedZonedMillis = callFn(
        eval, "date_trunc",
        {F("milliseconds"),
         TSTZ("2024-01-01 00:30:56.123456+02")});
    assert(truncatedZonedMillis.value ==
           "2023-12-31 22:30:56.123+00");
    assert(truncatedZonedMillis.typeName == "timestamptz");
    auto interval = IV("1 year 2 mons 3 days 04:05:06.789123");
    const auto truncatedInterval =
        callFn(eval, "date_trunc", {F("hour"), interval});
    assert(truncatedInterval.value == "1 year 2 mons 3 days 04:00:00");
    assert(truncatedInterval.typeName == "interval");
    assert(callFn(eval, "date_trunc",
                  {F("year"), interval}).value == "1 year");
    assert(callFn(eval, "date_trunc",
                  {F("quarter"), interval}).value == "1 year");
    assert(callFn(eval, "date_trunc",
                  {F("day"), interval}).value ==
           "1 year 2 mons 3 days");
    assert(callFn(eval, "date_trunc",
                  {F("minute"), interval}).value ==
           "1 year 2 mons 3 days 04:05:00");
    assert(callFn(eval, "date_trunc",
                  {F("second"), interval}).value ==
           "1 year 2 mons 3 days 04:05:06");
    assert(callFn(eval, "date_trunc",
                  {F("milliseconds"), interval}).value ==
           "1 year 2 mons 3 days 04:05:06.789000");
    assert(callFn(eval, "date_trunc",
                  {F("microseconds"), interval}).value ==
           "1 year 2 mons 3 days 04:05:06.789123");
    assert(callFn(eval, "date_trunc",
                  {F("milliseconds"),
                   IV("-3 days -04:05:06.789123")}).value ==
           "-3 days -04:05:06.789000");
    auto longInterval =
        IV("1234 years 8 mons 3 days 04:05:06.789123");
    assert(callFn(eval, "date_trunc",
                  {F("millennium"), longInterval}).value == "1000 years");
    assert(callFn(eval, "date_trunc",
                  {F("century"), longInterval}).value == "1200 years");
    assert(callFn(eval, "date_trunc",
                  {F("decade"), longInterval}).value == "1230 years");
    assert(callFn(eval, "date_trunc",
                  {F("quarter"), IV("-14 mons")}).value == "-1 years");
    expectDateTruncError("week", interval, "0A000");
    expectDateTruncError("epoch", interval, "22023");
    assert(callFn(eval, "date_trunc", {F("quarter"), ts}).value == "2026-04-01 00:00:00");
    assert(callFn(eval, "date_trunc",
                  {F("day"), TS("infinity")}).value == "infinity");
    assert(callFn(eval, "date_trunc",
                  {F("day"), TS("-infinity")}).value == "-infinity");
    assert(callFn(eval, "date_trunc",
                  {F("day"), TS("2026-02-30 14:35:09")}).isNull);
    assert(callFn(eval, "date_trunc",
                  {F("day"), TS("2026-13-01 14:35:09")}).isNull);
    assert(callFn(eval, "date_trunc",
                  {F("day"), TS("2026x06x26 14:35:09")}).isNull);
    assert(callFn(eval, "date_trunc",
                  {F("day"), TS("not-a-timestamp")}).isNull);
    expectDateTruncError("not_a_unit", ts, "22023");
    std::cout << "[DATEFN] date_trunc OK" << std::endl;
}

static void test_template_parsing() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "to_date", {F("15.08.2026"), F("DD.MM.YYYY")}).value ==
           "2026-08-15");
    assert(callFn(eval, "to_timestamp",
                  {F("2026-08-15 14:30:05"),
                   F("YYYY-MM-DD HH24:MI:SS")}).value ==
           "2026-08-15 14:30:05+00");

    // Malformed template input must fail as a value conversion; it must not
    // throw from stoi or manufacture an invalid civil date/time.
    assert(callFn(eval, "to_date",
                  {F("not-a-date"), F("YYYY-MM-DD")}).isNull);
    assert(callFn(eval, "to_date",
                  {F("2026-02-30"), F("YYYY-MM-DD")}).isNull);
    assert(callFn(eval, "to_timestamp",
                  {F("2026-08-15 25:30:05"),
                   F("YYYY-MM-DD HH24:MI:SS")}).isNull);
    assert(callFn(eval, "to_timestamp",
                  {F("2026-08-15 14:30"),
                   F("YYYY-MM-DD HH24:MI:SS")}).isNull);
    assert(callFn(eval, "to_date",
                  {F("2026-08-15"),
                   dbms::ExprValue("text", "", true)}).isNull);
    std::cout << "[DATEFN] template parsing OK" << std::endl;
}

static void test_current_family() {
    const std::string beforeDate = utcNow("%Y-%m-%d");
    const std::string beforeTimestamp = utcNow("%Y-%m-%d %H:%M:%S");
    const std::string beforeTime = utcNow("%H:%M:%S");
    dbms::ExprEvaluator eval;
    const auto currentDate = callFn(eval, "current_date", {});
    const auto currentTimestamp = callFn(eval, "current_timestamp", {});
    const auto localTimestamp = callFn(eval, "localtimestamp", {});
    const auto transactionTimestamp =
        callFn(eval, "transaction_timestamp", {});
    const auto statementTimestamp = callFn(eval, "statement_timestamp", {});
    const auto now = callFn(eval, "now", {});
    const auto clockTimestamp = callFn(eval, "clock_timestamp", {});
    const auto currentTime = callFn(eval, "current_time", {});
    const auto localTime = callFn(eval, "localtime", {});
    const std::string afterDate = utcNow("%Y-%m-%d");
    const std::string afterTimestamp = utcNow("%Y-%m-%d %H:%M:%S");
    const std::string afterTime = utcNow("%H:%M:%S");

    const auto currentValue = [](const std::string& value,
                                 const std::string& before,
                                 const std::string& after) {
        return value == before || value == after;
    };
    assert(currentValue(currentDate.value, beforeDate, afterDate));
    assert(currentValue(localTimestamp.value,
                        beforeTimestamp, afterTimestamp));
    assert(currentTimestamp.value == localTimestamp.value + "+00");
    assert(transactionTimestamp.value == currentTimestamp.value);
    assert(statementTimestamp.value == currentTimestamp.value);
    assert(now.value == currentTimestamp.value);
    assert(currentValue(clockTimestamp.value,
                        beforeTimestamp + "+00", afterTimestamp + "+00"));
    assert(currentValue(localTime.value, beforeTime, afterTime));
    assert(currentTime.value == localTime.value + "+00");

    const auto bareDate = dbms::ExprHelper::evalString("current_date", {});
    assert(bareDate.ok && !bareDate.isNull &&
           currentValue(bareDate.value, beforeDate, afterDate));
    std::cout << "[DATEFN] current_* family OK" << std::endl;
}

static void test_overlaps_point_boundaries() {
    dbms::ExprEvaluator eval;

    // OVERLAPS treats a non-empty period as [start, end).  A zero-length
    // period at the left edge overlaps it, while one at the right edge does
    // not.  Keep the result symmetric when the point is the second period.
    assert(callFn(eval, "overlaps",
                  {D("2026-01-02"), D("2026-01-02"),
                   D("2026-01-02"), D("2026-01-03")}).value == "t");
    assert(callFn(eval, "overlaps",
                  {D("2026-01-03"), D("2026-01-03"),
                   D("2026-01-02"), D("2026-01-03")}).value == "f");
    assert(callFn(eval, "overlaps",
                  {D("2026-01-02"), D("2026-01-03"),
                   D("2026-01-03"), D("2026-01-03")}).value == "f");
    assert(callFn(eval, "overlaps",
                  {D("2026-01-02"), D("2026-01-02"),
                   D("2026-01-02"), D("2026-01-02")}).value == "t");

    std::cout << "[DATEFN] overlaps point boundaries OK" << std::endl;
}

static void test_overlaps_null_semantics() {
    dbms::ExprEvaluator eval;

    // A period with one unknown endpoint can still be known to overlap when
    // its known endpoint lies strictly inside the other period.  Boundary and
    // outside cases remain unknown because the missing endpoint may extend in
    // either direction.
    const auto knownTrue = callFn(
        eval, "overlaps",
        {D("2026-01-02"), NullDate(),
         D("2026-01-01"), D("2026-01-03")});
    assert(!knownTrue.isNull && knownTrue.value == "t");
    const auto swappedNull = callFn(
        eval, "overlaps",
        {NullDate(), D("2026-01-02"),
         D("2026-01-01"), D("2026-01-03")});
    assert(!swappedNull.isNull && swappedNull.value == "t");
    const auto secondPeriod = callFn(
        eval, "overlaps",
        {D("2026-01-01"), D("2026-01-03"),
         D("2026-01-02"), NullDate()});
    assert(!secondPeriod.isNull && secondPeriod.value == "t");

    assert(callFn(eval, "overlaps",
                  {D("2026-01-01"), NullDate(),
                   D("2026-01-01"), D("2026-01-03")}).isNull);
    assert(callFn(eval, "overlaps",
                  {D("2026-01-04"), NullDate(),
                   D("2026-01-01"), D("2026-01-03")}).isNull);
    assert(callFn(eval, "overlaps",
                  {NullDate(), NullDate(),
                   D("2026-01-01"), D("2026-01-03")}).isNull);

    std::cout << "[DATEFN] overlaps NULL semantics OK" << std::endl;
}

static void test_overlaps_interval_endpoints() {
    dbms::ExprEvaluator eval;

    assert(callFn(eval, "overlaps",
                  {TS("2026-01-01 00:00:00"), IV("2 days"),
                   TS("2026-01-02 00:00:00"),
                   TS("2026-01-04 00:00:00")}).value == "t");
    assert(callFn(eval, "overlaps",
                  {TS("2026-01-02 00:00:00"),
                   TS("2026-01-04 00:00:00"),
                   TS("2026-01-01 00:00:00"), IV("2 days")}).value == "t");
    assert(callFn(eval, "overlaps",
                  {TS("2026-01-03 00:00:00"), IV("-2 days"),
                   TS("2026-01-02 00:00:00"),
                   TS("2026-01-04 00:00:00")}).value == "t");
    assert(callFn(eval, "overlaps",
                  {TS("2026-01-01 00:00:00"), IV("1 day"),
                   TS("2026-01-02 00:00:00"),
                   TS("2026-01-04 00:00:00")}).value == "f");
    assert(callFn(eval, "overlaps",
                  {TS("2026-01-04 00:00:00"), IV("0 days"),
                   TS("2026-01-02 00:00:00"),
                   TS("2026-01-04 00:00:00")}).value == "f");

    std::cout << "[DATEFN] overlaps interval endpoints OK" << std::endl;
}

static void test_age_fractional_seconds() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "age",
                  {TS("2024-01-01 00:00:00.5"),
                   TS("2024-01-01 00:00:00")}).value ==
           "00:00:00.500000");
    assert(callFn(eval, "age",
                  {TS("2024-01-01 00:00:00.000005"),
                   TS("2024-01-01 00:00:00")}).value ==
           "00:00:00.000005");
    assert(callFn(eval, "age",
                  {TS("2024-01-01 00:00:01.25"),
                   TS("2024-01-01 00:00:00.75")}).value ==
           "00:00:00.500000");

    std::cout << "[DATEFN] age fractional seconds OK" << std::endl;
}

static void test_age_single_argument() {
    dbms::ExprEvaluator eval;
    const auto today = callFn(eval, "current_date", {});
    assert(!today.isNull);

    const auto oneArgument =
        callFn(eval, "age", {TS("2000-01-15 12:34:56.5")});
    const auto explicitMidnight = callFn(
        eval, "age",
        {TS(today.value + " 00:00:00"),
         TS("2000-01-15 12:34:56.5")});
    assert(!oneArgument.isNull);
    assert(oneArgument.value == explicitMidnight.value);

    const auto laterToday = callFn(
        eval, "age", {TS(today.value + " 12:34:56.5")});
    assert(laterToday.value == "-12:34:56.500000");

    const auto nullArgument = callFn(
        eval, "age", {dbms::ExprValue("timestamp", "", true)});
    assert(nullArgument.isNull);

    std::cout << "[DATEFN] age(timestamp) OK" << std::endl;
}

int main() {
    test_extract_date_part();
    test_dow_doy_century();
    test_epoch();
    test_make();
    test_date_trunc();
    test_template_parsing();
    test_current_family();
    test_overlaps_point_boundaries();
    test_overlaps_null_semantics();
    test_overlaps_interval_endpoints();
    test_age_fractional_seconds();
    test_age_single_argument();
    std::cout << "[DATEFN] all passed" << std::endl;
    return 0;
}
