// ============================================================================
// Date/time function library test — Phase 4 Wave 4.19c
// Exercises the expanded date/time functions in ExprEvaluator: extract /
// date_part (year..second, quarter, dow/isodow, doy, epoch, century, ...),
// make_date / make_time / make_timestamp, date_trunc, and the reference
// "current" timestamp family.
// ============================================================================

#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include <cassert>
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
static dbms::ExprValue TS(const std::string& v) { return dbms::ExprValue("timestamp", v, false); }
static dbms::ExprValue IV(const std::string& v) { return dbms::ExprValue("interval", v, false); }
static dbms::ExprValue I(int64_t v) { return dbms::ExprValue("integer", std::to_string(v), false); }

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
                  {F("epoch"), TS("infinity")}).value == "Infinity");
    assert(callFn(eval, "extract",
                  {F("epoch"), TS("-infinity")}).value == "-Infinity");
    assert(callFn(eval, "extract", {F("epoch"), IV("3 days")}).value ==
           "259200.000000");
    assert(callFn(eval, "extract",
                  {F("epoch"),
                   IV("9223372036854775807 months")}).value ==
           "23906980319527578891744000.000000");
    assert(callFn(eval, "extract",
                  {F("epoch"),
                   IV(std::string(400, '9') + " months")}).isNull);
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
    auto ts = TS("2026-06-26 14:35:09");
    assert(callFn(eval, "date_trunc", {F("year"), ts}).value == "2026-01-01 00:00:00");
    assert(callFn(eval, "date_trunc", {F("month"), ts}).value == "2026-06-01 00:00:00");
    assert(callFn(eval, "date_trunc", {F("day"), ts}).value == "2026-06-26 00:00:00");
    assert(callFn(eval, "date_trunc", {F("hour"), ts}).value == "2026-06-26 14:00:00");
    assert(callFn(eval, "date_trunc", {F("minute"), ts}).value == "2026-06-26 14:35:00");
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
    dbms::ExprEvaluator eval;
    assert(!callFn(eval, "current_timestamp", {}).isNull);
    assert(!callFn(eval, "localtimestamp", {}).isNull);
    assert(!callFn(eval, "clock_timestamp", {}).isNull);
    assert(callFn(eval, "current_time", {}).value == "12:00:00");
    assert(callFn(eval, "localtime", {}).value == "12:00:00");
    std::cout << "[DATEFN] current_* family OK" << std::endl;
}

int main() {
    test_extract_date_part();
    test_dow_doy_century();
    test_epoch();
    test_make();
    test_date_trunc();
    test_template_parsing();
    test_current_family();
    std::cout << "[DATEFN] all passed" << std::endl;
    return 0;
}
