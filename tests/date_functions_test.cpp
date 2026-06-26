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
#include <memory>
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
static dbms::ExprValue I(int64_t v) { return dbms::ExprValue("integer", std::to_string(v), false); }

static void test_extract_date_part() {
    dbms::ExprEvaluator eval;
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
    // Unix epoch is exactly 0 seconds since 1970-01-01 00:00:00.
    assert(callFn(eval, "extract", {F("epoch"), TS("1970-01-01 00:00:00")}).value == "0");
    // One day later -> 86400 seconds.
    assert(callFn(eval, "extract", {F("epoch"), TS("1970-01-02 00:00:00")}).value == "86400");
    // A specific later instant.
    assert(callFn(eval, "extract", {F("epoch"), TS("1970-01-01 01:00:00")}).value == "3600");
    std::cout << "[DATEFN] epoch OK" << std::endl;
}

static void test_make() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "make_date", {I(2026), I(6), I(26)}).value == "2026-06-26");
    assert(callFn(eval, "make_time", {I(14), I(5), I(9)}).value == "14:05:09");
    assert(callFn(eval, "make_timestamp", {I(2026), I(6), I(26), I(14), I(5), I(9)}).value
           == "2026-06-26 14:05:09");
    // Invalid month -> NULL.
    assert(callFn(eval, "make_date", {I(2026), I(13), I(1)}).isNull);
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
    std::cout << "[DATEFN] date_trunc OK" << std::endl;
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
    test_current_family();
    std::cout << "[DATEFN] all passed" << std::endl;
    return 0;
}
