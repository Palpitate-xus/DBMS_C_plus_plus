// ============================================================================
// to_char formatting test — Phase 4 Wave 4.19k
// Exercises to_char(date/timestamp/time, fmt) template formatting and the
// minimal numeric to_char path. Args are bound as column refs so the timestamp
// text and the format template pass through the evaluator verbatim (the literal
// layer would otherwise reinterpret scalar values).
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
    dbms::RowContext ctx;
    for (size_t i = 0; i < args.size(); ++i) {
        std::string col = "a" + std::to_string(i);
        ctx.set(col, args[i]);
        auto ref = std::make_unique<dbms::ColumnRefExpr>();
        ref->column = col;
        call.args.push_back(std::move(ref));
    }
    return eval.eval(&call, ctx);
}

static dbms::ExprValue TS(const std::string& v) { return dbms::ExprValue("timestamp", v, false); }
static dbms::ExprValue DT(const std::string& v) { return dbms::ExprValue("date", v, false); }
static dbms::ExprValue TM(const std::string& v) { return dbms::ExprValue("time", v, false); }
static dbms::ExprValue F(const std::string& v) { return dbms::ExprValue("text", v, false); }
static dbms::ExprValue N(const std::string& v) { return dbms::ExprValue("numeric", v, false); }

static void test_basic_datetime() {
    dbms::ExprEvaluator eval;
    // 2026-06-20 is a Saturday (dow 6, D=7); day-of-year 171; quarter 2.
    auto ts = TS("2026-06-20 14:05:09");
    assert(callFn(eval, "to_char", {ts, F("YYYY-MM-DD HH24:MI:SS")}).value == "2026-06-20 14:05:09");
    assert(callFn(eval, "to_char", {ts, F("YYYY/MM/DD")}).value == "2026/06/20");
    assert(callFn(eval, "to_char", {ts, F("HH12:MI AM")}).value == "02:05 PM");
    assert(callFn(eval, "to_char", {ts, F("HH:MI am")}).value == "02:05 pm");
    std::cout << "[TOCHAR] basic datetime OK" << std::endl;
}

static void test_names_and_fields() {
    dbms::ExprEvaluator eval;
    auto ts = TS("2026-06-20 14:05:09");
    assert(callFn(eval, "to_char", {ts, F("Mon DD, YYYY")}).value == "Jun 20, 2026");
    assert(callFn(eval, "to_char", {ts, F("Month")}).value == "June");
    assert(callFn(eval, "to_char", {ts, F("MONTH")}).value == "JUNE");
    assert(callFn(eval, "to_char", {ts, F("month")}).value == "june");
    assert(callFn(eval, "to_char", {ts, F("Day")}).value == "Saturday");
    assert(callFn(eval, "to_char", {ts, F("DY")}).value == "SAT");
    assert(callFn(eval, "to_char", {ts, F("D")}).value == "7");       // Saturday, Sunday=1
    assert(callFn(eval, "to_char", {ts, F("DDD")}).value == "171");   // day-of-year
    assert(callFn(eval, "to_char", {ts, F("Q")}).value == "2");       // quarter
    assert(callFn(eval, "to_char", {ts, F("YY YYY Y")}).value == "26 026 6");
    std::cout << "[TOCHAR] names/fields OK" << std::endl;
}

static void test_quoting_and_date_time() {
    dbms::ExprEvaluator eval;
    // Double-quoted runs emit verbatim; plain separators pass through.
    assert(callFn(eval, "to_char", {TS("2026-06-20 14:05:09"), F("YYYY\"y\"MM\"m\"DD\"d\"")}).value == "2026y06m20d");
    // date-only value.
    assert(callFn(eval, "to_char", {DT("2026-01-02"), F("YYYY-MM-DD")}).value == "2026-01-02");
    // time-only value.
    assert(callFn(eval, "to_char", {TM("09:08:07"), F("HH24:MI:SS")}).value == "09:08:07");
    std::cout << "[TOCHAR] quoting/date/time OK" << std::endl;
}

static void test_numeric() {
    dbms::ExprEvaluator eval;
    // FM suppresses the leading sign blank; rounds to the fraction width.
    assert(callFn(eval, "to_char", {N("1234.5"), F("FM9999.99")}).value == "1234.50");
    // '0' zero-pads the integer part; non-FM keeps the leading sign blank.
    assert(callFn(eval, "to_char", {N("7"), F("000")}).value == " 007");
    // Non-FM keeps a leading blank in the sign position.
    assert(callFn(eval, "to_char", {N("42"), F("9999")}).value == " 42");
    // Negative.
    assert(callFn(eval, "to_char", {N("-3.14"), F("FM99.99")}).value == "-3.14");
    std::cout << "[TOCHAR] numeric OK" << std::endl;
}

static void test_null() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "to_char", {dbms::ExprValue("timestamp", "", true), F("YYYY")}).isNull);
    assert(callFn(eval, "to_char", {TS("2026-06-20 14:05:09"), dbms::ExprValue("text", "", true)}).isNull);
    std::cout << "[TOCHAR] null OK" << std::endl;
}

int main() {
    test_basic_datetime();
    test_names_and_fields();
    test_quoting_and_date_time();
    test_numeric();
    test_null();
    std::cout << "[TOCHAR] all passed" << std::endl;
    return 0;
}
