// ============================================================================
// Math function library test — Phase 4 Wave 4.19b
// Exercises the expanded set of PostgreSQL math functions in ExprEvaluator:
// pow/ceiling, log (1- and 2-arg), log10, trunc(x,n), degrees/radians, cot,
// hyperbolic (sinh/cosh/tanh/asinh/acosh/atanh), gcd/lcm, div, factorial,
// width_bucket.
// ============================================================================

#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include <cassert>
#include <cmath>
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

static dbms::ExprValue D(double v) { return dbms::ExprValue("double precision", std::to_string(v), false); }
static dbms::ExprValue I(int64_t v) { return dbms::ExprValue("integer", std::to_string(v), false); }

static bool approx(const dbms::ExprValue& r, double want) {
    return !r.isNull && std::fabs(std::stod(r.value) - want) < 1e-6;
}

// Looser tolerance for cases where the literal layer rounds the input (e.g. pi
// passed through std::to_string keeps only 6 decimals, which scales up under
// degrees()/cot()).
static bool approxT(const dbms::ExprValue& r, double want, double tol) {
    return !r.isNull && std::fabs(std::stod(r.value) - want) < tol;
}

__attribute__((noinline)) static void overwriteConstructorStack() {
    volatile unsigned char bytes[32768];
    for (size_t i = 0; i < sizeof(bytes); ++i)
        bytes[i] = static_cast<unsigned char>(i);
}

static void test_callback_lifetimes() {
    dbms::ExprEvaluator eval;
    overwriteConstructorStack();

    assert(approx(callFn(eval, "sin", {D(0.5)}), std::sin(0.5)));
    auto justified = callFn(
        eval, "justify_hours",
        {dbms::ExprValue("interval", "25:00:00", false)});
    assert(!justified.isNull && justified.value == "1 day 01:00:00");
    std::cout << "[MATHFN] callback lifetimes OK" << std::endl;
}

static void test_pow_log() {
    dbms::ExprEvaluator eval;
    assert(approx(callFn(eval, "pow", {D(2), D(10)}), 1024.0));
    const auto largeExponential = callFn(eval, "exp", {D(200)});
    assert(!largeExponential.isNull &&
           std::fabs(std::log(std::stold(largeExponential.value)) - 200.0L) <
               1e-12L);
    bool exponentialOverflowRejected = false;
    try {
        (void)callFn(eval, "exp", {D(12000)});
    } catch (const std::runtime_error& error) {
        exponentialOverflowRejected =
            std::string(error.what()).find("SQLSTATE 22003") !=
            std::string::npos;
    }
    assert(exponentialOverflowRejected);
    const auto largePower = callFn(eval, "power", {D(10), D(100.5)});
    assert(!largePower.isNull &&
           std::fabs(std::log10(std::stold(largePower.value)) - 100.5L) <
               1e-12L);
    assert(callFn(eval, "power",
                  {I(2), I(std::numeric_limits<int64_t>::min())})
               .value == "0");
    const dbms::ExprValue largePowerBase(
        "numeric", "12345678901234567890", false);
    assert(callFn(eval, "pow", {largePowerBase, I(2)}).value ==
           callFn(eval, "power", {largePowerBase, I(2)}).value);
    bool overflowRejected = false;
    try {
        (void)callFn(
            eval, "power",
            {I(2), I(std::numeric_limits<int64_t>::max())});
    } catch (const std::runtime_error& error) {
        overflowRejected =
            std::string(error.what()).find("SQLSTATE 22003") !=
            std::string::npos;
    }
    assert(overflowRejected);
    bool aliasOverflowRejected = false;
    try {
        (void)callFn(
            eval, "pow", {I(2), I(std::numeric_limits<int64_t>::max())});
    } catch (const std::runtime_error& error) {
        aliasOverflowRejected =
            std::string(error.what()).find("SQLSTATE 22003") !=
            std::string::npos;
    }
    assert(aliasOverflowRejected);
    assert(approx(callFn(eval, "log", {D(100)}), 2.0));            // base-10
    assert(approx(callFn(eval, "log", {D(2), D(8)}), 3.0));        // base-2 of 8
    assert(approx(callFn(eval, "log10", {D(1000)}), 3.0));
    assert(approx(callFn(eval, "ln", {D(1)}), 0.0));
    auto expectInvalidLog = [&](const std::string& function,
                                const std::vector<dbms::ExprValue>& args) {
        bool rejected = false;
        try {
            (void)callFn(eval, function, args);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 2201E") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectInvalidLog("ln", {D(0)});
    expectInvalidLog("log10", {D(-1)});
    expectInvalidLog("log", {D(1), D(10)});
    expectInvalidLog("log", {D(2), D(0)});
    std::cout << "[MATHFN] pow/log OK" << std::endl;
}

static void test_trunc_round() {
    dbms::ExprEvaluator eval;
    const dbms::ExprValue nullScale("integer", "", true);
    assert(callFn(eval, "round", {D(1.25), nullScale}).isNull);
    assert(callFn(eval, "trunc", {D(1.25), nullScale}).isNull);
    assert(approx(callFn(eval, "trunc", {D(42.789)}), 42.0));
    assert(approx(callFn(eval, "trunc", {D(2.71828), I(2)}), 2.71));
    const auto largeFloatTrunc = callFn(
        eval, "trunc",
        {dbms::ExprValue("double precision", "1e20", false)});
    assert(largeFloatTrunc.typeName == "double precision");
    assert(largeFloatTrunc.value == "1e+20");
    const auto infiniteFloatTrunc = callFn(
        eval, "trunc",
        {dbms::ExprValue("double precision", "Infinity", false)});
    assert(infiniteFloatTrunc.typeName == "double precision");
    assert(infiniteFloatTrunc.value == "Infinity");
    const dbms::ExprValue largeFractionalNumeric(
        "numeric", "123456789012345678901234567890.987", false);
    assert(callFn(eval, "trunc", {largeFractionalNumeric}).value ==
           "123456789012345678901234567890");
    assert(callFn(eval, "trunc", {largeFractionalNumeric, I(-2)}).value ==
           "123456789012345678901234567800");
    assert(callFn(eval, "trunc", {largeFractionalNumeric, I(-40)}).value ==
           "0");
    const dbms::ExprValue oversizedScale(
        "bigint", "2147483648", false);
    for (const std::string function : {"round", "trunc"}) {
        bool rejected = false;
        try {
            (void)callFn(eval, function, {D(1.25), oversizedScale});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22003") !=
                       std::string::npos;
        }
        assert(rejected);
        assert(approx(callFn(eval, function,
                             {D(1.25), I(std::numeric_limits<int>::max())}),
                      1.25));
        assert(callFn(eval, function,
                      {D(1.25), I(std::numeric_limits<int>::min())})
                   .value == "0");
    }
    assert(approx(callFn(eval, "ceiling", {D(4.2)}), 5.0));
    const dbms::ExprValue largeNumeric(
        "numeric", "123456789012345678901234567890", false);
    assert(callFn(eval, "round", {largeNumeric, I(-2)}).value ==
           "123456789012345678901234567900");
    assert(callFn(eval, "round",
                  {dbms::ExprValue("numeric", "149", false), I(-19)})
               .value == "0");
    bool negativeSquareRootRejected = false;
    try {
        (void)callFn(eval, "sqrt", {D(-1)});
    } catch (const std::runtime_error& error) {
        negativeSquareRootRejected =
            std::string(error.what()).find("SQLSTATE 2201F") !=
            std::string::npos;
    }
    assert(negativeSquareRootRejected);
    std::cout << "[MATHFN] trunc/ceiling OK" << std::endl;
}

static void test_angles() {
    dbms::ExprEvaluator eval;
    double pi = std::atan(1.0) * 4.0;
    assert(approxT(callFn(eval, "degrees", {D(pi)}), 180.0, 1e-3));
    assert(approxT(callFn(eval, "radians", {D(180)}), pi, 1e-3));
    assert(approxT(callFn(eval, "cot", {D(pi / 4.0)}), 1.0, 1e-3));
    const dbms::ExprValue infinity(
        "double precision", "Infinity", false);
    const dbms::ExprValue nan("double precision", "NaN", false);
    for (const std::string function : {"sin", "cos", "tan", "cot"}) {
        bool rejected = false;
        try {
            (void)callFn(eval, function, {infinity});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22003") !=
                       std::string::npos;
        }
        assert(rejected);
        assert(callFn(eval, function, {nan}).value == "NaN");
    }
    std::cout << "[MATHFN] degrees/radians/cot OK" << std::endl;
}

static void test_hyperbolic() {
    dbms::ExprEvaluator eval;
    assert(approx(callFn(eval, "sinh", {D(0)}), 0.0));
    assert(approx(callFn(eval, "cosh", {D(0)}), 1.0));
    assert(approx(callFn(eval, "tanh", {D(0)}), 0.0));
    assert(approx(callFn(eval, "asinh", {D(0)}), 0.0));
    assert(approx(callFn(eval, "acosh", {D(1)}), 0.0));
    assert(approx(callFn(eval, "atanh", {D(0)}), 0.0));
    auto expectOutOfRange = [&](const std::string& function, double value) {
        bool rejected = false;
        try {
            (void)callFn(eval, function, {D(value)});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22003") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectOutOfRange("asin", 2.0);
    expectOutOfRange("acos", -2.0);
    expectOutOfRange("acosh", 0.5);
    expectOutOfRange("atanh", 2.0);
    assert(callFn(eval, "atanh", {D(1)}).value == "Infinity");
    assert(callFn(eval, "atanh", {D(-1)}).value == "-Infinity");
    std::cout << "[MATHFN] hyperbolic OK" << std::endl;
}

static void test_int_math() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "gcd", {I(54), I(24)}).value == "6");
    assert(callFn(eval, "gcd", {I(-12), I(18)}).value == "6");
    assert(callFn(eval, "lcm", {I(4), I(6)}).value == "12");
    assert(callFn(eval, "lcm", {I(0), I(5)}).value == "0");
    assert(callFn(eval, "gcd",
                  {I(std::numeric_limits<int64_t>::min()), I(2)}).value ==
           "2");

    auto expectOutOfRange = [&](const std::string& function,
                                const std::vector<dbms::ExprValue>& args) {
        bool rejected = false;
        try {
            (void)callFn(eval, function, args);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22003") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    auto expectDivisionByZero = [&](const std::string& function,
                                    const std::vector<dbms::ExprValue>& args) {
        bool rejected = false;
        try {
            (void)callFn(eval, function, args);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22012") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectOutOfRange(
        "abs", {I(std::numeric_limits<int64_t>::min())});
    expectOutOfRange(
        "gcd", {I(std::numeric_limits<int64_t>::min()), I(0)});
    expectOutOfRange(
        "lcm", {I(std::numeric_limits<int64_t>::max()), I(2)});

    assert(callFn(eval, "div", {I(9), I(4)}).value == "2");
    assert(callFn(eval, "div",
                  {dbms::ExprValue(
                       "numeric", "100000000000000000000000", false),
                   I(3)}).value == "33333333333333333333333");
    assert(callFn(eval, "div",
                  {dbms::ExprValue("numeric", "-10.5", false),
                   I(3)}).value == "-3");
    assert(callFn(eval, "div",
                  {I(std::numeric_limits<int64_t>::min()), I(-1)}).value ==
           "9223372036854775808");
    expectDivisionByZero("div", {I(9), I(0)});
    expectDivisionByZero("mod", {I(9), I(0)});
    expectDivisionByZero(
        "mod", {dbms::ExprValue("numeric", "9.0", false),
                dbms::ExprValue("numeric", "0.0", false)});
    assert(callFn(
               eval, "mod",
               {dbms::ExprValue(
                    "numeric", "100000000000000000000001", false),
                I(3)})
               .value == "2");
    assert(callFn(
               eval, "mod",
               {dbms::ExprValue("numeric", "10.50", false),
                dbms::ExprValue("numeric", "3.0", false)})
               .value == "1.50");
    assert(callFn(eval, "mod",
                  {I(std::numeric_limits<int64_t>::min()), I(-1)})
               .value == "0");
    assert(callFn(eval, "factorial", {I(5)}).value == "120");
    assert(callFn(eval, "factorial", {I(0)}).value == "1");
    assert(callFn(eval, "factorial", {I(21)}).value ==
           "51090942171709440000");
    expectOutOfRange("factorial", {I(-1)});
    expectOutOfRange("factorial", {I(450)});
    std::cout << "[MATHFN] gcd/lcm/div/factorial OK" << std::endl;
}

static void test_width_bucket() {
    dbms::ExprEvaluator eval;
    auto expectInvalidArgument = [&](const std::vector<dbms::ExprValue>& args) {
        bool rejected = false;
        try {
            (void)callFn(eval, "width_bucket", args);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 2201G") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    // PG doc example: width_bucket(5.35, 0.024, 10.06, 5) = 3
    assert(callFn(eval, "width_bucket", {D(5.35), D(0.024), D(10.06), I(5)}).value == "3");
    assert(callFn(eval, "width_bucket", {D(-1), D(0), D(10), I(5)}).value == "0");     // below low
    assert(callFn(eval, "width_bucket", {D(20), D(0), D(10), I(5)}).value == "6");     // above high -> count+1
    assert(callFn(eval, "width_bucket", {D(0), D(0), D(10), I(5)}).value == "1");      // at low edge
    expectInvalidArgument({D(5), D(0), D(10), I(0)});
    expectInvalidArgument({D(5), D(1), D(1), I(4)});
    expectInvalidArgument({D(std::numeric_limits<double>::quiet_NaN()),
                           D(0), D(10), I(4)});
    expectInvalidArgument({D(5),
                           D(-std::numeric_limits<double>::infinity()),
                           D(std::numeric_limits<double>::infinity()), I(4)});
    assert(callFn(eval, "width_bucket",
                  {D(std::numeric_limits<double>::infinity()), D(0), D(10),
                   I(4)}).value == "5");
    bool overflowRejected = false;
    try {
        (void)callFn(
            eval, "width_bucket",
            {D(20), D(0), D(10),
             I(std::numeric_limits<int64_t>::max())});
    } catch (const std::runtime_error& error) {
        overflowRejected =
            std::string(error.what()).find("SQLSTATE 22003") !=
            std::string::npos;
    }
    assert(overflowRejected);
    std::cout << "[MATHFN] width_bucket OK" << std::endl;
}

int main() {
    test_callback_lifetimes();
    test_pow_log();
    test_trunc_round();
    test_angles();
    test_hyperbolic();
    test_int_math();
    test_width_bucket();
    std::cout << "[MATHFN] all passed" << std::endl;
    return 0;
}
