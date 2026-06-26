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

static void test_pow_log() {
    dbms::ExprEvaluator eval;
    assert(approx(callFn(eval, "pow", {D(2), D(10)}), 1024.0));
    assert(approx(callFn(eval, "log", {D(100)}), 2.0));            // base-10
    assert(approx(callFn(eval, "log", {D(2), D(8)}), 3.0));        // base-2 of 8
    assert(approx(callFn(eval, "log10", {D(1000)}), 3.0));
    assert(approx(callFn(eval, "ln", {D(1)}), 0.0));
    std::cout << "[MATHFN] pow/log OK" << std::endl;
}

static void test_trunc_round() {
    dbms::ExprEvaluator eval;
    assert(approx(callFn(eval, "trunc", {D(42.789)}), 42.0));
    assert(approx(callFn(eval, "trunc", {D(2.71828), I(2)}), 2.71));
    assert(approx(callFn(eval, "ceiling", {D(4.2)}), 5.0));
    std::cout << "[MATHFN] trunc/ceiling OK" << std::endl;
}

static void test_angles() {
    dbms::ExprEvaluator eval;
    double pi = std::atan(1.0) * 4.0;
    assert(approxT(callFn(eval, "degrees", {D(pi)}), 180.0, 1e-3));
    assert(approxT(callFn(eval, "radians", {D(180)}), pi, 1e-3));
    assert(approxT(callFn(eval, "cot", {D(pi / 4.0)}), 1.0, 1e-3));
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
    std::cout << "[MATHFN] hyperbolic OK" << std::endl;
}

static void test_int_math() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "gcd", {I(54), I(24)}).value == "6");
    assert(callFn(eval, "gcd", {I(-12), I(18)}).value == "6");
    assert(callFn(eval, "lcm", {I(4), I(6)}).value == "12");
    assert(callFn(eval, "lcm", {I(0), I(5)}).value == "0");
    assert(callFn(eval, "div", {I(9), I(4)}).value == "2");
    assert(callFn(eval, "div", {I(9), I(0)}).isNull);             // division by zero -> NULL
    assert(callFn(eval, "factorial", {I(5)}).value == "120");
    assert(callFn(eval, "factorial", {I(0)}).value == "1");
    std::cout << "[MATHFN] gcd/lcm/div/factorial OK" << std::endl;
}

static void test_width_bucket() {
    dbms::ExprEvaluator eval;
    // PG doc example: width_bucket(5.35, 0.024, 10.06, 5) = 3
    assert(callFn(eval, "width_bucket", {D(5.35), D(0.024), D(10.06), I(5)}).value == "3");
    assert(callFn(eval, "width_bucket", {D(-1), D(0), D(10), I(5)}).value == "0");     // below low
    assert(callFn(eval, "width_bucket", {D(20), D(0), D(10), I(5)}).value == "6");     // above high -> count+1
    assert(callFn(eval, "width_bucket", {D(0), D(0), D(10), I(5)}).value == "1");      // at low edge
    std::cout << "[MATHFN] width_bucket OK" << std::endl;
}

int main() {
    test_pow_log();
    test_trunc_round();
    test_angles();
    test_hyperbolic();
    test_int_math();
    test_width_bucket();
    std::cout << "[MATHFN] all passed" << std::endl;
    return 0;
}
