// ============================================================================
// Range function library test — Phase 4 Wave 4.19h
// Exercises range functions in ExprEvaluator operating on range literal text:
// lower/upper (overloaded on range-typed args), isempty, lower_inc, upper_inc,
// lower_inf, upper_inf. Also confirms lower/upper still behave as the string
// case-folding functions for non-range (text) arguments.
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

static dbms::ExprValue R(const std::string& v) { return dbms::ExprValue("int4range", v, false); }
static dbms::ExprValue T(const std::string& v) { return dbms::ExprValue("text", v, false); }

static void test_bounds() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "lower", {R("[1,10)")}).value == "1");
    assert(callFn(eval, "upper", {R("[1,10)")}).value == "10");
    // Unbounded -> NULL.
    assert(callFn(eval, "lower", {R("(,10)")}).isNull);
    assert(callFn(eval, "upper", {R("[1,)")}).isNull);
    // empty -> NULL bounds.
    assert(callFn(eval, "lower", {R("empty")}).isNull);
    std::cout << "[RANGEFN] bounds OK" << std::endl;
}

static void test_lower_upper_string_still_works() {
    dbms::ExprEvaluator eval;
    // Non-range (text) args keep the string case-folding behavior.
    assert(callFn(eval, "lower", {T("HeLLo")}).value == "hello");
    assert(callFn(eval, "upper", {T("HeLLo")}).value == "HELLO");
    std::cout << "[RANGEFN] string lower/upper preserved OK" << std::endl;
}

static void test_predicates() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "isempty", {R("empty")}).value == "t");
    assert(callFn(eval, "isempty", {R("[1,10)")}).value == "f");

    assert(callFn(eval, "lower_inc", {R("[1,10)")}).value == "t");
    assert(callFn(eval, "lower_inc", {R("(1,10)")}).value == "f");
    assert(callFn(eval, "upper_inc", {R("[1,10]")}).value == "t");
    assert(callFn(eval, "upper_inc", {R("[1,10)")}).value == "f");

    assert(callFn(eval, "lower_inf", {R("(,10)")}).value == "t");
    assert(callFn(eval, "lower_inf", {R("[1,10)")}).value == "f");
    assert(callFn(eval, "upper_inf", {R("[1,)")}).value == "t");
    assert(callFn(eval, "upper_inf", {R("[1,10)")}).value == "f");

    // Predicates on empty range are all false.
    assert(callFn(eval, "lower_inc", {R("empty")}).value == "f");
    assert(callFn(eval, "lower_inf", {R("empty")}).value == "f");
    std::cout << "[RANGEFN] predicates OK" << std::endl;
}

int main() {
    test_bounds();
    test_lower_upper_string_still_works();
    test_predicates();
    std::cout << "[RANGEFN] all passed" << std::endl;
    return 0;
}
