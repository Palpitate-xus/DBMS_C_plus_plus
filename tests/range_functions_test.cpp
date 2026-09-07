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

static dbms::ExprValue callBinary(dbms::ExprEvaluator& eval,
                                  const std::string& op,
                                  const dbms::ExprValue& left,
                                  const dbms::ExprValue& right) {
    dbms::RowContext ctx;
    ctx.set("lhs", left);
    ctx.set("rhs", right);
    auto lhs = std::make_unique<dbms::ColumnRefExpr>();
    lhs->column = "lhs";
    auto rhs = std::make_unique<dbms::ColumnRefExpr>();
    rhs->column = "rhs";
    dbms::BinaryOpExpr expression;
    expression.op = op;
    expression.left = std::move(lhs);
    expression.right = std::move(rhs);
    return eval.eval(&expression, ctx);
}

static dbms::ExprValue R(const std::string& v) { return dbms::ExprValue("int4range", v, false); }
static dbms::ExprValue RT(const std::string& type, const std::string& v) {
    return dbms::ExprValue(type, v, false);
}
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

static void test_bound_result_types() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "lower", {RT("int4range", "[1,10)")}).typeName ==
           "integer");
    assert(callFn(eval, "lower", {RT("int8range", "[1,10)")}).typeName ==
           "bigint");
    assert(callFn(eval, "lower", {RT("numrange", "[1.5,10.5)")}).typeName ==
           "numeric");
    assert(callFn(eval, "lower",
                  {RT("daterange", "[2020-01-01,2020-01-02)")}).typeName ==
           "date");
    assert(callFn(eval, "lower",
                  {RT("tsrange", "[2020-01-01,2020-01-02)")}).typeName ==
           "timestamp");
    assert(callFn(eval, "lower",
                  {RT("tstzrange", "[2020-01-01+00,2020-01-02+00)")}).typeName ==
           "timestamptz");

    const auto unbounded = callFn(eval, "lower", {R("(,10)")});
    assert(unbounded.isNull && unbounded.typeName == "integer");
    const auto empty = callFn(eval, "upper", {RT("daterange", "empty")});
    assert(empty.isNull && empty.typeName == "date");
    const auto typedNull = callFn(
        eval, "lower", {dbms::ExprValue("int8range", "", true)});
    assert(typedNull.isNull && typedNull.typeName == "bigint");

    std::cout << "[RANGEFN] bound result types OK" << std::endl;
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

static void test_overlap_operator() {
    dbms::ExprEvaluator eval;
    assert(callBinary(eval, "&&", R("[1,5)"), R("[4,8)")).value == "t");
    assert(callBinary(eval, "&&", R("[1,5)"), R("[5,8)")).value == "f");
    assert(callBinary(eval, "&&", R("empty"), R("[1,5)")).value == "f");
    assert(callBinary(eval, "&&",
                      RT("numrange", "[2,11)"),
                      RT("numrange", "[10,20)")).value == "t");
    assert(callBinary(eval, "&&",
                      RT("daterange", "[2024-01-01,2024-02-01)"),
                      RT("daterange", "[2024-01-15,2024-03-01)")).value == "t");
    assert(callBinary(eval, "&&",
                      RT("numrange", "[1,5]"),
                      RT("numrange", "[5,8)")).value == "t");
    assert(callBinary(eval, "&&",
                      RT("numrange", "(,0)"),
                      RT("numrange", "[0,)")).value == "f");

    const auto nullRange = dbms::ExprValue("int4range", "", true);
    assert(callBinary(eval, "&&", nullRange, R("[1,5)")).isNull);

    const auto arrayOverlap = callBinary(
        eval, "&&", RT("integer[]", "{1,2}"), RT("integer[]", "{2,3}"));
    assert(!arrayOverlap.isNull && arrayOverlap.value == "t");

    std::cout << "[RANGEFN] overlap operator OK" << std::endl;
}

static void test_containment_operators() {
    dbms::ExprEvaluator eval;
    assert(callBinary(eval, "@>", R("[1,10)"), R("[3,5)")).value == "t");
    assert(callBinary(eval, "<@", R("[3,5)"), R("[1,10)")).value == "t");
    assert(callBinary(eval, "@>", R("[1,10)"),
                      RT("integer", "3")).value == "t");
    assert(callBinary(eval, "@>", R("[1,10)"),
                      RT("integer", "10")).value == "f");
    assert(callBinary(eval, "<@", RT("integer", "3"),
                      R("[1,10)")).value == "t");

    assert(callBinary(eval, "@>", R("[1,10)"), R("empty")).value == "t");
    assert(callBinary(eval, "@>", R("empty"), R("empty")).value == "t");
    assert(callBinary(eval, "@>", R("empty"),
                      RT("integer", "1")).value == "f");
    assert(callBinary(eval, "@>", RT("numrange", "(1,5]"),
                      RT("numeric", "1")).value == "f");
    assert(callBinary(eval, "@>", RT("numrange", "(1,5]"),
                      RT("numeric", "5")).value == "t");

    const auto nullRange = dbms::ExprValue("int4range", "", true);
    assert(callBinary(eval, "@>", nullRange, RT("integer", "1")).isNull);

    const auto arrayContains = callBinary(
        eval, "@>", RT("integer[]", "{1,2}"), RT("integer[]", "{2}"));
    assert(!arrayContains.isNull && arrayContains.value == "t");

    std::cout << "[RANGEFN] containment operators OK" << std::endl;
}

int main() {
    test_bounds();
    test_bound_result_types();
    test_lower_upper_string_still_works();
    test_predicates();
    test_overlap_operator();
    test_containment_operators();
    std::cout << "[RANGEFN] all passed" << std::endl;
    return 0;
}
