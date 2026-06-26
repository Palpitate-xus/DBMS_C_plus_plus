// ============================================================================
// Array function library test — Phase 4 Wave 4.19e
// Exercises array functions in ExprEvaluator that operate on the '{...}'
// array-literal text: array_length/upper/lower/ndims, cardinality,
// array_append/prepend/cat, array_position, array_to_string, string_to_array.
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

static dbms::ExprValue A(const std::string& v) { return dbms::ExprValue("integer[]", v, false); }
static dbms::ExprValue S(const std::string& v) { return dbms::ExprValue("text", v, false); }
static dbms::ExprValue I(int64_t v) { return dbms::ExprValue("integer", std::to_string(v), false); }

static void test_dims() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "array_length", {A("{1,2,3}"), I(1)}).value == "3");
    assert(callFn(eval, "array_length", {A("{}"), I(1)}).isNull);
    assert(callFn(eval, "array_length", {A("{{1,2},{3,4},{5,6}}"), I(1)}).value == "3");
    assert(callFn(eval, "array_length", {A("{{1,2},{3,4},{5,6}}"), I(2)}).value == "2");
    assert(callFn(eval, "cardinality", {A("{{1,2},{3,4},{5,6}}")}).value == "6");
    assert(callFn(eval, "cardinality", {A("{1,2,3}")}).value == "3");
    assert(callFn(eval, "array_ndims", {A("{1,2,3}")}).value == "1");
    assert(callFn(eval, "array_ndims", {A("{{1,2},{3,4}}")}).value == "2");
    assert(callFn(eval, "array_lower", {A("{7,8,9}"), I(1)}).value == "1");
    assert(callFn(eval, "array_upper", {A("{7,8,9}"), I(1)}).value == "3");
    std::cout << "[ARRFN] dims OK" << std::endl;
}

static void test_mutate() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "array_append", {A("{1,2}"), I(3)}).value == "{1,2,3}");
    assert(callFn(eval, "array_prepend", {I(0), A("{1,2}")}).value == "{0,1,2}");
    assert(callFn(eval, "array_cat", {A("{1,2}"), A("{3,4}")}).value == "{1,2,3,4}");
    assert(callFn(eval, "array_append", {A("{}"), I(9)}).value == "{9}");
    std::cout << "[ARRFN] mutate OK" << std::endl;
}

static void test_position() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "array_position", {A("{10,20,30}"), I(20)}).value == "2");
    assert(callFn(eval, "array_position", {A("{10,20,30}"), I(99)}).isNull);
    assert(callFn(eval, "array_position", {S("{a,b,c}"), S("c")}).value == "3");
    std::cout << "[ARRFN] position OK" << std::endl;
}

static void test_join_split() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "array_to_string", {A("{1,2,3}"), S(",")}).value == "1,2,3");
    assert(callFn(eval, "array_to_string", {S("{a,b,c}"), S("-")}).value == "a-b-c");
    // NULL element omitted by default, included with null_string.
    assert(callFn(eval, "array_to_string", {A("{1,NULL,3}"), S(",")}).value == "1,3");
    assert(callFn(eval, "array_to_string", {A("{1,NULL,3}"), S(","), S("*")}).value == "1,*,3");

    assert(callFn(eval, "string_to_array", {S("a,b,c"), S(",")}).value == "{a,b,c}");
    assert(callFn(eval, "string_to_array", {S("x"), S(",")}).value == "{x}");
    // null_string maps a matching field to NULL.
    assert(callFn(eval, "string_to_array", {S("1,*,3"), S(","), S("*")}).value == "{1,NULL,3}");
    // Round-trip a value that needs quoting.
    auto sp = callFn(eval, "string_to_array", {S("a b,c"), S(",")});
    assert(sp.value == "{\"a b\",c}");
    assert(callFn(eval, "array_to_string", {S(sp.value), S("|")}).value == "a b|c");
    std::cout << "[ARRFN] join/split OK" << std::endl;
}

int main() {
    test_dims();
    test_mutate();
    test_position();
    test_join_split();
    std::cout << "[ARRFN] all passed" << std::endl;
    return 0;
}
