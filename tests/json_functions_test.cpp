// ============================================================================
// JSON function library test — Phase 4 Wave 4.19f
// Exercises JSON functions in ExprEvaluator: json_typeof / jsonb_typeof,
// json_array_length, json_build_array, json_build_object, to_json. Output
// JSON is the compact (no-space) canonical form this engine produces.
// ============================================================================

#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// Evaluate name(args...) by binding each argument as a column reference into a
// RowContext. This mirrors the production path (functions over column values),
// where values flow through verbatim rather than being re-parsed by the literal
// layer (which would unquote/reinterpret scalar JSON text like "hi"/true/null).
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

static dbms::ExprValue J(const std::string& v) { return dbms::ExprValue("json", v, false); }
static dbms::ExprValue S(const std::string& v) { return dbms::ExprValue("text", v, false); }
static dbms::ExprValue I(int64_t v) { return dbms::ExprValue("integer", std::to_string(v), false); }
static dbms::ExprValue B(bool v) { return dbms::ExprValue("boolean", v ? "t" : "f", false); }

static void test_typeof() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "json_typeof", {J("{\"a\":1}")}).value == "object");
    assert(callFn(eval, "json_typeof", {J("[1,2,3]")}).value == "array");
    assert(callFn(eval, "json_typeof", {J("\"hi\"")}).value == "string");
    assert(callFn(eval, "json_typeof", {J("42")}).value == "number");
    assert(callFn(eval, "json_typeof", {J("true")}).value == "boolean");
    assert(callFn(eval, "json_typeof", {J("null")}).value == "null");
    assert(callFn(eval, "jsonb_typeof", {J("  [9]  ")}).value == "array");  // whitespace tolerant
    std::cout << "[JSONFN] typeof OK" << std::endl;
}

static void test_array_length() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "json_array_length", {J("[1,2,3]")}).value == "3");
    assert(callFn(eval, "json_array_length", {J("[]")}).value == "0");
    // Nested elements counted at top level only.
    assert(callFn(eval, "json_array_length", {J("[[1,2],[3,4],5]")}).value == "3");
    assert(callFn(eval, "json_array_length", {J("[\"a,b\",\"c\"]")}).value == "2");  // quoted comma
    // Not an array -> NULL.
    assert(callFn(eval, "json_array_length", {J("{\"a\":1}")}).isNull);
    std::cout << "[JSONFN] array_length OK" << std::endl;
}

static void test_build() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "json_build_array", {I(1), S("a"), B(true)}).value == "[1,\"a\",true]");
    assert(callFn(eval, "json_build_array", {}).value == "[]");
    assert(callFn(eval, "json_build_object", {S("k"), I(5)}).value == "{\"k\":5}");
    assert(callFn(eval, "json_build_object", {S("a"), S("x"), S("b"), I(2)}).value
           == "{\"a\":\"x\",\"b\":2}");
    // Nested: a pre-built JSON value is embedded as-is, not re-quoted.
    auto inner = callFn(eval, "json_build_array", {I(1), I(2)});
    assert(callFn(eval, "json_build_object", {S("arr"), inner}).value == "{\"arr\":[1,2]}");
    std::cout << "[JSONFN] build OK" << std::endl;
}

static void test_to_json() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "to_json", {S("hi")}).value == "\"hi\"");
    assert(callFn(eval, "to_json", {I(7)}).value == "7");
    assert(callFn(eval, "to_json", {B(false)}).value == "false");
    assert(callFn(eval, "to_json", {dbms::ExprValue("text", "", true)}).value == "null");
    // Embedded quotes are escaped.
    assert(callFn(eval, "to_json", {S("a\"b")}).value == "\"a\\\"b\"");
    std::cout << "[JSONFN] to_json OK" << std::endl;
}

int main() {
    test_typeof();
    test_array_length();
    test_build();
    test_to_json();
    std::cout << "[JSONFN] all passed" << std::endl;
    return 0;
}
