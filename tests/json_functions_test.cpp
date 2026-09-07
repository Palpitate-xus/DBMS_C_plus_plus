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
#include <stdexcept>
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
static dbms::ExprValue F(const std::string& v) { return dbms::ExprValue("double precision", v, false); }
static dbms::ExprValue IA(const std::string& v) { return dbms::ExprValue("integer[]", v, false); }
static dbms::ExprValue TA(const std::string& v) { return dbms::ExprValue("text[]", v, false); }

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
    // Objects and scalars are valid JSON, but not valid arguments for this
    // operation.
    for (const auto& value : {J("{\"a\":1}"), J("42")}) {
        bool rejected = false;
        try {
            (void)callFn(eval, "json_array_length", {value});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22023") !=
                       std::string::npos;
        }
        assert(rejected);
    }
    std::cout << "[JSONFN] array_length OK" << std::endl;
}

static void test_build() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "json_build_array", {I(1), S("a"), B(true)}).value == "[1,\"a\",true]");
    assert(callFn(eval, "json_build_array", {}).value == "[]");
    assert(callFn(eval, "jsonb_build_array", {I(1)}).typeName == "jsonb");
    assert(callFn(eval, "json_build_object", {S("k"), I(5)}).value == "{\"k\":5}");
    assert(callFn(eval, "jsonb_build_object",
                  {S("k"), I(5)}).typeName == "jsonb");
    assert(callFn(eval, "json_build_object", {S("a"), S("x"), S("b"), I(2)}).value
           == "{\"a\":\"x\",\"b\":2}");
    // Nested: a pre-built JSON value is embedded as-is, not re-quoted.
    auto inner = callFn(eval, "json_build_array", {I(1), I(2)});
    assert(callFn(eval, "json_build_object", {S("arr"), inner}).value == "{\"arr\":[1,2]}");

    const dbms::ExprValue nullKey("text", "", true);
    struct InvalidObjectCall {
        const char* function;
        std::vector<dbms::ExprValue> arguments;
        const char* sqlstate;
    };
    const std::vector<InvalidObjectCall> invalidCalls = {
        {"json_build_object", {S("a"), I(1), S("b")}, "22023"},
        {"jsonb_build_object", {S("a"), I(1), S("b")}, "22023"},
        {"json_build_object", {nullKey, I(1)}, "22004"},
        {"jsonb_build_object", {nullKey, I(1)}, "22023"},
    };
    for (const auto& invalid : invalidCalls) {
        bool rejected = false;
        try {
            (void)callFn(eval, invalid.function, invalid.arguments);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find(
                           std::string("SQLSTATE ") + invalid.sqlstate) !=
                       std::string::npos;
        }
        assert(rejected);
    }
    std::cout << "[JSONFN] build OK" << std::endl;
}

static void test_to_json() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "to_json", {S("hi")}).value == "\"hi\"");
    assert(callFn(eval, "to_json", {I(7)}).value == "7");
    assert(callFn(eval, "to_jsonb", {I(7)}).typeName == "jsonb");
    assert(callFn(eval, "to_json", {B(false)}).value == "false");
    assert(callFn(eval, "to_json", {dbms::ExprValue("text", "", true)}).value == "null");
    // Embedded quotes are escaped.
    assert(callFn(eval, "to_json", {S("a\"b")}).value == "\"a\\\"b\"");
    assert(callFn(eval, "to_json", {S("a\nb\t\"\\")}).value ==
           "\"a\\nb\\t\\\"\\\\\"");
    assert(callFn(eval, "to_json", {S(std::string("x\x01y", 3))}).value ==
           "\"x\\u0001y\"");
    assert(callFn(eval, "json_build_object",
                  {S("line\nbreak"), I(1)}).value ==
           "{\"line\\nbreak\":1}");
    assert(callFn(eval, "to_json", {F("NaN")}).value == "\"NaN\"");
    assert(callFn(eval, "to_json", {F("Infinity")}).value ==
           "\"Infinity\"");
    assert(callFn(eval, "to_json", {F("-Infinity")}).value ==
           "\"-Infinity\"");
    assert(callFn(eval, "json_build_array", {F("Infinity")}).value ==
           "[\"Infinity\"]");
    assert(callFn(eval, "to_json", {IA("{1,2,NULL}")}).value ==
           "[1,2,null]");
    assert(callFn(eval, "to_json", {IA("{{1,2},{3,4}}")}).value ==
           "[[1,2],[3,4]]");
    assert(callFn(eval, "to_json",
                  {TA("{a,\"b,c\",NULL,\"NULL\"}")}).value ==
           "[\"a\",\"b,c\",null,\"NULL\"]");
    assert(callFn(eval, "json_build_array", {IA("{1,2}")}).value ==
           "[[1,2]]");
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
