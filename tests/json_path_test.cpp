// ============================================================================
// JSON path accessor test — Phase 4 Wave 4.19j
// Exercises json_extract_path / json_extract_path_text (and jsonb variants):
// navigating JSON objects by key and arrays by index, with the _text variant
// unquoting strings and mapping JSON null to SQL NULL.
// ============================================================================

#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// Bind args as column refs so JSON text is passed through verbatim (the literal
// layer would otherwise reinterpret scalar JSON).
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
static dbms::ExprValue K(const std::string& v) { return dbms::ExprValue("text", v, false); }

static void test_object_path() {
    dbms::ExprEvaluator eval;
    std::string doc = "{\"a\":{\"b\":42,\"c\":\"hi\"},\"d\":[10,20]}";
    // Sub-object returned as JSON.
    assert(callFn(eval, "json_extract_path", {J(doc), K("a")}).value == "{\"b\":42,\"c\":\"hi\"}");
    // Nested scalar.
    assert(callFn(eval, "json_extract_path", {J(doc), K("a"), K("b")}).value == "42");
    // _text unquotes strings.
    assert(callFn(eval, "json_extract_path_text", {J(doc), K("a"), K("c")}).value == "hi");
    assert(callFn(eval, "json_extract_path_text", {J(doc), K("a"), K("b")}).value == "42");
    std::cout << "[JSONPATH] object path OK" << std::endl;
}

static void test_array_index() {
    dbms::ExprEvaluator eval;
    std::string doc = "{\"d\":[10,20,{\"e\":\"x\"}]}";
    assert(callFn(eval, "json_extract_path", {J(doc), K("d"), K("1")}).value == "20");
    assert(callFn(eval, "json_extract_path_text", {J(doc), K("d"), K("2"), K("e")}).value == "x");
    // jsonb variants share the implementation.
    assert(callFn(eval, "jsonb_extract_path", {J(doc), K("d"), K("0")}).value == "10");
    std::cout << "[JSONPATH] array index OK" << std::endl;
}

static void test_missing_and_null() {
    dbms::ExprEvaluator eval;
    std::string doc = "{\"a\":1,\"n\":null}";
    // Missing key -> NULL.
    assert(callFn(eval, "json_extract_path", {J(doc), K("zzz")}).isNull);
    // Out-of-range index -> NULL.
    assert(callFn(eval, "json_extract_path", {J("[1,2]"), K("9")}).isNull);
    // Stepping into a scalar -> NULL.
    assert(callFn(eval, "json_extract_path", {J(doc), K("a"), K("b")}).isNull);
    // JSON null -> SQL NULL in the _text variant.
    assert(callFn(eval, "json_extract_path_text", {J(doc), K("n")}).isNull);
    // ... but the JSON variant returns the literal token "null".
    assert(callFn(eval, "json_extract_path", {J(doc), K("n")}).value == "null");
    std::cout << "[JSONPATH] missing/null OK" << std::endl;
}

int main() {
    test_object_path();
    test_array_index();
    test_missing_and_null();
    std::cout << "[JSONPATH] all passed" << std::endl;
    return 0;
}
