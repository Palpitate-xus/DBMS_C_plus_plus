// ============================================================================
// format() / null-coalescing function test — Phase 4 Wave 4.19i
// Exercises format(fmtstr, ...) with %s/%I/%L/%% specifiers and the 2-arg
// null-coalescing aliases nvl / ifnull.
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

static dbms::ExprValue S(const std::string& v) { return dbms::ExprValue("text", v, false); }
static dbms::ExprValue I(int64_t v) { return dbms::ExprValue("integer", std::to_string(v), false); }
static dbms::ExprValue NULLV() { return dbms::ExprValue("text", "", true); }

static void test_format() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "format", {S("Hello %s"), S("World")}).value == "Hello World");
    assert(callFn(eval, "format", {S("%s = %s"), S("a"), I(1)}).value == "a = 1");
    // %% is a literal percent.
    assert(callFn(eval, "format", {S("100%%")}).value == "100%");
    // %I quotes only when needed.
    assert(callFn(eval, "format", {S("table %I"), S("my table")}).value == "table \"my table\"");
    assert(callFn(eval, "format", {S("col %I"), S("name")}).value == "col name");
    // %L quotes as a SQL literal; NULL -> NULL.
    assert(callFn(eval, "format", {S("val %L"), S("O'Brien")}).value == "val 'O''Brien'");
    assert(callFn(eval, "format", {S("val %L"), NULLV()}).value == "val NULL");
    // %s of NULL renders empty.
    assert(callFn(eval, "format", {S("[%s]"), NULLV()}).value == "[]");
    std::cout << "[FORMATFN] format OK" << std::endl;
}

static void test_nvl_ifnull() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "nvl", {NULLV(), S("fallback")}).value == "fallback");
    assert(callFn(eval, "nvl", {S("present"), S("fallback")}).value == "present");
    assert(callFn(eval, "ifnull", {NULLV(), I(7)}).value == "7");
    assert(callFn(eval, "ifnull", {I(3), I(7)}).value == "3");
    std::cout << "[FORMATFN] nvl/ifnull OK" << std::endl;
}

int main() {
    test_format();
    test_nvl_ifnull();
    std::cout << "[FORMATFN] all passed" << std::endl;
    return 0;
}
