// ============================================================================
// Regular-expression function test — Phase 4 Wave 4.19g
// Exercises regexp_replace / regexp_match / regexp_split_to_array /
// regexp_count / regexp_substr in ExprEvaluator (std::regex, ECMAScript).
// Arguments are bound as column references (verbatim) to avoid the literal
// layer reinterpreting backslash patterns.
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

static void test_replace() {
    dbms::ExprEvaluator eval;
    // First match only by default.
    assert(callFn(eval, "regexp_replace", {S("foobarbaz"), S("b.."), S("X")}).value == "fooXbaz");
    // Global flag.
    assert(callFn(eval, "regexp_replace", {S("foobarbaz"), S("b.."), S("X"), S("g")}).value == "fooXX");
    // Backreferences (PG \1 \2 -> swap words).
    assert(callFn(eval, "regexp_replace",
                  {S("John Smith"), S("(\\w+) (\\w+)"), S("\\2 \\1")}).value == "Smith John");
    // Case-insensitive flag.
    assert(callFn(eval, "regexp_replace", {S("HELLO"), S("hello"), S("hi"), S("i")}).value == "hi");
    std::cout << "[REGEXFN] replace OK" << std::endl;
}

static void test_match() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "regexp_match", {S("foobarbequebaz"), S("(bar)(beque)")}).value
           == "{bar,beque}");
    // No parenthesized groups -> single-element array with whole match.
    assert(callFn(eval, "regexp_match", {S("abc"), S("b")}).value == "{b}");
    // No match -> NULL.
    assert(callFn(eval, "regexp_match", {S("abc"), S("(x)")}).isNull);
    std::cout << "[REGEXFN] match OK" << std::endl;
}

static void test_split() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "regexp_split_to_array", {S("a,b,c"), S(",")}).value == "{a,b,c}");
    assert(callFn(eval, "regexp_split_to_array", {S("the quick   brown"), S("\\s+")}).value
           == "{the,quick,brown}");
    std::cout << "[REGEXFN] split OK" << std::endl;
}

static void test_count_substr() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "regexp_count", {S("abcabcabc"), S("abc")}).value == "3");
    assert(callFn(eval, "regexp_count", {S("hello world"), S("o")}).value == "2");
    // Count from a start offset (skip the first 'o').
    assert(callFn(eval, "regexp_count", {S("hello world"), S("o"), I(6)}).value == "1");
    assert(callFn(eval, "regexp_substr", {S("foobar"), S("o+")}).value == "oo");
    // 2nd match.
    assert(callFn(eval, "regexp_substr", {S("a1b2c3"), S("[0-9]"), I(1), I(2)}).value == "2");
    // No match -> NULL.
    assert(callFn(eval, "regexp_substr", {S("abc"), S("[0-9]")}).isNull);
    std::cout << "[REGEXFN] count/substr OK" << std::endl;
}

int main() {
    test_replace();
    test_match();
    test_split();
    test_count_substr();
    std::cout << "[REGEXFN] all passed" << std::endl;
    return 0;
}
