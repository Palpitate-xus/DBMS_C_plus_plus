// ============================================================================
// String function library test — Phase 4 Wave 4.19a
// Exercises the expanded set of PostgreSQL string functions registered in
// ExprEvaluator: char_length/octet_length/bit_length, substr, lpad/rpad,
// btrim, split_part, strpos, initcap, to_hex, concat_ws, starts_with,
// translate, overlay, quote_literal/quote_ident, and the chars-argument
// variants of trim/ltrim/rtrim.
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
        // A NULL ExprValue is represented at the literal layer by the text "null",
        // which evalLiteral maps back to a NULL ExprValue.
        lit->value = a.isNull ? "null" : a.value;
        lit->typeName = a.isNull ? "null" : a.typeName;
        call.args.push_back(std::move(lit));
    }
    dbms::RowContext ctx;
    return eval.eval(&call, ctx);
}

static dbms::ExprValue S(const std::string& v) { return dbms::ExprValue("text", v, false); }
static dbms::ExprValue I(int64_t v) { return dbms::ExprValue("integer", std::to_string(v), false); }

static void test_length_family() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "char_length", {S("hello")}).value == "5");
    assert(callFn(eval, "character_length", {S("héllo")}).value == "6");  // bytes for non-ASCII
    assert(callFn(eval, "octet_length", {S("abc")}).value == "3");
    assert(callFn(eval, "bit_length", {S("abc")}).value == "24");
    std::cout << "[STRFN] length family OK" << std::endl;
}

static void test_substr() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "substr", {S("alphabet"), I(3)}).value == "phabet");
    assert(callFn(eval, "substr", {S("alphabet"), I(3), I(2)}).value == "ph");
    // Non-positive start clamps; length window shrinks accordingly (PG semantics).
    assert(callFn(eval, "substr", {S("alphabet"), I(0), I(2)}).value == "a");
    assert(callFn(eval, "substr", {S("alphabet"), I(20)}).value == "");
    std::cout << "[STRFN] substr OK" << std::endl;
}

static void test_pad() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "lpad", {S("hi"), I(5)}).value == "   hi");
    assert(callFn(eval, "lpad", {S("hi"), I(5), S("xy")}).value == "xyxhi");
    assert(callFn(eval, "lpad", {S("hello"), I(3)}).value == "hel");   // truncates
    assert(callFn(eval, "rpad", {S("hi"), I(5)}).value == "hi   ");
    assert(callFn(eval, "rpad", {S("hi"), I(5), S("xy")}).value == "hixyx");
    assert(callFn(eval, "rpad", {S("hello"), I(3)}).value == "hel");
    std::cout << "[STRFN] lpad/rpad OK" << std::endl;
}

static void test_trim_chars() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "btrim", {S("  spaced  ")}).value == "spaced");
    assert(callFn(eval, "btrim", {S("xxhixx"), S("x")}).value == "hi");
    assert(callFn(eval, "ltrim", {S("xxhixx"), S("x")}).value == "hixx");
    assert(callFn(eval, "rtrim", {S("xxhixx"), S("x")}).value == "xxhi");
    assert(callFn(eval, "trim", {S("...hi.."), S(".")}).value == "hi");
    // Default whitespace behavior still works with one arg.
    assert(callFn(eval, "trim", {S("  hi  ")}).value == "hi");
    std::cout << "[STRFN] trim with chars OK" << std::endl;
}

static void test_split_strpos() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "split_part", {S("a,b,c"), S(","), I(2)}).value == "b");
    assert(callFn(eval, "split_part", {S("a,b,c"), S(","), I(-1)}).value == "c");
    assert(callFn(eval, "split_part", {S("a,b,c"), S(","), I(9)}).value == "");
    assert(callFn(eval, "strpos", {S("high"), S("ig")}).value == "2");
    assert(callFn(eval, "strpos", {S("high"), S("zz")}).value == "0");
    std::cout << "[STRFN] split_part/strpos OK" << std::endl;
}

static void test_initcap_tohex() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "initcap", {S("hi THERE ji-ha")}).value == "Hi There Ji-Ha");
    assert(callFn(eval, "to_hex", {I(255)}).value == "ff");
    assert(callFn(eval, "to_hex", {I(0)}).value == "0");
    assert(callFn(eval, "to_hex", {I(4096)}).value == "1000");
    std::cout << "[STRFN] initcap/to_hex OK" << std::endl;
}

static void test_concat_ws_starts_translate() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "concat_ws", {S(","), S("a"), S("b"), S("c")}).value == "a,b,c");
    // NULL argument is skipped, not propagated.
    dbms::ExprValue nullArg("text", "", true);
    assert(callFn(eval, "concat_ws", {S("-"), S("x"), nullArg, S("z")}).value == "x-z");
    assert(callFn(eval, "starts_with", {S("alphabet"), S("alph")}).value == "t");
    assert(callFn(eval, "starts_with", {S("alphabet"), S("beta")}).value == "f");
    assert(callFn(eval, "translate", {S("12345"), S("143"), S("ax")}).value == "a2x5");  // 1->a, 4->x, 3 deleted
    std::cout << "[STRFN] concat_ws/starts_with/translate OK" << std::endl;
}

static void test_overlay_quote() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "overlay", {S("Txxxxas"), S("hom"), I(2), I(4)}).value == "Thomas");
    assert(callFn(eval, "overlay", {S("abcdef"), S("XY"), I(3)}).value == "abXYef");
    assert(callFn(eval, "quote_literal", {S("O'Brien")}).value == "'O''Brien'");
    assert(callFn(eval, "quote_ident", {S("simple")}).value == "simple");
    assert(callFn(eval, "quote_ident", {S("Mixed Case")}).value == "\"Mixed Case\"");
    std::cout << "[STRFN] overlay/quote OK" << std::endl;
}

static void test_null_propagation() {
    dbms::ExprEvaluator eval;
    dbms::ExprValue nullArg("text", "", true);
    assert(callFn(eval, "char_length", {nullArg}).isNull);
    assert(callFn(eval, "lpad", {nullArg, I(5)}).isNull);
    assert(callFn(eval, "initcap", {nullArg}).isNull);
    assert(callFn(eval, "strpos", {S("x"), nullArg}).isNull);
    std::cout << "[STRFN] NULL propagation OK" << std::endl;
}

int main() {
    test_length_family();
    test_substr();
    test_pad();
    test_trim_chars();
    test_split_strpos();
    test_initcap_tohex();
    test_concat_ws_starts_translate();
    test_overlay_quote();
    test_null_propagation();
    std::cout << "[STRFN] all passed" << std::endl;
    return 0;
}
