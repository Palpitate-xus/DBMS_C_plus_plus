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
#include <limits>
#include <memory>
#include <stdexcept>
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
static dbms::ExprValue C(const std::string& v) { return dbms::ExprValue("character", v, false); }

static void test_length_family() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "char_length", {S("hello")}).value == "5");
    assert(callFn(eval, "length", {S("héllo")}).value == "5");
    assert(callFn(eval, "character_length", {S("héllo")}).value == "5");
    assert(callFn(eval, "octet_length", {S("héllo")}).value == "6");
    assert(callFn(eval, "octet_length", {S("abc")}).value == "3");
    assert(callFn(eval, "bit_length", {S("abc")}).value == "24");
    const dbms::ExprValue padded("character", "ab  ", false);
    assert(callFn(eval, "length", {padded}).value == "2");
    assert(callFn(eval, "char_length", {padded}).value == "2");
    assert(callFn(eval, "character_length", {padded}).value == "2");
    assert(callFn(eval, "octet_length", {padded}).value == "4");
    assert(callFn(eval, "bit_length", {padded}).value == "16");
    const dbms::ExprValue paddedUtf8("bpchar", "你好  ", false);
    assert(callFn(eval, "length", {paddedUtf8}).value == "2");
    assert(callFn(eval, "octet_length", {paddedUtf8}).value == "8");
    assert(callFn(eval, "bit_length", {paddedUtf8}).value == "48");
    std::cout << "[STRFN] length family OK" << std::endl;
}

static void test_substr() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "substr", {S("alphabet"), I(3)}).value == "phabet");
    assert(callFn(eval, "substr", {S("alphabet"), I(3), I(2)}).value == "ph");
    assert(callFn(eval, "substr", {S("aé中z"), I(2), I(2)}).value ==
           "é中");
    assert(callFn(eval, "substring", {S("aé中z"), I(2), I(2)}).value ==
           "é中");
    assert(callFn(eval, "substring", {C("abcd  "), I(2), I(4)}).value ==
           "bcd");
    // Non-positive start clamps; length window shrinks accordingly (PG semantics).
    assert(callFn(eval, "substr", {S("alphabet"), I(0), I(2)}).value == "a");
    assert(callFn(eval, "substr", {S("alphabet"), I(20)}).value == "");
    bool negativeLengthRejected = false;
    try {
        (void)callFn(eval, "substring", {S("alphabet"), I(2), I(-1)});
    } catch (const std::runtime_error& error) {
        negativeLengthRejected =
            std::string(error.what()).find("SQLSTATE 22011") !=
            std::string::npos;
    }
    assert(negativeLengthRejected);
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
    assert(callFn(eval, "lpad", {S("é"), I(2), S("界")}).value ==
           "界é");
    assert(callFn(eval, "rpad", {S("é"), I(3), S("界a")}).value ==
           "é界a");
    assert(callFn(eval, "lpad", {S("éx"), I(1), S("z")}).value ==
           "é");
    assert(callFn(eval, "lpad", {C("ab  "), I(4), S("x")}).value ==
           "xxab");
    assert(callFn(eval, "rpad", {C("ab  "), I(4), S("x")}).value ==
           "abxx");
    assert(callFn(eval, "lpad", {C("ab  "), I(5), C("x  ")}).value ==
           "xxxab");
    const dbms::ExprValue nullFill("text", "", true);
    assert(callFn(eval, "lpad", {S("hi"), I(5), nullFill}).isNull);
    std::cout << "[STRFN] lpad/rpad OK" << std::endl;
}

static void test_trim_chars() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "btrim", {S("  spaced  ")}).value == "spaced");
    assert(callFn(eval, "btrim", {S("xxhixx"), S("x")}).value == "hi");
    assert(callFn(eval, "ltrim", {S("xxhixx"), S("x")}).value == "hixx");
    assert(callFn(eval, "rtrim", {S("xxhixx"), S("x")}).value == "xxhi");
    assert(callFn(eval, "trim", {S("...hi.."), S(".")}).value == "hi");
    assert(callFn(eval, "btrim", {S("éhelloé"), S("é")}).value ==
           "hello");
    assert(callFn(eval, "ltrim", {S("丰x"), S("中估")}).value ==
           "丰x");
    assert(callFn(eval, "rtrim", {C("abxx  "), S("x")}).value == "ab");
    assert(callFn(eval, "btrim", {C("xxabxx  "), S("x")}).value == "ab");
    assert(callFn(eval, "trim", {C("abxx  "), S("x")}).value == "ab");
    assert(callFn(eval, "trim", {S("trailing"), S("x"), C("abxx  ")})
               .value == "ab");
    // Default whitespace behavior still works with one arg.
    assert(callFn(eval, "trim", {S("  hi  ")}).value == "hi");
    // PostgreSQL's default trim character is a space, not every ASCII
    // whitespace character.
    const std::string tabDelimited = "\t x \t";
    assert(callFn(eval, "trim", {S(tabDelimited)}).value == tabDelimited);
    assert(callFn(eval, "btrim", {S(tabDelimited)}).value == tabDelimited);
    assert(callFn(eval, "ltrim", {S(tabDelimited)}).value == tabDelimited);
    assert(callFn(eval, "rtrim", {S(tabDelimited)}).value == tabDelimited);
    std::cout << "[STRFN] trim with chars OK" << std::endl;
}

static void test_split_strpos() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "left",
                  {S("abc"), I(std::numeric_limits<int64_t>::min())})
               .value.empty());
    assert(callFn(eval, "right",
                  {S("abc"), I(std::numeric_limits<int64_t>::min())})
               .value.empty());
    assert(callFn(eval, "split_part", {S("a,b,c"), S(","), I(2)}).value == "b");
    assert(callFn(eval, "split_part", {S("a,b,c"), S(","), I(-1)}).value == "c");
    assert(callFn(eval, "split_part", {S("a,b,c"), S(","), I(9)}).value == "");
    assert(callFn(eval, "split_part", {C("a,b  "), C(",  "), I(2)})
               .value == "b");
    bool zeroFieldRejected = false;
    try {
        (void)callFn(eval, "split_part", {S("a,b,c"), S(","), I(0)});
    } catch (const std::runtime_error& error) {
        zeroFieldRejected =
            std::string(error.what()).find("SQLSTATE 22023") !=
            std::string::npos;
    }
    assert(zeroFieldRejected);
    assert(callFn(eval, "strpos", {S("high"), S("ig")}).value == "2");
    assert(callFn(eval, "strpos", {S("high"), S("zz")}).value == "0");
    assert(callFn(eval, "strpos", {C("ab  "), C("  ")}).value == "1");
    std::cout << "[STRFN] split_part/strpos OK" << std::endl;
}

static void test_initcap_tohex() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "initcap", {S("hi THERE ji-ha")}).value == "Hi There Ji-Ha");
    assert(callFn(eval, "initcap", {C("hi  ")}).value == "Hi");
    assert(callFn(eval, "reverse", {S("aé中")}).value == "中éa");
    assert(callFn(eval, "lower", {C("AB  ")}).value == "ab");
    assert(callFn(eval, "upper", {C("ab  ")}).value == "AB");
    assert(callFn(eval, "reverse", {C("ab  ")}).value == "ba");
    assert(callFn(eval, "ascii", {S("é")}).value == "233");
    assert(callFn(eval, "ascii", {S("中")}).value == "20013");
    assert(callFn(eval, "ascii", {S("")}).value == "0");
    assert(callFn(eval, "chr", {I(233)}).value == "é");
    assert(callFn(eval, "chr", {I(20013)}).value == "中");
    auto expectChrError = [&](int64_t codePoint, const std::string& sqlstate) {
        bool rejected = false;
        try {
            (void)callFn(eval, "chr", {I(codePoint)});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE " + sqlstate) !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectChrError(-1, "22023");
    expectChrError(0, "54000");
    expectChrError(0xd800, "54000");
    expectChrError(0x110000, "54000");
    assert(callFn(eval, "to_hex", {I(255)}).value == "ff");
    assert(callFn(eval, "to_hex", {I(0)}).value == "0");
    assert(callFn(eval, "to_hex", {I(4096)}).value == "1000");
    assert(callFn(eval, "to_hex", {I(-1)}).value == "ffffffff");
    assert(callFn(eval, "to_hex",
                  {dbms::ExprValue("integer", "-2147483648", false)})
               .value == "80000000");
    assert(callFn(eval, "to_hex",
                  {dbms::ExprValue("bigint", "-1", false)})
               .value == "ffffffffffffffff");
    assert(callFn(eval, "to_hex",
                  {dbms::ExprValue("bigint", "-9223372036854775808", false)})
               .value == "8000000000000000");
    std::cout << "[STRFN] initcap/to_hex OK" << std::endl;
}

static void test_bpchar_text_inputs() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "replace", {C("ab  "), C("a "), C("x  ")}).value ==
           "xb");
    assert(callFn(eval, "position", {S(" "), C("ab  ")}).value == "0");
    assert(callFn(eval, "position", {C("b "), C("ab  ")}).value == "2");
    assert(callFn(eval, "left", {C("ab  "), I(4)}).value == "ab");
    assert(callFn(eval, "right", {C("ab  "), I(1)}).value == "b");
    assert(callFn(eval, "repeat", {C("ab  "), I(2)}).value == "abab");
    assert(callFn(eval, "ascii", {C("   ")}).value == "0");
    std::cout << "[STRFN] bpchar text inputs OK" << std::endl;
}

static void test_concat_ws_starts_translate() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "concat_ws", {S(","), S("a"), S("b"), S("c")}).value == "a,b,c");
    // NULL argument is skipped, not propagated.
    dbms::ExprValue nullArg("text", "", true);
    assert(callFn(eval, "concat_ws", {S("-"), S("x"), nullArg, S("z")}).value == "x-z");
    assert(callFn(eval, "starts_with", {S("alphabet"), S("alph")}).value == "t");
    assert(callFn(eval, "starts_with", {S("alphabet"), S("beta")}).value == "f");
    assert(callFn(eval, "starts_with", {C("ab  "), S("ab ")}).value == "f");
    assert(callFn(eval, "translate", {S("12345"), S("143"), S("ax")}).value == "a2x5");  // 1->a, 4->x, 3 deleted
    assert(callFn(eval, "translate",
                  {S("aé中"), S("é中"), S("界")}).value == "a界");
    assert(callFn(eval, "translate",
                  {S("xé"), S("xé"), S("中a")}).value == "中a");
    assert(callFn(eval, "translate", {C("ab  "), C("a "), C("x ")})
               .value == "xb");
    std::cout << "[STRFN] concat_ws/starts_with/translate OK" << std::endl;
}

static void test_overlay_quote() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "overlay", {S("Txxxxas"), S("hom"), I(2), I(4)}).value == "Thomas");
    assert(callFn(eval, "overlay", {S("abcdef"), S("XY"), I(3)}).value == "abXYef");
    assert(callFn(eval, "overlay",
                  {S("abc"), S(""), I(2), I(1)}).value == "ac");
    assert(callFn(eval, "overlay",
                  {S("aé中z"), S("X"), I(2), I(2)}).value == "aXz");
    assert(callFn(eval, "overlay",
                  {S("aé中z"), S("界"), I(2)}).value == "a界中z");
    assert(callFn(eval, "overlay", {C("abcdef  "), C("X  "), I(2), I(3)})
               .value == "aXef");
    assert(callFn(eval, "overlay",
                  {S("abcdef"), S("X"), I(2), I(-1)}).value ==
           "aXabcdef");
    assert(callFn(eval, "overlay",
                  {S("abcdef"), S("X"), I(7), I(-2)}).value ==
           "abcdefXef");
    bool invalidStartRejected = false;
    try {
        (void)callFn(eval, "overlay", {S("abcdef"), S("X"), I(0)});
    } catch (const std::runtime_error& error) {
        invalidStartRejected =
            std::string(error.what()).find("SQLSTATE 22011") !=
            std::string::npos;
    }
    assert(invalidStartRejected);
    assert(callFn(eval, "quote_literal", {S("O'Brien")}).value == "'O''Brien'");
    assert(callFn(eval, "quote_ident", {S("simple")}).value == "simple");
    assert(callFn(eval, "quote_ident", {S("select")}).value == "\"select\"");
    assert(callFn(eval, "quote_ident", {S("user")}).value == "\"user\"");
    assert(callFn(eval, "quote_ident", {S("between")}).value ==
           "\"between\"");
    assert(callFn(eval, "quote_ident", {S("text")}).value == "text");
    assert(callFn(eval, "quote_ident", {S("Mixed Case")}).value == "\"Mixed Case\"");
    assert(callFn(eval, "quote_literal", {C("ab  ")}).value == "'ab'");
    assert(callFn(eval, "quote_ident", {C("ab  ")}).value == "ab");
    std::cout << "[STRFN] overlay/quote OK" << std::endl;
}

static void test_null_propagation() {
    dbms::ExprEvaluator eval;
    dbms::ExprValue nullArg("text", "", true);
    assert(callFn(eval, "char_length", {nullArg}).isNull);
    assert(callFn(eval, "lpad", {nullArg, I(5)}).isNull);
    assert(callFn(eval, "initcap", {nullArg}).isNull);
    assert(callFn(eval, "strpos", {S("x"), nullArg}).isNull);
    assert(callFn(eval, "btrim", {S(" x "), nullArg}).isNull);
    assert(callFn(eval, "trim", {S(" x "), nullArg}).isNull);
    assert(callFn(eval, "ltrim", {S(" x "), nullArg}).isNull);
    assert(callFn(eval, "rtrim", {S(" x "), nullArg}).isNull);
    assert(callFn(eval, "overlay",
                  {S("abc"), S("X"), I(2), nullArg}).isNull);
    std::cout << "[STRFN] NULL propagation OK" << std::endl;
}

int main() {
    test_length_family();
    test_substr();
    test_pad();
    test_trim_chars();
    test_split_strpos();
    test_initcap_tohex();
    test_bpchar_text_inputs();
    test_concat_ws_starts_translate();
    test_overlay_quote();
    test_null_propagation();
    std::cout << "[STRFN] all passed" << std::endl;
    return 0;
}
