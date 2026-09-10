// ============================================================================
// Encoding / hash function test — Phase 4 Wave 4.19d
// Exercises md5 and encode/decode (hex, base64, escape) in ExprEvaluator,
// validated against well-known reference vectors.
// ============================================================================

#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include <cassert>
#include <iostream>
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
        lit->value = a.isNull ? "null" : a.value;
        lit->typeName = a.isNull ? "null" : a.typeName;
        call.args.push_back(std::move(lit));
    }
    dbms::RowContext ctx;
    return eval.eval(&call, ctx);
}

static dbms::ExprValue S(const std::string& v) { return dbms::ExprValue("text", v, false); }
static dbms::ExprValue C(const std::string& v) { return dbms::ExprValue("character", v, false); }
static dbms::ExprValue B(const std::string& v) { return dbms::ExprValue("bytea", v, false); }

static void expectInvalidEncoding(dbms::ExprEvaluator& eval,
                                  const std::string& value,
                                  const std::string& format) {
    bool rejected = false;
    try {
        (void)callFn(eval, "decode", {S(value), S(format)});
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find("SQLSTATE 22023") !=
                   std::string::npos;
    }
    assert(rejected);
}

static void test_md5() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "md5", {S("")}).value == "d41d8cd98f00b204e9800998ecf8427e");
    assert(callFn(eval, "md5", {S("abc")}).value == "900150983cd24fb0d6963f7d28e17f72");
    assert(callFn(eval, "md5", {C("ab  ")}).value ==
           "187ef4436122d1cc2f40dc2b92f0eba0");
    assert(callFn(eval, "md5", {S("The quick brown fox jumps over the lazy dog")}).value
           == "9e107d9d372bb6826bd81d3542a419d6");
    assert(callFn(eval, "md5", {dbms::ExprValue("text", "", true)}).isNull);
    std::cout << "[ENCFN] md5 OK" << std::endl;
}

static void test_hex() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "encode", {S("abc"), S("hex")}).value == "616263");
    assert(callFn(eval, "decode", {S("616263"), S("hex")}).value == "\\x616263");
    assert(callFn(eval, "decode", {C("6162  "), C("hex  ")}).value == "\\x6162");
    assert(callFn(eval, "encode", {S("ab"), C("hex  ")}).value == "6162");
    assert(callFn(eval, "encode", {B("\\x6162"), S("hex")}).value == "6162");
    // Round-trip with uppercase hex digits and whitespace tolerance on decode.
    assert(callFn(eval, "decode", {S("4D 61 6E"), S("hex")}).value == "\\x4d616e");
    // Odd-length hex is rejected.
    expectInvalidEncoding(eval, "616", "hex");
    expectInvalidEncoding(eval, "zz", "hex");
    std::cout << "[ENCFN] hex OK" << std::endl;
}

static void test_base64() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "encode", {S("Man"), S("base64")}).value == "TWFu");
    assert(callFn(eval, "encode", {S("M"), S("base64")}).value == "TQ==");
    assert(callFn(eval, "encode", {S("Ma"), S("base64")}).value == "TWE=");
    assert(callFn(eval, "decode", {S("TWFu"), S("base64")}).value == "\\x4d616e");
    assert(callFn(eval, "decode", {S("TQ=="), S("base64")}).value == "\\x4d");
    assert(callFn(eval, "decode", {S("T Q\n=\t="), S("base64")}).value ==
           "\\x4d");
    expectInvalidEncoding(eval, "TW$u", "base64");
    expectInvalidEncoding(eval, "TQ", "base64");
    expectInvalidEncoding(eval, "TWE", "base64");
    expectInvalidEncoding(eval, "A===", "base64");
    // Round-trip a longer string.
    std::string msg = "hello, world!";
    auto enc = callFn(eval, "encode", {S(msg), S("base64")});
    const auto decoded = callFn(eval, "decode", {S(enc.value), S("base64")});
    assert(callFn(eval, "encode", {decoded, S("base64")}).value == enc.value);
    std::cout << "[ENCFN] base64 OK" << std::endl;
}

static void test_escape() {
    dbms::ExprEvaluator eval;
    // Backslash is doubled; printable text is unchanged.
    assert(callFn(eval, "encode", {S("a\\b"), S("escape")}).value == "a\\\\b");
    // A control byte becomes an octal escape; round-trips through decode.
    std::string raw = std::string("x") + '\007' + "y";  // 0x07 = \007
    auto enc = callFn(eval, "encode", {S(raw), S("escape")});
    assert(enc.value == "x\\007y");
    const auto decoded = callFn(eval, "decode", {S(enc.value), S("escape")});
    assert(callFn(eval, "encode", {decoded, S("escape")}).value == enc.value);
    const auto zero = callFn(eval, "decode", {S("\\000"), S("escape")});
    assert(zero.value == "\\x00");
    auto expectInvalidEscape = [&](const std::string& value) {
        bool rejected = false;
        try {
            (void)callFn(eval, "decode", {S(value), S("escape")});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22P02") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectInvalidEscape("\\");
    expectInvalidEscape("\\12");
    expectInvalidEscape("\\128");
    expectInvalidEscape("\\400");
    expectInvalidEscape("\\x");
    std::cout << "[ENCFN] escape OK" << std::endl;
}

static void test_invalid_format() {
    dbms::ExprEvaluator eval;
    auto expectInvalidParameter = [&](const std::string& function) {
        bool rejected = false;
        try {
            (void)callFn(eval, function, {S("abc"), S("rot13")});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22023") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectInvalidParameter("encode");
    expectInvalidParameter("decode");
    std::cout << "[ENCFN] invalid format OK" << std::endl;
}

int main() {
    test_md5();
    test_hex();
    test_base64();
    test_escape();
    test_invalid_format();
    std::cout << "[ENCFN] all passed" << std::endl;
    return 0;
}
