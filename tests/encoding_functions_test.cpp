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

static void test_md5() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "md5", {S("")}).value == "d41d8cd98f00b204e9800998ecf8427e");
    assert(callFn(eval, "md5", {S("abc")}).value == "900150983cd24fb0d6963f7d28e17f72");
    assert(callFn(eval, "md5", {S("The quick brown fox jumps over the lazy dog")}).value
           == "9e107d9d372bb6826bd81d3542a419d6");
    assert(callFn(eval, "md5", {dbms::ExprValue("text", "", true)}).isNull);
    std::cout << "[ENCFN] md5 OK" << std::endl;
}

static void test_hex() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "encode", {S("abc"), S("hex")}).value == "616263");
    assert(callFn(eval, "decode", {S("616263"), S("hex")}).value == "abc");
    // Round-trip with uppercase hex digits and whitespace tolerance on decode.
    assert(callFn(eval, "decode", {S("4D 61 6E"), S("hex")}).value == "Man");
    // Odd-length hex is rejected.
    assert(callFn(eval, "decode", {S("616"), S("hex")}).isNull);
    std::cout << "[ENCFN] hex OK" << std::endl;
}

static void test_base64() {
    dbms::ExprEvaluator eval;
    assert(callFn(eval, "encode", {S("Man"), S("base64")}).value == "TWFu");
    assert(callFn(eval, "encode", {S("M"), S("base64")}).value == "TQ==");
    assert(callFn(eval, "encode", {S("Ma"), S("base64")}).value == "TWE=");
    assert(callFn(eval, "decode", {S("TWFu"), S("base64")}).value == "Man");
    assert(callFn(eval, "decode", {S("TQ=="), S("base64")}).value == "M");
    // Round-trip a longer string.
    std::string msg = "hello, world!";
    auto enc = callFn(eval, "encode", {S(msg), S("base64")});
    assert(callFn(eval, "decode", {S(enc.value), S("base64")}).value == msg);
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
    assert(callFn(eval, "decode", {S(enc.value), S("escape")}).value == raw);
    std::cout << "[ENCFN] escape OK" << std::endl;
}

int main() {
    test_md5();
    test_hex();
    test_base64();
    test_escape();
    std::cout << "[ENCFN] all passed" << std::endl;
    return 0;
}
