#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include "types/bytea.h"

#include <cassert>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

dbms::ExprValue bytea(const std::string& value) {
    return dbms::ExprValue("bytea", value, false);
}

dbms::ExprValue integer(const std::string& value,
                        const std::string& type = "integer") {
    return dbms::ExprValue(type, value, false);
}

dbms::ExprValue text(const std::string& value) {
    return dbms::ExprValue("text", value, false);
}

std::unique_ptr<dbms::LiteralExpr> literal(const dbms::ExprValue& value) {
    auto result = std::make_unique<dbms::LiteralExpr>();
    result->typeName = value.isNull ? "null" : value.typeName;
    result->value = value.isNull ? "null" : value.value;
    return result;
}

dbms::ExprValue call(dbms::ExprEvaluator& evaluator,
                     const std::string& name,
                     const std::vector<dbms::ExprValue>& arguments) {
    dbms::FunctionCallExpr expression;
    expression.funcName = name;
    for (const auto& argument : arguments)
        expression.args.push_back(literal(argument));
    return evaluator.eval(&expression, {});
}

dbms::ExprValue cast(dbms::ExprEvaluator& evaluator,
                     const dbms::ExprValue& input,
                     const std::string& target) {
    dbms::CastExpr expression;
    expression.operand = literal(input);
    expression.typeName = target;
    return evaluator.eval(&expression, {});
}

dbms::ExprValue binary(dbms::ExprEvaluator& evaluator,
                       const std::string& op,
                       const dbms::ExprValue& left,
                       const dbms::ExprValue& right) {
    dbms::BinaryOpExpr expression;
    expression.op = op;
    expression.left = literal(left);
    expression.right = literal(right);
    return evaluator.eval(&expression, {});
}

void testCodec() {
    dbms::ByteaValue value;
    assert(dbms::ByteaValue::parse("\\x00 AF ff", value));
    assert(value.bytes().size() == 3);
    assert(value.toString() == "\\x00afff");
    assert(!dbms::ByteaValue::parse("\\x0 0", value));
    assert(!dbms::ByteaValue::parse("\\xabc", value));
    const std::string escapeInput = std::string("A") + "\\\\" + "\\000";
    assert(dbms::ByteaValue::parse(escapeInput, value));
    assert(value.toString() == "\\x415c00");
}

void testOperatorsAndLengths() {
    dbms::ExprEvaluator evaluator;
    assert(call(evaluator, "length", {bytea("\\x00ff10")}).value == "3");
    assert(call(evaluator, "octet_length", {bytea("\\x00ff10")}).value == "3");
    assert(call(evaluator, "bit_length", {bytea("\\x00ff10")}).value == "24");
    assert(binary(evaluator, "||", bytea("\\x00ff"), bytea("\\x10")).value ==
           "\\x00ff10");
    assert(binary(evaluator, "<", bytea("\\x7f"), bytea("\\x80")).value ==
           "t");
    assert(binary(evaluator, "<", bytea("\\x01"), bytea("\\x0100")).value ==
           "t");
}

void testByteFunctions() {
    dbms::ExprEvaluator evaluator;
    assert(call(evaluator, "substring",
                {bytea("\\x0102ff04"), integer("2"), integer("2")}).value ==
           "\\x02ff");
    assert(call(evaluator, "reverse", {bytea("\\x0102ff")}).value ==
           "\\xff0201");
    assert(call(evaluator, "position",
                {bytea("\\x02ff"), bytea("\\x0102ff04")}).value == "2");
    assert(call(evaluator, "overlay",
                {bytea("\\x010203"), bytea("\\xaabb"), integer("2")}).value ==
           "\\x01aabb");
    assert(call(evaluator, "btrim",
                {bytea("\\x00ff0100"), bytea("\\x00ff")}).value ==
           "\\x01");
    assert(call(evaluator, "ltrim",
                {bytea("\\x00000100"), bytea("\\x00")}).value ==
           "\\x0100");
    assert(call(evaluator, "rtrim",
                {bytea("\\x00010000"), bytea("\\x00")}).value ==
           "\\x0001");

    assert(call(evaluator, "get_byte",
                {bytea("\\x00ff10"), integer("1")}).value == "255");
    assert(call(evaluator, "set_byte",
                {bytea("\\x00ff10"), integer("1"), integer("7")}).value ==
           "\\x000710");
    assert(call(evaluator, "get_bit",
                {bytea("\\x01"), integer("0")}).value == "1");
    assert(call(evaluator, "get_bit",
                {bytea("\\x01"), integer("7")}).value == "0");
    assert(call(evaluator, "set_bit",
                {bytea("\\x01"), integer("7"), integer("1")}).value ==
           "\\x81");
    assert(call(evaluator, "bit_count", {bytea("\\x00ff81")}).value == "10");
    assert(call(evaluator, "crc32", {bytea("123456789")}).value ==
           "3421780262");
    assert(call(evaluator, "crc32c", {bytea("123456789")}).value ==
           "3808858755");
}

void testIntegerCasts() {
    dbms::ExprEvaluator evaluator;
    assert(cast(evaluator, integer("513"), "bytea").value == "\\x00000201");
    assert(cast(evaluator, integer("-1", "smallint"), "bytea").value ==
           "\\xffff");
    assert(cast(evaluator, bytea("\\x8000"), "integer").value == "32768");
    assert(cast(evaluator, bytea("\\x8000"), "smallint").value == "-32768");
    assert(cast(evaluator, bytea("\\xffff"), "smallint").value == "-1");

    bool rejected = false;
    try {
        (void)cast(evaluator, bytea("\\x010203"), "smallint");
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find("SQLSTATE 22003") !=
                   std::string::npos;
    }
    assert(rejected);
}

void testEncodingConversions() {
    dbms::ExprEvaluator evaluator;
    assert(call(evaluator, "length",
                {bytea("\\xe282ac"), text("UTF8")}).value == "1");
    assert(call(evaluator, "length",
                {bytea("\\xc4"), text("LATIN1")}).value == "1");
    assert(call(evaluator, "convert",
                {bytea("\\xc4"), text("LATIN1"), text("UTF8")}).value ==
           "\\xc384");
    assert(call(evaluator, "convert",
                {bytea("\\xc384"), text("UTF8"), text("LATIN1")}).value ==
           "\\xc4");
    assert(call(evaluator, "convert_from",
                {bytea("\\xc4"), text("LATIN1")}).value == "\xc3\x84");
    assert(call(evaluator, "convert_to",
                {text("\xc3\x84"), text("LATIN1")}).value == "\\xc4");
    assert(call(evaluator, "convert_to",
                {text("ASCII"), text("SQL_ASCII")}).value ==
           "\\x4153434949");

    auto expectError = [&](const std::string& function,
                           const std::vector<dbms::ExprValue>& arguments,
                           const std::string& sqlState) {
        bool rejected = false;
        try {
            (void)call(evaluator, function, arguments);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find(sqlState) !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectError("length", {bytea("\\xc0af"), text("UTF8")}, "22021");
    expectError("convert_to", {text("\xe2\x82\xac"), text("LATIN1")},
                "22021");
    expectError("convert_to", {text("x"), text("NO_SUCH_ENCODING")},
                "22023");
}

void testDigests() {
    dbms::ExprEvaluator evaluator;
    const dbms::ExprValue abc = bytea("\\x616263");
    assert(call(evaluator, "md5", {abc}).value ==
           "900150983cd24fb0d6963f7d28e17f72");
    assert(call(evaluator, "sha224", {abc}).value ==
           "\\x23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7");
    assert(call(evaluator, "sha256", {abc}).value ==
           "\\xba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    assert(call(evaluator, "sha384", {abc}).value ==
           "\\xcb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff"
           "5bed8086072ba1e7cc2358baeca134c825a7");
    assert(call(evaluator, "sha512", {abc}).value ==
           "\\xddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d"
           "39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54"
           "ca49f");

    const dbms::ExprValue binary = bytea("\\x0061626300");
    assert(call(evaluator, "sha224", {binary}).value ==
           "\\xa29e18bf3df333b8289f6769d0761f559c0d2f0f2abed0ad51457fd6");
    assert(call(evaluator, "sha384", {binary}).value ==
           "\\x278217c228368518a415a50853162abd4253541b6600e1ab4896150afce0f0c5"
           "26b6559d804e51bc08e6fdb9954217ac");
    assert(call(evaluator, "sha512", {binary}).value ==
           "\\xbfb2b7e1fad00b91e7cc9a0111a6a31cb0b7cd310298d56be47adeec765b8a6f"
           "db1956aea9d02c728336bd60852f553dc0c53e29e33fbef42d1847683ef07044");
}

}  // namespace

int main() {
    testCodec();
    testOperatorsAndLengths();
    testByteFunctions();
    testIntegerCasts();
    testEncodingConversions();
    testDigests();
    std::cout << "[BYTEA FUNCTIONS] all passed" << std::endl;
    return 0;
}
