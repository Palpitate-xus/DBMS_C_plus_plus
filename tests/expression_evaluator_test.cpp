// test_sources: src/expression/ExprEvaluator.cpp src/parser/parser.cpp src/catalog/type_registry.cpp src/common/Config.cpp src/types/numeric.cpp
#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace dbms;

static void test_literals() {
    ExprEvaluator eval;
    {
        LiteralExpr lit;
        lit.value = "42";
        ExprValue v = eval.eval(&lit, {});
        assert(!v.isNull);
        assert(v.typeName == "integer");
        assert(v.value == "42");
    }
    {
        LiteralExpr lit;
        lit.value = "1e-3";
        ExprValue v = eval.eval(&lit, {});
        assert(v.typeName == "numeric");
        assert(v.value == "1e-3");
    }
    {
        LiteralExpr lit;
        lit.value = "'hello'";
        ExprValue v = eval.eval(&lit, {});
        assert(v.typeName == "character varying");
        assert(v.value == "hello");
    }
    {
        LiteralExpr lit;
        lit.value = "NULL";
        ExprValue v = eval.eval(&lit, {});
        assert(v.isNull);
    }
    {
        LiteralExpr lit;
        lit.value = "TRUE";
        ExprValue v = eval.eval(&lit, {});
        assert(v.typeName == "boolean");
        assert(v.asBool());
    }
    std::cout << "[EXPR] literals OK" << std::endl;
}

static void test_column_refs() {
    ExprEvaluator eval;
    RowContext ctx;
    ctx.set("id", ExprValue("integer", "7", false));
    ctx.set("name", ExprValue("character varying", "Alice", false));
    ctx.set("source.id", ExprValue("integer", "99", false));

    ColumnRefExpr col;
    col.column = "id";
    ExprValue v = eval.eval(&col, ctx);
    assert(v.value == "7");

    col.column = "name";
    v = eval.eval(&col, ctx);
    assert(v.value == "Alice");

    col.column = "missing";
    v = eval.eval(&col, ctx);
    assert(v.isNull);

    col.table = "source";
    col.column = "id";
    v = eval.eval(&col, ctx);
    assert(v.value == "99");

    std::cout << "[EXPR] column refs OK" << std::endl;
}

static void test_arithmetic() {
    ExprEvaluator eval;
    auto makeLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        return e;
    };
    auto bin = std::make_unique<BinaryOpExpr>();
    bin->op = "+";
    bin->left = makeLit("3");
    bin->right = makeLit("4");
    ExprValue v = eval.eval(bin.get(), {});
    assert(v.value == "7");

    bin->op = "*";
    bin->left = makeLit("6");
    bin->right = makeLit("7");
    v = eval.eval(bin.get(), {});
    assert(v.value == "42");

    bin->op = "-";
    bin->left = makeLit("10");
    bin->right = makeLit("4");
    v = eval.eval(bin.get(), {});
    assert(v.value == "6");

    auto expectOverflow = [&](const std::string& op,
                              const std::string& left,
                              const std::string& right) {
        auto expression = std::make_unique<BinaryOpExpr>();
        expression->op = op;
        auto leftLiteral = makeLit(left);
        leftLiteral->typeName = "integer";
        expression->left = std::move(leftLiteral);
        auto rightLiteral = makeLit(right);
        rightLiteral->typeName = "integer";
        expression->right = std::move(rightLiteral);
        bool rejected = false;
        try {
            (void)eval.eval(expression.get(), {});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22003") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectOverflow("+", "9223372036854775807", "1");
    expectOverflow("-", "-9223372036854775808", "1");
    expectOverflow("*", "9223372036854775807", "2");
    expectOverflow("/", "-9223372036854775808", "-1");
    expectOverflow("^", "10", "1000");

    auto exponent = std::make_unique<BinaryOpExpr>();
    exponent->op = "^";
    exponent->left = makeLit("2");
    exponent->right = makeLit("-3");
    ExprValue exponentResult = eval.eval(exponent.get(), {});
    assert(exponentResult.typeName == "double precision");
    assert(exponentResult.value == "0.125");

    exponent->left = makeLit("10");
    exponent->right = makeLit("100");
    exponentResult = eval.eval(exponent.get(), {});
    assert(exponentResult.value == "1e+100");

    auto scientificSum = std::make_unique<BinaryOpExpr>();
    scientificSum->op = "+";
    scientificSum->left = makeLit("1e-3");
    scientificSum->right = makeLit("0.002");
    ExprValue scientificResult = eval.eval(scientificSum.get(), {});
    assert(scientificResult.typeName == "numeric");
    assert(scientificResult.value == "0.003");

    auto typedExponent = [&](const std::string& left,
                             const std::string& leftType,
                             const std::string& right,
                             const std::string& rightType) {
        auto expression = std::make_unique<BinaryOpExpr>();
        expression->op = "^";
        auto leftLiteral = makeLit(left);
        leftLiteral->typeName = leftType;
        expression->left = std::move(leftLiteral);
        auto rightLiteral = makeLit(right);
        rightLiteral->typeName = rightType;
        expression->right = std::move(rightLiteral);
        return eval.eval(expression.get(), {});
    };
    ExprValue numericExponent =
        typedExponent("2.0", "numeric", "3", "integer");
    assert(numericExponent.typeName == "numeric");
    assert(numericExponent.value == "8.0000000000000000");
    numericExponent = typedExponent(
        "12345678901234567890", "numeric", "2", "integer");
    assert(numericExponent.value ==
           "152415787532388367501905199875019052100");
    ExprValue floatExponent = typedExponent(
        "2", "double precision", "3", "double precision");
    assert(floatExponent.typeName == "double precision");
    assert(floatExponent.value == "8");

    for (const std::string type : {"numeric", "double precision"}) {
        bool invalidPowerRejected = false;
        try {
            (void)typedExponent("-2", type, "0.5", type);
        } catch (const std::runtime_error& error) {
            invalidPowerRejected =
                std::string(error.what()).find("SQLSTATE 2201F") !=
                std::string::npos;
        }
        assert(invalidPowerRejected);

        invalidPowerRejected = false;
        try {
            (void)typedExponent("0", type, "-1", type);
        } catch (const std::runtime_error& error) {
            invalidPowerRejected =
                std::string(error.what()).find("SQLSTATE 2201F") !=
                std::string::npos;
        }
        assert(invalidPowerRejected);
    }

    auto unaryOverflow = std::make_unique<UnaryOpExpr>();
    unaryOverflow->op = "-";
    auto minimumInteger = makeLit("-9223372036854775808");
    minimumInteger->typeName = "integer";
    unaryOverflow->operand = std::move(minimumInteger);
    bool unaryRejected = false;
    try {
        (void)eval.eval(unaryOverflow.get(), {});
    } catch (const std::runtime_error& error) {
        unaryRejected = std::string(error.what()).find("SQLSTATE 22003") !=
                        std::string::npos;
    }
    assert(unaryRejected);

    auto expectDivisionByZero = [&](const std::string& left,
                                    const std::string& right) {
        auto expression = std::make_unique<BinaryOpExpr>();
        expression->op = "%";
        expression->left = makeLit(left);
        expression->right = makeLit(right);
        bool rejected = false;
        try {
            (void)eval.eval(expression.get(), {});
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE 22012") !=
                       std::string::npos;
        }
        assert(rejected);
    };
    expectDivisionByZero("10", "0");
    expectDivisionByZero("10.5", "0.0");

    auto minimumModulo = std::make_unique<BinaryOpExpr>();
    minimumModulo->op = "%";
    minimumModulo->left = makeLit("-9223372036854775808");
    minimumModulo->right = makeLit("-1");
    ExprValue minimumModuloResult = eval.eval(minimumModulo.get(), {});
    assert(minimumModuloResult.value == "0");

    std::cout << "[EXPR] arithmetic OK" << std::endl;
}

static void test_comparisons() {
    ExprEvaluator eval;
    auto makeLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        return e;
    };
    auto bin = std::make_unique<BinaryOpExpr>();
    bin->left = makeLit("5");
    bin->right = makeLit("10");

    bin->op = "<";
    assert(eval.eval(bin.get(), {}).asBool());
    bin->op = ">";
    assert(!eval.eval(bin.get(), {}).asBool());
    bin->op = "=";
    assert(!eval.eval(bin.get(), {}).asBool());

    bin->left = makeLit("'abc'");
    bin->right = makeLit("'abc'");
    bin->op = "=";
    assert(eval.eval(bin.get(), {}).asBool());

    std::cout << "[EXPR] comparisons OK" << std::endl;
}

static void test_logical() {
    ExprEvaluator eval;
    auto makeLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        return e;
    };
    auto bin = std::make_unique<BinaryOpExpr>();
    bin->op = "AND";
    bin->left = makeLit("TRUE");
    bin->right = makeLit("FALSE");
    assert(!eval.eval(bin.get(), {}).asBool());

    bin->op = "OR";
    assert(eval.eval(bin.get(), {}).asBool());

    auto un = std::make_unique<UnaryOpExpr>();
    un->op = "NOT";
    un->operand = makeLit("TRUE");
    assert(!eval.eval(un.get(), {}).asBool());

    std::cout << "[EXPR] logical OK" << std::endl;
}

static void test_null() {
    ExprEvaluator eval;
    auto makeLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        return e;
    };
    auto un = std::make_unique<UnaryOpExpr>();
    un->op = "IS NULL";
    un->operand = makeLit("NULL");
    assert(eval.eval(un.get(), {}).asBool());

    un->op = "IS NOT NULL";
    assert(!eval.eval(un.get(), {}).asBool());

    un->op = "IS NULL";
    un->operand = makeLit("'x'");
    assert(!eval.eval(un.get(), {}).asBool());

    std::cout << "[EXPR] null OK" << std::endl;
}

static void test_like() {
    ExprEvaluator eval;
    auto makeLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        return e;
    };
    auto bin = std::make_unique<BinaryOpExpr>();
    bin->op = "like";
    bin->left = makeLit("'hello world'");
    bin->right = makeLit("'hello%'");
    assert(eval.eval(bin.get(), {}).asBool());

    bin->right = makeLit("'h_llo%'");
    assert(eval.eval(bin.get(), {}).asBool());

    bin->op = "not like";
    assert(!eval.eval(bin.get(), {}).asBool());

    std::cout << "[EXPR] like OK" << std::endl;
}

static void test_cast() {
    ExprEvaluator eval;
    auto makeLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        return e;
    };
    auto cast = std::make_unique<CastExpr>();
    cast->operand = makeLit("'123'");
    cast->typeName = "integer";
    ExprValue v = eval.eval(cast.get(), {});
    assert(v.typeName == "integer");
    assert(v.value == "123");

    auto bin = std::make_unique<BinaryOpExpr>();
    bin->op = "::";
    bin->left = makeLit("'456'");
    bin->right = makeLit("integer");
    v = eval.eval(bin.get(), {});
    assert(v.value == "456");

    auto evaluateCast = [&](const std::string& sourceType,
                            const std::string& sourceValue,
                            const std::string& targetType) {
        auto expression = std::make_unique<CastExpr>();
        auto operand = std::make_unique<LiteralExpr>();
        operand->value = sourceValue;
        operand->typeName = sourceType;
        expression->operand = std::move(operand);
        expression->typeName = targetType;
        return eval.eval(expression.get(), {});
    };
    auto expectCastError = [&](const std::string& sourceType,
                               const std::string& sourceValue,
                               const std::string& targetType,
                               const std::string& sqlstate) {
        bool rejected = false;
        try {
            (void)evaluateCast(sourceType, sourceValue, targetType);
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE " + sqlstate) !=
                       std::string::npos;
        }
        if (!rejected) {
            std::cerr << "cast " << sourceType << " value " << sourceValue
                      << " to " << targetType << " did not report SQLSTATE "
                      << sqlstate << std::endl;
        }
        assert(rejected);
    };
    auto evaluateNumericTypmod = [&](const std::string& sourceValue,
                                     std::vector<std::string> modifiers) {
        auto expression = std::make_unique<CastExpr>();
        auto operand = std::make_unique<LiteralExpr>();
        operand->value = sourceValue;
        operand->typeName = "numeric";
        expression->operand = std::move(operand);
        expression->typeName = "numeric";
        expression->typeMods = std::move(modifiers);
        return eval.eval(expression.get(), {});
    };
    auto expectNumericTypmodError = [&](const std::string& sourceValue,
                                        std::vector<std::string> modifiers,
                                        const std::string& sqlstate) {
        bool rejected = false;
        try {
            (void)evaluateNumericTypmod(sourceValue, std::move(modifiers));
        } catch (const std::runtime_error& error) {
            rejected = std::string(error.what()).find("SQLSTATE " + sqlstate) !=
                       std::string::npos;
        }
        assert(rejected);
    };

    assert(evaluateCast("integer", "32767", "smallint").value == "32767");
    assert(evaluateCast("integer", "-32768", "smallint").value == "-32768");
    expectCastError("integer", "32768", "smallint", "22003");
    expectCastError("bigint", "2147483648", "integer", "22003");
    expectCastError("character varying", "9223372036854775808", "bigint",
                    "22003");
    expectCastError("character varying", "3.7", "integer", "22P02");
    expectCastError("character varying", "'true'", "integer", "22P02");

    assert(evaluateCast("numeric", "2.5", "integer").value == "3");
    assert(evaluateCast("numeric", "-2.5", "integer").value == "-3");
    assert(evaluateCast("double precision", "2.5", "integer").value == "2");
    assert(evaluateCast("double precision", "-2.5", "integer").value == "-2");
    expectCastError("numeric", "2147483647.5", "integer", "22003");
    expectCastError("double precision", "1e100", "integer", "22003");

    assert(evaluateCast("boolean", "t", "integer").value == "1");
    expectCastError("boolean", "t", "bigint", "42846");

    assert(evaluateCast("numeric", "1.2345678901234567",
                        "double precision").value ==
           "1.2345678901234567");
    assert(evaluateCast("numeric", "1.2345678901234567", "real").value ==
           "1.2345679");
    expectCastError("character varying", "not-a-number",
                    "double precision", "22P02");
    expectCastError("numeric", "1e100", "real", "22003");
    expectCastError("character varying", "1e1000", "double precision",
                    "22003");
    expectCastError("double precision", "1e-46", "real", "22003");
    assert(evaluateCast("double precision", "1e-45", "real").value ==
           "1e-45");
    assert(evaluateCast("character varying", "Infinity",
                        "double precision").value == "Infinity");
    assert(evaluateCast("character varying", "NaN", "real").value ==
           "NaN");
    expectCastError("boolean", "t", "double precision", "42846");

    assert(evaluateCast("character varying", "TRU", "boolean").value == "t");
    assert(evaluateCast("character varying", "  yes  ", "boolean").value ==
           "t");
    assert(evaluateCast("character varying", "of", "boolean").value == "f");
    assert(evaluateCast("character varying", "0", "boolean").value == "f");
    expectCastError("character varying", "o", "boolean", "22P02");
    expectCastError("character varying", "garbage", "boolean", "22P02");
    assert(evaluateCast("integer", "0", "boolean").value == "f");
    assert(evaluateCast("integer", "-2", "boolean").value == "t");
    expectCastError("bigint", "1", "boolean", "42846");
    expectCastError("smallint", "1", "boolean", "42846");
    expectCastError("numeric", "1", "boolean", "42846");
    expectCastError("double precision", "1", "boolean", "42846");

    expectCastError("character varying", "not-numeric", "numeric",
                    "22P02");
    expectCastError("boolean", "t", "numeric", "42846");
    assert(evaluateCast("character varying", "1.25e3", "numeric").value ==
           "1250");
    assert(evaluateCast("double precision", "1e-20", "numeric").value ==
           "0.00000000000000000001");
    expectCastError("character varying", "1e", "numeric", "22P02");
    expectCastError("character varying", "1e999999999999999999999",
                    "numeric", "22003");
    assert(evaluateNumericTypmod("12.345", {"4", "2"}).value == "12.35");
    assert(evaluateNumericTypmod("7", {"4", "2"}).value == "7.00");
    expectNumericTypmodError("999.99", {"4", "2"}, "22003");
    assert(evaluateNumericTypmod("1499", {"2", "-", "3"}).value ==
           "1000");
    assert(evaluateNumericTypmod("0.00999", {"3", "5"}).value ==
           "0.00999");
    expectNumericTypmodError("0.01", {"3", "5"}, "22003");
    assert(evaluateNumericTypmod("NaN", {"4", "2"}).value == "NaN");
    expectNumericTypmodError("Infinity", {"4", "2"}, "22003");
    expectNumericTypmodError("1", {"0", "0"}, "22023");
    expectNumericTypmodError("1", {"2", "-", "1001"}, "22023");

    auto postfixCast = std::make_unique<BinaryOpExpr>();
    postfixCast->op = "::";
    auto postfixValue = std::make_unique<LiteralExpr>();
    postfixValue->value = "1499";
    postfixValue->typeName = "numeric";
    postfixCast->left = std::move(postfixValue);
    auto postfixType = std::make_unique<LiteralExpr>();
    postfixType->value = "numeric 2 , - 3)";
    postfixCast->right = std::move(postfixType);
    assert(eval.eval(postfixCast.get(), {}).value == "1000");

    std::cout << "[EXPR] cast OK" << std::endl;
}

static void test_numeric() {
    ExprEvaluator eval;
    auto makeNumLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        e->typeName = "numeric";
        return e;
    };

    // Addition preserves scale.
    {
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "+";
        bin->left = makeNumLit("1.23");
        bin->right = makeNumLit("2.77");
        ExprValue v = eval.eval(bin.get(), {});
        assert(v.typeName == "numeric");
        assert(v.value == "4.00");
    }

    // Comparison uses exact decimal semantics.
    {
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "=";
        bin->left = makeNumLit("0.1");
        bin->right = makeNumLit("0.10");
        assert(eval.eval(bin.get(), {}).asBool());
    }

    // Numeric modulo is exact and preserves the wider input scale.
    {
        auto bin = std::make_unique<BinaryOpExpr>();
        bin->op = "%";
        bin->left = makeNumLit("10.50");
        bin->right = makeNumLit("3.000");
        ExprValue v = eval.eval(bin.get(), {});
        assert(!v.isNull && v.value == "1.500");

        bin->left = makeNumLit("-10.5");
        bin->right = makeNumLit("3");
        v = eval.eval(bin.get(), {});
        assert(!v.isNull && v.value == "-1.5");

        bin->left = makeNumLit("100000000000000000000000000000.5");
        bin->right = makeNumLit("3");
        v = eval.eval(bin.get(), {});
        assert(!v.isNull && v.value == "1.5");
    }

    // Cast to numeric.
    {
        auto cast = std::make_unique<CastExpr>();
        cast->operand = makeNumLit("'123.4500'");
        cast->typeName = "numeric";
        ExprValue v = eval.eval(cast.get(), {});
        assert(v.value == "123.4500");
    }

    // abs and round.
    {
        auto f = std::make_unique<FunctionCallExpr>();
        f->funcName = "abs";
        f->args.push_back(makeNumLit("-5.5"));
        ExprValue v = eval.eval(f.get(), {});
        assert(v.value == "5.5");
    }
    {
        auto f = std::make_unique<FunctionCallExpr>();
        f->funcName = "round";
        f->args.push_back(makeNumLit("3.14159"));
        f->args.push_back(makeNumLit("2"));
        ExprValue v = eval.eval(f.get(), {});
        assert(v.value == "3.14");
    }

    std::cout << "[EXPR] numeric OK" << std::endl;
}

static void test_case() {
    ExprEvaluator eval;
    auto makeLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        return e;
    };
    auto c = std::make_unique<CaseExpr>();
    {
        auto when = std::make_unique<BinaryOpExpr>();
        when->op = ">";
        when->left = makeLit("5");
        when->right = makeLit("3");
        c->whenClauses.emplace_back(std::move(when), makeLit("'big'"));
    }
    {
        auto when = std::make_unique<LiteralExpr>();
        when->value = "TRUE";
        c->whenClauses.emplace_back(std::move(when), makeLit("'other'"));
    }
    c->elseExpr = makeLit("'small'");
    ExprValue v = eval.eval(c.get(), {});
    assert(v.value == "big");

    std::cout << "[EXPR] case OK" << std::endl;
}

static void test_functions() {
    ExprEvaluator eval;
    auto makeLit = [](const std::string& s) {
        auto e = std::make_unique<LiteralExpr>();
        e->value = s;
        return e;
    };
    {
        auto f = std::make_unique<FunctionCallExpr>();
        f->funcName = "coalesce";
        f->args.push_back(makeLit("NULL"));
        f->args.push_back(makeLit("'fallback'"));
        ExprValue v = eval.eval(f.get(), {});
        assert(v.value == "fallback");
    }
    {
        auto f = std::make_unique<FunctionCallExpr>();
        f->funcName = "upper";
        f->args.push_back(makeLit("'abc'"));
        ExprValue v = eval.eval(f.get(), {});
        assert(v.value == "ABC");
    }
    {
        auto f = std::make_unique<FunctionCallExpr>();
        f->funcName = "length";
        f->args.push_back(makeLit("'hello'"));
        ExprValue v = eval.eval(f.get(), {});
        assert(v.value == "5");
    }

    std::cout << "[EXPR] functions OK" << std::endl;
}

int main() {
    TypeRegistry::instance().bootstrap();
    test_literals();
    test_column_refs();
    test_arithmetic();
    test_comparisons();
    test_logical();
    test_null();
    test_like();
    test_cast();
    test_numeric();
    test_case();
    test_functions();
    std::cout << "[EXPR] all passed" << std::endl;
    return 0;
}
