// test_sources: src/expression/ExprEvaluator.cpp src/parser/parser.cpp src/catalog/type_registry.cpp src/common/Config.cpp src/types/numeric.cpp
#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <iostream>
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
