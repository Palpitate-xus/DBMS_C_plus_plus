// IN / NOT IN list parsing and SQL three-valued comparison semantics.
// Driven through ExprHelper so both parser and evaluator paths are covered.

#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <string>

using namespace dbms;

static ExprEvalResult evaluate(const std::string& expression) {
    return ExprHelper::evalString(expression, {});
}

static void expectBoolean(const std::string& expression, bool expected) {
    const ExprEvalResult result = evaluate(expression);
    assert(result.ok && !result.isNull);
    assert(result.value == (expected ? "t" : "f"));
}

static void test_list_expressions() {
    expectBoolean("2 IN (1, 2, 3)", true);
    expectBoolean("4 NOT IN (1, 2, 3)", true);
    expectBoolean("'hello world' IN ('other', 'hello world')", true);
    expectBoolean("',' IN (',', 'x')", true);
    expectBoolean("3 IN (1 + 2, 4 * 2)", true);
    expectBoolean("8 NOT IN (1 + 2, 4 * 2)", false);

    std::cout << "[IN] parsed list expressions OK" << std::endl;
}

static void test_null_semantics() {
    expectBoolean("2 IN (1, NULL, 2)", true);
    expectBoolean("2 NOT IN (1, NULL, 2)", false);

    const ExprEvalResult inUnknown = evaluate("2 IN (1, NULL, 3)");
    assert(inUnknown.ok && inUnknown.isNull);
    const ExprEvalResult notInUnknown = evaluate("2 NOT IN (1, NULL, 3)");
    assert(notInUnknown.ok && notInUnknown.isNull);
    const ExprEvalResult nullInput = evaluate("NULL IN (1, 2, 3)");
    assert(nullInput.ok && nullInput.isNull);

    std::cout << "[IN] NULL semantics OK" << std::endl;
}

int main() {
    test_list_expressions();
    test_null_semantics();
    std::cout << "[IN] all tests passed" << std::endl;
    return 0;
}
