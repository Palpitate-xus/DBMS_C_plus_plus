// PostgreSQL E'...' escape string parsing and quote_literal round trips.

#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <string>

using namespace dbms;

static std::string evaluate(const std::string& expression) {
    const ExprEvalResult result = ExprHelper::evalString(expression, {});
    assert(result.ok && !result.isNull);
    return result.value;
}

int main() {
    assert(evaluate(R"(E'a\\b')") == "a\\b");
    assert(evaluate(R"(e'line\nnext')") == "line\nnext");
    assert(evaluate(R"(E'it\'s')") == "it's");
    assert(evaluate(R"(E'\x41\101')") == "AA");
    assert(evaluate(R"('a\\b')") == "a\\\\b");

    const std::string quoted = evaluate(R"(quote_literal(E'a\\b'))");
    assert(quoted == R"(E'a\\b')");
    assert(evaluate(quoted) == "a\\b");

    std::cout << "[ESCAPE STRING] all passed" << std::endl;
    return 0;
}
