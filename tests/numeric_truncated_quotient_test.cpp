#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto check = [](const std::string& expression, const std::string& expected) {
        const auto result = dbms::ExprHelper::evalString(expression, {});
        assert(result.ok && !result.isNull && result.value == expected);
    };
    check("mod(100000000000000000000001::numeric,3)", "2");
    check("100000000000000000000001::numeric%3", "2");
    check("div(100000000000000000000001::numeric,3)", "33333333333333333333333");
    check("mod(-100000000000000000000001::numeric,3)", "-2");
    check("div(-100000000000000000000001::numeric,3)", "-33333333333333333333333");
    check("mod(100000000000000000000001::numeric,-3)", "2");
    check("div(100000000000000000000001::numeric,-3)", "-33333333333333333333333");
    check("mod(-100000000000000000000001::numeric,-3)", "-2");
    check("div(-100000000000000000000001::numeric,-3)", "33333333333333333333333");
    check("mod(10.50::numeric,3.0::numeric)", "1.50");
    check("div(10.50::numeric,3.0::numeric)", "3");
    check("div(0.00::numeric,7)", "0");
    std::cout << "[NUMERIC TRUNCATED QUOTIENT] passed" << std::endl;
}
