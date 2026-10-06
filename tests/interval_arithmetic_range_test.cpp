#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    for (const std::string expression : {
        "INTERVAL '9223372036854775807 microseconds'+INTERVAL '1 microsecond'",
        "INTERVAL '-9223372036854775808 microseconds'-INTERVAL '1 microsecond'",
        "INTERVAL '2147483647 months'+INTERVAL '1 month'",
        "INTERVAL '-2147483648 months'-INTERVAL '1 month'",
        "INTERVAL '2147483647 days'+INTERVAL '1 day'",
        "INTERVAL '-2147483648 days'-INTERVAL '1 day'",
        "INTERVAL '2147483647 months'*2",
        "INTERVAL '2147483647 days'*2",
        "INTERVAL '3000000000 microseconds'*4000000000",
        "INTERVAL '-2147483648 months' / -1",
        "INTERVAL '-2147483648 days' / -1",
        "INTERVAL '-9223372036854775808 microseconds' * -1"}) {
        const auto result = dbms::ExprHelper::evalString(expression, {});
        if (result.ok || result.sqlState != "22008") {
            std::cerr << expression << " ok=" << result.ok << " NULL=" << result.isNull
                      << " value=" << result.value << " state=" << result.sqlState << '\n';
            assert(false);
        }
    }
    for (const auto& control : std::vector<std::pair<std::string,std::string>>{
        {"INTERVAL '-9223372036854775807 microseconds'-INTERVAL '1 microsecond'", "-2562047788:00:54.775808"},
        {"INTERVAL '9223372036854775806 microseconds'+INTERVAL '1 microsecond'", "2562047788:00:54.775807"},
        {"INTERVAL '-9223372036854775808 microseconds'+INTERVAL '0 microseconds'", "-2562047788:00:54.775808"},
        {"INTERVAL '-2147483648 months'+INTERVAL '0 months'", "-178956970 years -8 mons"},
        {"INTERVAL '-2147483648 days'+INTERVAL '0 days'", "-2147483648 days"},
        {"INTERVAL '2 days'*2", "4 days"},
        {"INTERVAL '4 hours'/2", "02:00:00"}}) {
        const auto result = dbms::ExprHelper::evalString(control.first, {});
        assert(result.ok && !result.isNull && result.value == control.second);
    }
    const auto nullable = dbms::ExprHelper::evalString("CAST(NULL AS INTERVAL)+INTERVAL '1 day'", {});
    assert(nullable.ok && nullable.isNull);
    std::cout << "[INTERVAL ARITHMETIC RANGE] exact fields, SQLSTATE and inclusive minimum passed\n";
}
