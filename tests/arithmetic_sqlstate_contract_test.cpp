#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    const std::vector<std::pair<std::string,std::string>> failures = {
        {"1/0", "22012"}, {"1%0", "22012"},
        {"CAST(1 AS SMALLINT)/CAST(0 AS SMALLINT)", "22012"},
        {"CAST(1 AS BIGINT)%CAST(0 AS BIGINT)", "22012"},
        {"1.0/0.0", "22012"}, {"1.0%0.0", "22012"},
        {"CAST(1 AS REAL)/CAST(0 AS REAL)", "22012"},
        {"CAST(1 AS DOUBLE PRECISION)/CAST(0 AS DOUBLE PRECISION)", "22012"},
        {"CAST('1' AS MONEY)/CAST('0' AS MONEY)", "22012"},
        {"CAST('1' AS MONEY)/0", "22012"},
        {"INTERVAL '1 second'/0", "22012"},
        {"2147483647+1", "22003"},
        {"CAST(32767 AS SMALLINT)+CAST(1 AS SMALLINT)", "22003"},
        {"CAST('9223372036854775807' AS BIGINT)+CAST(1 AS BIGINT)", "22003"},
        {"CAST('3e38' AS REAL)*CAST(2 AS REAL)", "22003"},
        {"'one'+'two'", "42725"}, {"1+'not an integer'", "22P02"},
    };
    for (const auto& [expression, expected] : failures) {
        const auto result = ExprHelper::evalStringWithNulls(expression, {}, {});
        if (result.ok || result.sqlState != expected)
            std::cerr << expression << " actual ok=" << result.ok << " state=" << result.sqlState
                      << " error=" << result.error << " expected=" << expected << '\n';
        assert(!result.ok && result.sqlState == expected && !result.error.empty());
        if (expression == "1/0")
            assert(result.error == "division by zero (SQLSTATE 22012)");
    }
    const auto good = ExprHelper::evalStringWithNulls("7/2", {}, {});
    assert(good.ok && !good.isNull && good.value == "3" && good.typeName == "integer");
    const auto null = ExprHelper::evalStringWithNulls("NULL::INT/0", {}, {});
    assert(null.ok && null.isNull && null.typeName == "integer");
    const auto lazy = ExprHelper::evalStringWithNulls("CASE WHEN false THEN 1/0 ELSE 7 END", {}, {});
    assert(lazy.ok && lazy.value == "7");
    std::cout << "[ARITHMETIC SQLSTATE CONTRACT] passed\n";
}
