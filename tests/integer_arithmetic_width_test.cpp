#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <map>
#include <string>

static void expectError(const std::string& expression, const std::string& state) {
    const auto result = dbms::ExprHelper::evalStringWithNulls(expression, {}, {});
    if (result.ok || result.error.find("SQLSTATE " + state) == std::string::npos) {
        std::cerr << expression << ": " << result.value << " " << result.typeName << " " << result.error << '\n';
        assert(false);
    }
}

static void expect(const std::string& expression, const std::string& value,
                   const std::string& type) {
    const auto result = dbms::ExprHelper::evalStringWithNulls(expression, {}, {});
    assert(result.ok && !result.isNull && result.value == value && result.typeName == type);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    expect("CAST(2147483647 AS INT)+0", "2147483647", "integer");
    expectError("CAST(2147483647 AS INT)+1", "22003");
    expectError("CAST(-2147483648 AS INT)-1", "22003");
    expectError("CAST(1073741824 AS INT)*2", "22003");
    expectError("CAST(-2147483648 AS INT)/-1", "22003");
    expectError("-CAST(-2147483648 AS INT)", "22003");
    expectError("-CAST(-32768 AS SMALLINT)", "22003");
    expect("-2147483648", "-2147483648", "integer");
    expect("-9223372036854775808", "-9223372036854775808", "bigint");
    expect("CAST(-2147483648 AS INT)%-1", "0", "integer");
    expectError("CAST(32767 AS SMALLINT)+CAST(1 AS SMALLINT)", "22003");
    expectError("CAST(-32768 AS SMALLINT)-CAST(1 AS SMALLINT)", "22003");
    expectError("CAST(16384 AS SMALLINT)*CAST(2 AS SMALLINT)", "22003");
    expectError("CAST(-32768 AS SMALLINT)/CAST(-1 AS SMALLINT)", "22003");
    expect("CAST(-32768 AS SMALLINT)%CAST(-1 AS SMALLINT)", "0", "smallint");
    expect("CAST(32767 AS SMALLINT)+CAST(1 AS INT)", "32768", "integer");
    expect("CAST(2147483647 AS INT)+CAST(1 AS BIGINT)", "2147483648", "bigint");
    expect("CAST(9223372036854775807 AS BIGINT)+0", "9223372036854775807", "bigint");
    expectError("CAST(9223372036854775807 AS BIGINT)+1", "22003");
    expectError("CAST(-9223372036854775808 AS BIGINT)/-1", "22003");
    expect("CAST(-9223372036854775808 AS BIGINT)%-1", "0", "bigint");
    expectError("1/0", "22012");
    expectError("CAST(1 AS INT)+'oops'", "22P02");
    expectError("CAST(1 AS SMALLINT)+'32768'", "22003");
    expect("CAST(1 AS SMALLINT)+'2'", "3", "smallint");
    for (const std::string left : {"smallint", "integer", "bigint"}) {
        for (const std::string right : {"smallint", "integer", "bigint"}) {
            const std::map<std::string, int> rank{{"smallint", 1}, {"integer", 2}, {"bigint", 3}};
            const std::string type = rank.at(left) >= rank.at(right) ? left : right;
            const auto result = dbms::ExprHelper::evalStringWithNulls(
                "l+r", {{"l", ""}, {"r", "1"}}, {"l"}, {{"l", left}, {"r", right}});
            assert(result.ok && result.isNull && result.typeName == type);
        }
    }
    std::cout << "[INTEGER ARITHMETIC WIDTH] promotion, typed NULL and overflow passed\n";
}
