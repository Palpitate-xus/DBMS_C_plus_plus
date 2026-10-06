#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <cmath>
#include <iostream>
#include <string>

static void expect(const std::string& expression, double wanted, const std::string& type) {
    const auto result = dbms::ExprHelper::evalStringWithNulls(expression, {}, {});
    if (!result.ok || result.isNull || result.typeName != type || std::stod(result.value) != wanted) {
        std::cerr << expression << ": " << result.value << " " << result.typeName << " " << result.error << '\n';
        assert(false);
    }
}

static void error(const std::string& expression, const std::string& state) {
    const auto result = dbms::ExprHelper::evalStringWithNulls(expression, {}, {});
    assert(!result.ok && result.error.find("SQLSTATE " + state) != std::string::npos);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    expect("CAST(16777216 AS REAL)+CAST(1 AS REAL)", 16777216, "real");
    expect("CAST(9007199254740992 AS DOUBLE PRECISION)+CAST(1 AS DOUBLE PRECISION)", 9007199254740992.0, "double precision");
    expect("CAST(16777216 AS REAL)+1", 16777217, "double precision");
    expect("CAST(16777216 AS REAL)+CAST(1 AS NUMERIC)", 16777217, "double precision");
    expect("CAST(16777216 AS REAL)+'1'", 16777216, "real");
    expect("CAST(3 AS REAL)/CAST(2 AS REAL)", 1.5, "real");
    expect("CAST(3 AS DOUBLE PRECISION)/CAST(2 AS DOUBLE PRECISION)", 1.5, "double precision");
    expect("CAST(1.5 AS REAL)*CAST(2 AS REAL)", 3, "real");
    expect("CAST(1.5 AS REAL)-CAST(2 AS REAL)", -0.5, "real");
    expect("CAST(0.1 AS REAL)+CAST(0 AS INT)", static_cast<double>(0.1f), "double precision");
    expect("CAST(0.1 AS DOUBLE PRECISION)+CAST(0.2 AS DOUBLE PRECISION)", 0.30000000000000004, "double precision");
    expect("CAST(0.1 AS REAL)^CAST(2 AS REAL)", std::pow(static_cast<double>(0.1f), 2), "double precision");
    expect("CAST(1 AS REAL)/CAST('Infinity' AS REAL)", 0, "real");
    error("CAST(3.4028235e38 AS REAL)*CAST(2 AS REAL)", "22003");
    error("CAST(1.7976931348623157e308 AS DOUBLE PRECISION)*CAST(2 AS DOUBLE PRECISION)", "22003");
    error("CAST('1e-45' AS REAL)/CAST(2 AS REAL)", "22003");
    error("CAST('5e-324' AS DOUBLE PRECISION)/CAST(2 AS DOUBLE PRECISION)", "22003");
    error("CAST(1 AS REAL)/CAST(0 AS REAL)", "22012");
    error("CAST(1 AS REAL)%CAST(1 AS REAL)", "42883");
    error("CAST(1 AS REAL)+'oops'", "22P02");
    for (const std::string right : {"real", "integer", "numeric", "double precision"}) {
        const auto result = dbms::ExprHelper::evalStringWithNulls(
            "l+r", {{"l", ""}, {"r", "1"}}, {"l"}, {{"l", "real"}, {"r", right}});
        assert(result.ok && result.isNull && result.typeName == (right == "real" ? "real" : "double precision"));
    }
    const auto powerNull = dbms::ExprHelper::evalStringWithNulls(
        "l^r", {{"l", ""}, {"r", "1"}}, {"l"}, {{"l", "real"}, {"r", "real"}});
    assert(powerNull.ok && powerNull.isNull && powerNull.typeName == "double precision");
    const auto nan = dbms::ExprHelper::evalStringWithNulls("CAST('Infinity' AS REAL)+CAST('-Infinity' AS REAL)", {}, {});
    assert(nan.ok && !nan.isNull && nan.typeName == "real" && nan.value == "NaN");
    const auto infinity = dbms::ExprHelper::evalStringWithNulls("CAST('Infinity' AS REAL)*CAST(2 AS REAL)", {}, {});
    assert(infinity.ok && infinity.value == "Infinity" && infinity.typeName == "real");
    std::cout << "[FLOATING ARITHMETIC WIDTH] IEEE rounding, promotion, NULL and range errors passed\n";
}
