#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto rangeError = [](const std::string& expression, const std::string& state = "22015") {
        const auto result = dbms::ExprHelper::evalString(expression, {});
        if (result.ok || result.sqlState != state) {
            std::cerr << expression << ": ok=" << result.ok << " value=" << result.value
                      << " NULL=" << result.isNull << " state=" << result.sqlState << '\n';
            assert(false);
        }
    };
    for (const std::string unit : {"months", "years", "weeks"}) {
        for (const std::string& expression : {
            "INTERVAL '9223372036854775807 " + unit + "'",
            "DATE '2024-02-29'+INTERVAL '9223372036854775807 " + unit + "'",
            "CAST('9223372036854775807 " + unit + "' AS INTERVAL)",
            "'9223372036854775807 " + unit + "'::INTERVAL",
            "CAST(NULL AS DATE)+INTERVAL '9223372036854775807 " + unit + "'"}) {
            rangeError(expression);
        }
    }
    for (const std::string text : {
        "2147483648 months", "-2147483649 months", "2147483648 days", "-2147483649 days",
        "306783379 weeks", "9223372036854775808 microseconds",
        "-9223372036854775809 microseconds", "2562047789 hours", "9223372036854775807:00:00",
        "P9223372036854775807Y", "P2147483648D", "@ 9223372036854775807 months",
        "2147483647 months 1 month", "2147483647 days 1 day",
        "9223372036854775807 microseconds 1 microsecond", "-2147483648 months ago",
        "-9223372036854775808 microseconds ago"}) {
        rangeError("INTERVAL '" + text + "'");
        rangeError("CAST('" + text + "' AS INTERVAL)");
    }
    rangeError("INTERVAL '178956971 years'", "22008");
    rangeError("CAST('178956971 years' AS INTERVAL)", "22008");
    rangeError("INTERVAL '" + std::string(400, '9') + " months'", "22007");
    rangeError("CAST('" + std::string(400, '9') + " months' AS INTERVAL)", "22007");
    for (const std::string text : {
        "2147483647 months", "-2147483648 months", "2147483647 days", "-2147483648 days",
        "9223372036854775807 microseconds", "-9223372036854775808 microseconds",
        "178956971 years -5 months"}) {
        const auto result = dbms::ExprHelper::evalString("INTERVAL '" + text + "'", {});
        assert(result.ok && !result.isNull && result.typeName == "interval");
    }
    for (const std::string expression : {
        "DATE '2024-02-29'+INTERVAL '1 month'",
        "DATE '2024-02-29'+INTERVAL '1 year'",
        "DATE '2024-02-29'+INTERVAL '1 week'",
        "DATE '2024-02-29'+INTERVAL 'P1D'",
        "DATE '2024-02-29'+INTERVAL '@ 1 day'"}) {
        const auto result = dbms::ExprHelper::evalString(expression, {});
        assert(result.ok && !result.isNull);
    }
    const auto nullable = dbms::ExprHelper::evalString("CAST(NULL AS DATE)+INTERVAL '1 day'", {});
    assert(nullable.ok && nullable.isNull);
    const auto minimum = dbms::ExprHelper::evalString(
        "date_trunc('microseconds',INTERVAL '-9223372036854775808 microseconds')", {});
    assert(minimum.ok && !minimum.isNull && minimum.value == "-2562047788:00:54.775808");
    std::cout << "[TYPED INTERVAL RANGE] strict component bounds and NULL propagation passed\n";
}
