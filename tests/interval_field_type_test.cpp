#include "parser/parser.h"
#include "catalog/type_registry.h"
#include "expression/expr_helper.h"
#include "utils/interval_type.h"
#include <cassert>
#include <iostream>
#include <string>

int main() {
    using namespace dbms;
    using namespace dbms::interval_type_detail;
    const int masks[] = {year, month, day, hour, minute, second, year | month,
        day | hour, day | hour | minute, day | hour | minute | second,
        hour | minute, hour | minute | second, minute | second};
    size_t checks = 0;
    for (int mask : masks) {
        const std::string spelling = std::string("INTERVAL ") + fields(mask);
        const auto declaration = SQLParser::parseTypeSpecification(spelling);
        assert(SQLParser::toLower(declaration.typeName) == "interval");
        assert(declaration.typeMods == std::vector<std::string>{std::to_string(mask)});
        assert(render(declaration.typeName, declaration.typeMods) == spelling);
        const auto mod = TypeRegistry::instance().applyTypeMods("interval", declaration.typeMods);
        assert(mod.ok() && mod.typmod == static_cast<int32_t>((uint32_t(mask) << 16) | fullPrecision));
        SQLParser parser;
        assert(parser.parseForBinding("SELECT NULL::" + spelling + " IS NULL").isValid());
        checks += 5;
    }
    for (int mask : {second, day | hour | minute | second, hour | minute | second, minute | second}) {
        const std::string spelling = std::string("INTERVAL ") + fields(mask) + "(3)";
        const auto declaration = SQLParser::parseTypeSpecification(spelling);
        assert(declaration.typeMods == (std::vector<std::string>{std::to_string(mask), "3"}));
        assert(render(declaration.typeName, declaration.typeMods) == spelling);
        const auto mod = TypeRegistry::instance().applyTypeMods("interval", declaration.typeMods);
        assert(mod.ok() && mod.typmod == static_cast<int32_t>((uint32_t(mask) << 16) | 3));
        checks += 3;
    }
    for (const std::string suffix : {"YEAR TO DAY", "DAY TO MONTH", "MONTH TO YEAR", "SECOND TO MINUTE",
         "DAY(3)", "YEAR TO MONTH(3)", "(3) DAY TO SECOND", "(3,4)", "SECOND(-1)"}) {
        SQLParser parser;
        const auto parsed = parser.parseForBinding("SELECT NULL::INTERVAL " + suffix);
        assert(!parsed.isValid() && parsed.sqlState == "42601");
        ++checks;
    }
    for (const auto& control : std::vector<std::pair<std::string, std::string>>{
        {"'1.5 seconds'::INTERVAL(0)", "00:00:02"},
        {"'-1.5 seconds'::INTERVAL SECOND(0)", "-00:00:02"},
        {"'14 months 3 days 4 hours'::INTERVAL YEAR", "1 year"},
        {"'14 months 3 days 4 hours'::INTERVAL MONTH", "1 year 2 mons"},
        {"'2'::INTERVAL DAY", "2 days"},
        {"'2'::INTERVAL HOUR", "02:00:00"},
        {"CAST('1.2345 seconds' AS INTERVAL DAY TO SECOND(3))", "00:00:01.235"}}) {
        const auto result = ExprHelper::evalString(control.first, {});
        std::cout << "INTERVAL_FIELDS_NATIVE " << control.first << " actual=" << result.value
                  << " state=" << result.sqlState << std::endl;
        assert(result.ok && !result.isNull && result.value == control.second);
        ++checks;
    }
    for (const std::string expression : {"'9223372036854775807 microseconds'::INTERVAL(0)",
                                         "'-9223372036854775808 microseconds'::INTERVAL(0)"}) {
        const auto result = ExprHelper::evalString(expression, {});
        assert(!result.ok && result.sqlState == "22008");
        ++checks;
    }
    std::cout << "[INTERVAL FIELD TYPE] checks=" << checks << " passed" << std::endl;
}
