#include "catalog/type_registry.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <map>
#include <string>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::map<std::string, std::string> values{{"a", "10"}, {"b", "2"}};
    for (const std::string type : {"text", "varchar", "character varying"}) {
        const std::map<std::string, std::string> types{{"a", type}, {"b", type}};
        const auto result = dbms::ExprHelper::evalStringWithNulls("a < b", values, {}, types);
        assert(result.ok && !result.isNull && result.value == "t");
    }
    for (const auto& item : std::map<std::string, std::string>{
            {"'010' = '10'", "f"},
            {"'10' < '2'", "t"},
            {"least('10', '2')", "10"},
            {"greatest('10', '2')", "2"},
            {"'10'::integer < '2'::integer", "f"},
            {"'1 '::character(2) = '1'::character(1)", "t"}}) {
        const auto result = dbms::ExprHelper::evalString(item.first, {});
        assert(result.ok && result.value == item.second);
    }
    std::cout << "[TEXT COMPARISON TYPE] passed\n";
}
