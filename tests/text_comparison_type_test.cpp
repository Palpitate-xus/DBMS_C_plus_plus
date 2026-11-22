#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"

#include <cassert>
#include <iostream>
#include <map>
#include <stdexcept>
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
    dbms::ExprEvaluator evaluator;
    auto binding = dbms::ExprEvaluator::resolveComparison("=", "text", "character varying");
    binding.collation = "NOCASE";
    const dbms::ExprValue upper("text", "A"), lower("character varying", "a");
    assert(evaluator.comparePrepared(binding, upper, lower).asBool());
    binding.collation = "C";
    assert(!evaluator.comparePrepared(binding, upper, lower).asBool());
    assert(evaluator.comparePrepared(binding, upper, dbms::ExprValue("text", "", true)).isNull);
    binding.collation = "default";
    const dbms::ExprValue composed("text", "\xC3\xA9");
    const dbms::ExprValue decomposed("text", "e\xCC\x81");
    assert(evaluator.comparePrepared(binding, composed, composed).asBool());
    assert(!evaluator.comparePrepared(binding, composed, decomposed).asBool());

    binding.collation = "dbms_missing_test_locale";
    binding.collationUsesSystemLocale = true;
    bool missingLocale = false;
    try {
        (void)evaluator.comparePrepared(binding, upper, upper);
    } catch (const std::runtime_error&) {
        missingLocale = true;
    }
    assert(missingLocale);
    std::cout << "[TEXT COMPARISON TYPE] passed\n";
}
