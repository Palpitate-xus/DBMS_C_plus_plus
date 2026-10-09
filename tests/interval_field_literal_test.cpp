#include "parser/parser.h"
#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    using namespace dbms;
    const std::vector<std::pair<std::string, std::string>> fields = {
        {"YEAR", "2 years"}, {"MONTH", "2 mons"}, {"DAY", "2 days"},
        {"HOUR", "02:00:00"}, {"MINUTE", "00:02:00"}, {"SECOND", "00:00:02"},
        {"YEAR TO MONTH", "2 mons"}, {"DAY TO HOUR", "02:00:00"},
        {"DAY TO MINUTE", "00:02:00"}, {"DAY TO SECOND", "00:00:02"},
        {"HOUR TO MINUTE", "00:02:00"}, {"HOUR TO SECOND", "00:00:02"},
        {"MINUTE TO SECOND", "00:00:02"}
    };
    for (const auto& control : fields) {
        const std::string sql = "SELECT INTERVAL '2' " + control.first;
        SQLParser parser;
        const auto parsed = parser.parseForBinding(sql);
        assert(parsed.isValid());
        const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
        assert(select && select->selectList.size() == 1);
        const auto* literal = dynamic_cast<const LiteralExpr*>(select->selectList[0].expr.get());
        assert(literal && literal->typeName == "INTERVAL " + control.first);
        assert(literal->sourceBegin == 7 && literal->sourceEnd == sql.size());
        const auto value = ExprHelper::evalString(sql.substr(7), {});
        assert(value.ok && !value.isNull && value.value == control.second);
    }
    for (const std::string sql : {"SELECT INTERVAL DAY '2'", "SELECT INTERVAL YEAR TO MONTH '2'",
        "SELECT INTERVAL(3) '2' DAY", "SELECT INTERVAL '2' YEAR TO DAY",
        "SELECT INTERVAL '2' DAY(3)", "SELECT INTERVAL '2' SECOND(-1)"}) {
        SQLParser parser;
        const auto parsed = parser.parseForBinding(sql);
        assert(!parsed.isValid() && parsed.sqlState == "42601");
    }
    const auto round = ExprHelper::evalString("INTERVAL '2.3456 seconds' DAY TO SECOND(3)", {});
    assert(round.ok && round.value == "00:00:02.346");
    const auto prefix = ExprHelper::evalString("INTERVAL(3) '2.3456 seconds'", {});
    assert(prefix.ok && prefix.value == "00:00:02.346");
    const auto sum = ExprHelper::evalString("INTERVAL '2' DAY + INTERVAL '3' DAY", {});
    assert(sum.ok && sum.value == "5 days");
    std::cout << "[INTERVAL FIELD LITERAL] exact AST/source/value and grammar controls passed\n";
}
