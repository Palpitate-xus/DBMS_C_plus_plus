#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    dbms::SQLParser parser;
    for (const std::string suffix : {"", ";", "; /* trailing */", "; -- trailing\n"}) {
        auto parsed = parser.parse("SELECT 1 AS n, NULL AS absent, '; -- /* literal */' AS data" + suffix);
        assert(parsed.success);
        const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
        assert(select && select->selectList.size() == 3);
        assert(select->selectList[0].alias == "n");
        assert(select->selectList[1].alias == "absent");
        assert(select->selectList[2].alias == "data");
        const auto* literal = dynamic_cast<const dbms::LiteralExpr*>(select->selectList[2].expr.get());
        // LiteralExpr retains the quoted SQL token, not its decoded datum.
        assert(literal && literal->value == "'; -- /* literal */'");
    }
    auto parsed = parser.parse("SELECT 1;");
    const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
    assert(parsed.success && select && select->selectList.size() == 1);
    std::cout << "[SELECT PROJECTION TERMINATOR] passed\n";
}
