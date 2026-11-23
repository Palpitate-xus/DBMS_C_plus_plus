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
    for (const std::string sql : {"SELECT", "SELECT ALL;", "SELECT FROM t;",
                                  "SELECT ALL FROM t LIMIT 1;"}) {
        auto empty = parser.parseForBinding(sql);
        const auto* statement = dynamic_cast<const dbms::SelectStmt*>(empty.stmt.get());
        assert(empty.success && statement && statement->selectList.empty());
    }
    for (const std::string sql : {"SELECT DISTINCT FROM t",
                                  "SELECT DISTINCT ON (id) FROM t",
                                  "SELECT id, FROM t"}) {
        auto invalid = parser.parseForBinding(sql);
        assert(!invalid.success && invalid.sqlState == "42601");
    }
    std::cout << "[SELECT PROJECTION TERMINATOR] passed\n";
}
