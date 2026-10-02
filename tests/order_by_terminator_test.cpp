#include "parser/parser.h"
#include <cassert>
#include <iostream>
#include <string>

int main() {
    dbms::SQLParser parser;
    for (const std::string& suffix : {std::string(), std::string(";"),
             std::string("; /* trailing */"), std::string("; -- trailing\n")}) {
        for (const std::string& order : {std::string("n DESC"),
                 std::string("n DESC NULLS LAST"), std::string("n ASC NULLS FIRST")}) {
            const auto parsed = parser.parse(
                "SELECT relname::text,nullif(relnatts,1) AS n FROM pg_class ORDER BY " + order + suffix);
            assert(parsed.success);
            const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
            assert(select && select->orderBy.size() == 1);
            const auto* column = dynamic_cast<const dbms::ColumnRefExpr*>(select->orderBy[0].expr.get());
            assert(column && column->column == "n");
            assert(select->orderBy[0].asc == (order.find("ASC") != std::string::npos));
        }
        const auto parsed = parser.parse("SELECT id FROM t ORDER BY id DESC, id ASC" + suffix);
        const auto* select = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
        assert(parsed.success && select && select->orderBy.size() == 2);
        assert(!select->orderBy[0].asc && select->orderBy[1].asc);
    }
    std::cout << "[ORDER BY TERMINATOR] passed\n";
}
