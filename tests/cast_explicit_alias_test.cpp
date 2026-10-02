#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const std::string& expression : {"1::integer", "1::numeric(10,2)",
            "1::double precision", "NULL::text", "1::integer::text", "relname::text"}) {
        const auto parsed = parser.parse("SELECT " + expression +
            " AS \"Label Name\",2::integer AS n FROM pg_class;");
        assert(parsed.success);
        const auto* query = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
        assert(query && query->selectList.size() == 2);
        assert(query->selectList[0].alias == "\"Label Name\"" ||
               query->selectList[0].alias == "Label Name");
        assert(query->selectList[1].alias == "n");
        const auto* cast = dynamic_cast<const dbms::BinaryOpExpr*>(query->selectList[0].expr.get());
        assert(cast && cast->op == "::");
        const auto* type = dynamic_cast<const dbms::LiteralExpr*>(cast->right.get());
        assert(type && type->value.find("as") == std::string::npos);
    }
    const auto noAlias = parser.parse("SELECT 1::integer+2::integer;");
    assert(noAlias.success);
    std::cout << "[CAST EXPLICIT ALIAS] passed\n";
}
