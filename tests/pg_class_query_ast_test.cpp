#include "parser/parser.h"
#include "catalog/systables.h"
#include <cassert>
#include <iostream>

int main() {
    assert(dbms::mapBuiltinTypeNameToOid("\"char\"") == 18);
    assert(dbms::mapBuiltinTypeNameToOid("char") == 1042);
    assert(dbms::mapBuiltinTypeNameToOid("character") == 1042);
    dbms::SQLParser parser;
    const auto parsed = parser.parse(
        "SELECT relname::text AS \"Label Name\",relkind::text,relpersistence::text "
        "FROM pg_catalog.pg_class WHERE relname='catalog_fixture';");
    assert(parsed.success);
    const auto* query = dynamic_cast<const dbms::SelectStmt*>(parsed.stmt.get());
    assert(query && query->selectList.size() == 3);
    assert(query->selectList[0].alias == "\"Label Name\"" ||
           query->selectList[0].alias == "Label Name");
    for (const auto& item : query->selectList) {
        const auto* cast = dynamic_cast<const dbms::BinaryOpExpr*>(item.expr.get());
        assert(cast && cast->op == "::");
        const auto* operand = dynamic_cast<const dbms::ColumnRefExpr*>(cast->left.get());
        const auto* type = dynamic_cast<const dbms::LiteralExpr*>(cast->right.get());
        assert(operand && type && type->value == "text");
    }
    std::cout << "[PG CLASS QUERY AST] passed\n";
}
