#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    SQLParser parser;
    for (const auto& sql : {
        "SELECT 1", "SELECT 1,2", "SELECT -1", "SELECT +1", "SELECT .5", "SELECT 1e3",
        "SELECT NULL", "SELECT TRUE", "SELECT NOT FALSE", "SELECT 'writer() FROM t'",
        "SELECT B'101'", "SELECT 1+2*3", "SELECT 1/0", "SELECT 'a'||'b'",
        "SELECT 1=1 AND 2<3", "SELECT 1 IN(1,2)",
        "SELECT 1 WHERE FALSE", "SELECT 1 ORDER BY 1 LIMIT 0 OFFSET 2",
        "SELECT DISTINCT 1", "/* FROM nextval() */ SELECT 1 AS value",
    }) {
        auto parsed = parser.parseForBinding(sql);
        assert(parsed.success && parsed.stmt);
        if (!SQLParser::isDatabaseIndependentQuery(*parsed.stmt))
            std::cerr << "EXPECTED INDEPENDENT: " << sql << '\n';
        assert(SQLParser::isDatabaseIndependentQuery(*parsed.stmt));
    }
    for (const auto& sql : {
        "SELECT id FROM t", "SELECT id", "SELECT *", "SELECT $1", "SELECT abs(1)",
        "SELECT nextval('s')", "SELECT writer(1)", "SELECT 1+writer(1)",
        "SELECT 1 WHERE writer(1)=1", "SELECT 1 ORDER BY writer(1)",
        "SELECT DISTINCT ON(writer(1)) 1", "SELECT (SELECT writer(1))",
        "SELECT EXISTS(SELECT 1)", "WITH q AS(SELECT writer(1)) SELECT 1",
        "SELECT CAST('1' AS INT)", "SELECT 1::INT", "SELECT DATE '2026-10-06'",
        "SELECT 1 COLLATE \"C\"", "SELECT CURRENT_USER", "SELECT CURRENT_TIMESTAMP",
        "SELECT CASE WHEN TRUE THEN 1 ELSE writer(1) END", "SELECT 1 AS v ORDER BY v",
        "SELECT 1 UNION SELECT 2", "SELECT count(*)", "VALUES(1)",
        "SELECT 1 BETWEEN 0 AND 2", // current parser's intrinsic-call envelope stays owned
    }) {
        auto parsed = parser.parseForBinding(sql);
        assert(parsed.success && parsed.stmt);
        assert(!SQLParser::isDatabaseIndependentQuery(*parsed.stmt));
    }
    // Snapshot demand is orthogonal and remains unchanged in user txns.
    assert(SQLParser::requiresQuerySnapshot("SELECT 1"));
    auto unknown = parser.parseForBinding("SELECT 1");
    auto* select = static_cast<SelectStmt*>(unknown.stmt.get());
    auto* literal = static_cast<LiteralExpr*>(select->selectList[0].expr.get());
    literal->typeName = "unknown";
    assert(!SQLParser::isDatabaseIndependentQuery(*select));
    literal->typeName.clear(); literal->value = "(SELECT writer(1))";
    assert(!SQLParser::isDatabaseIndependentQuery(*select));
    literal->value = "1"; literal->preparedSubquery = std::make_shared<SelectStmt>();
    assert(!SQLParser::isDatabaseIndependentQuery(*select));
    std::cout << "[LITERAL QUERY TRANSACTION DEMAND] passed\n";
}
