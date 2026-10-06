#include "parser/parser.h"
#include "parser/query_binding.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    QueryBindingMetadata metadata;
    metadata.relation = [](const std::string& name) {
        return QueryRelationMetadata{"public",name,{{"id","integer"}}, {}};
    };
    metadata.functionType = [](const FunctionCallExpr*) { return "integer"; };
    for (const auto& sql : {
        "SELECT(WITH c AS(SELECT 7 AS id)SELECT c.id FROM c)",
        "SELECT 1 WHERE(WITH c AS(SELECT 7 AS id)SELECT c.id FROM c)>0",
        "SELECT id FROM t ORDER BY(WITH c AS(SELECT 7 AS id)SELECT c.id FROM c)",
        "SELECT(WITH c AS(SELECT 7 AS id)SELECT(WITH d AS(SELECT c.id AS id)SELECT d.id FROM d)FROM c)",
        "SELECT(CASE WHEN false THEN(WITH c AS(SELECT 1 AS id)SELECT c.id FROM c) ELSE 9 END)"}) {
        SQLParser parser;
        auto parsed = parser.parseForBinding(sql);
        assert(parsed.isValid());
        auto prepared = prepareQuery(sql,{},metadata);
        assert(prepared.ast && !prepared.output.empty());
    }
    auto prepared = prepareQuery("SELECT(WITH c AS(SELECT wanted)SELECT c.wanted FROM c)",
        {{"local:wanted","wanted","bigint",{},true,"2147483648"}},metadata);
    auto* root = static_cast<SelectStmt*>(prepared.ast.get());
    auto* literal = dynamic_cast<LiteralExpr*>(root->selectList[0].expr.get());
    assert(literal && literal->preparedSubquery && literal->typeName == "bigint");
    assert(prepared.parameters.size() == 1 && prepared.output[0].type == "bigint");
    assert(prepared.legacySql().find("CAST('2147483648' AS bigint) AS \"wanted\"") != std::string::npos);
    assert(literal->sourceBegin == prepared.source.find('(') && literal->sourceEnd == prepared.source.size());
    prepared = prepareQuery("SELECT(WITH c AS(SELECT a.id)SELECT c.id FROM c)FROM t a",{},metadata);
    root = static_cast<SelectStmt*>(prepared.ast.get());
    auto* child = static_cast<SelectStmt*>(root->selectList[0].expr->preparedSubquery.get());
    auto* cte = static_cast<SelectStmt*>(child->ctes[0].query.get());
    auto* correlated = static_cast<ColumnRefExpr*>(cte->selectList[0].expr.get());
    assert(correlated->binding && correlated->binding->scopeDepth == 2);
    const auto& source = prepared.sourceRanges[correlated->binding->sourceOrdinal];
    assert(source.owner == root && source.source == root->fromClause.get());
    for (const auto& [sql,state] : std::map<std::string,std::string>{
        {"SELECT(WITH c AS(SELECT 1 AS id)SELECT c.missing FROM c)","42703"},
        {"SELECT(WITH c AS(SELECT 1 AS id)SELECT missing.id FROM c)","42P01"},
        {"SELECT(WITH c AS(SELECT 1 AS id)SELECT c.id,c.id FROM c)","42601"},
        {"SELECT(WITH c AS(SELECT 1 AS id)SELECT c.id FROM c)","ok"},
        {"SELECT(WITH c AS(SELECT 1 AS id)SELECT c.id FROM c", "42601"}}) {
        bool seen = false;
        try { (void)prepareQuery(sql,{},metadata); assert(state == "ok"); }
        catch (const DbError& error) { seen = true; assert(error.sqlState() == state); }
        assert(seen == (state != "ok"));
    }
    std::cout << "[WITH SCALAR SUBQUERY PARSER] passed\n";
}
