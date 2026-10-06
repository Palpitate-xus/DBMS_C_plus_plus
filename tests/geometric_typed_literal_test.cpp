#include "parser/query_binding.h"
#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    for (const auto& item : std::vector<std::pair<std::string,std::string>>{
        {"point", "(1,2)"}, {"line", "{1,2,3}"}, {"lseg", "[(0,0),(1,2)]"},
        {"box", "(2,3),(0,0)"}, {"path", "[(0,0),(1,2)]"},
        {"polygon", "((0,0),(1,0),(0,1))"}, {"circle", "<(1,2),3>"}}) {
        auto query=prepareQuery("SELECT CASE true WHEN true THEN "+item.first+" '"+item.second+"' ELSE NULL END",{},metadata);
        assert(query.output.at(0).type==item.first);
        auto* select=dynamic_cast<SelectStmt*>(query.ast.get());
        const auto* conditional=dynamic_cast<CaseExpr*>(select->selectList[0].expr.get());
        const auto* literal=dynamic_cast<LiteralExpr*>(conditional->whenClauses[0].second.get());
        assert(literal && literal->typeName==item.first && literal->sourceEnd>literal->sourceBegin);
        ExprEvaluator evaluator;
        const auto value=evaluator.eval(literal,RowContext{});
        assert(value.typeName==item.first && value.value==item.second && !value.isNull);
        bool rejected=false;
        try { (void)prepareQuery("SELECT CASE true WHEN true THEN "+item.first+" 'bad' ELSE NULL END WHERE false",{},metadata); }
        catch(const DbError& error) { rejected=error.sqlState()=="22P02"; }
        assert(rejected);
        LiteralExpr invalid;invalid.typeName=item.first;invalid.value="'bad'";rejected=false;
        try { (void)evaluator.eval(&invalid,RowContext{}); }
        catch(const DbError& error) { rejected=error.sqlState()=="22P02"; }
        assert(rejected);
    }
    std::cout<<"geometric typed literals retain grammar, descriptor and validated values\n";
}
