#include "parser/parser.h"
#include "parser/query_binding.h"
#include <cassert>
#include <iostream>
int main(){
    using namespace dbms;SQLParser parser;
    for(const auto& keyword:std::vector<std::string>{"CURRENT_TIMESTAMP","CURRENT_TIMESTAMP(3)","CURRENT_DATE","CURRENT_TIME","LOCALTIME","LOCALTIMESTAMP","CURRENT_USER","SESSION_USER","CURRENT_ROLE","CURRENT_CATALOG","CURRENT_SCHEMA"}){
        auto parsed=parser.parseForBinding("SELECT CASE "+keyword+" WHEN NULL THEN 1 ELSE 2 END");assert(parsed.isValid());
        auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());assert(select);
        auto* conditional=dynamic_cast<CaseExpr*>(select->selectList.front().expr.get());assert(conditional);
        assert(dynamic_cast<FunctionCallExpr*>(conditional->switchExpr.get()));
        assert(!dynamic_cast<LiteralExpr*>(conditional->switchExpr.get()));
        std::cout<<keyword<<" FunctionCall, not planning literal\n";
    }
    for(const auto& variable:std::vector<std::string>{"v","\"current_date\""}){
        auto parsed=parser.parseForBinding("SELECT CASE "+variable+" WHEN NULL THEN 1 ELSE 2 END");assert(parsed.isValid());
        auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());assert(select);
        auto* conditional=dynamic_cast<CaseExpr*>(select->selectList.front().expr.get());assert(conditional);
        assert(dynamic_cast<ColumnRefExpr*>(conditional->switchExpr.get()));
        std::cout<<variable<<" ColumnRef, not planning literal\n";
    }
    QueryBindingDatum datum;datum.identity="case-bound-variable";datum.name="v";datum.type="integer";datum.position=1;
    for(const auto& variable:std::vector<std::string>{"v","$1"}){
        auto prepared=prepareQuery("SELECT CASE "+variable+" WHEN 1 THEN 1 ELSE 2 END",{datum},{});
        auto* select=dynamic_cast<SelectStmt*>(prepared.ast.get());assert(select);
        auto* conditional=dynamic_cast<CaseExpr*>(select->selectList.front().expr.get());assert(conditional);
        auto* parameter=dynamic_cast<ParameterExpr*>(conditional->switchExpr.get());assert(parameter);
        assert(parameter->declaredType=="integer" && prepared.parameters.at(parameter->slot).isNull);
        std::cout<<variable<<" nullable Parameter, not planning literal\n";
    }
}
