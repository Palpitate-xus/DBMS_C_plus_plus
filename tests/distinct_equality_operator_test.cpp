#include "parser/query_binding.h"
#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include <cassert>
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"CIRCLE '<(0,0),NaN>' IS DISTINCT FROM CIRCLE '<(0,0),NaN>'","1"},
        {"PATH '[(0,0),(1,1)]' IS NOT DISTINCT FROM PATH '[(2,2),(3,3)]'","1"},
        {"LINE '{1,2,3}' IS NOT DISTINCT FROM LINE '{2,4,6}'","1"},
        {"LSEG '[(0,0),(1,1)]' IS DISTINCT FROM LSEG '[(1,1),(0,0)]'","1"},
        {"CAST(NULL AS CIRCLE) IS NOT DISTINCT FROM CAST(NULL AS CIRCLE)","1"},
        {"CAST(NULL AS CIRCLE) IS DISTINCT FROM CIRCLE '<(0,0),1>'","1"},
        {"CIRCLE '<(0,0),1>' IS DISTINCT FROM CAST(NULL AS CIRCLE)","1"}}) {
        auto query=prepareQuery("SELECT CASE WHEN "+item.first+" THEN 1 ELSE 2 END",{},metadata);
        auto* select=dynamic_cast<SelectStmt*>(query.ast.get());
        ExprEvaluator evaluator;
        const auto value=evaluator.eval(select->selectList[0].expr.get(),RowContext{});
        if(value.value!=item.second) std::cerr<<item.first<<" actual="<<value.value<<" expected="<<item.second<<"\n";
        assert(value.value==item.second);
    }
    std::cout<<"DISTINCT dispatches equality rather than an unrelated inequality operator\n";
}
