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
    struct Case { std::string type,left,right; bool equal; };
    for(const auto& item : std::vector<Case>{
        {"path","[(0,0),(1,1)]","[(7,8),(9,10)]",true},
        {"path","[(0,0),(1,1)]","((7,8),(9,10))",true},
        {"path","[(0,0),(1,1)]","[(0,0)]",false},
        {"circle","<(0,0),3>","<(9,8),3>",true},
        {"circle","<(0,0),3>","<(0,0),4>",false},
        {"circle","<(0,0),1>","<(7,8),1.00000001>",true},
        {"circle","<(0,0),NaN>","<(0,0),NaN>",false},
        {"circle","<(0,0),Infinity>","<(1,1),Infinity>",true},
        {"line","{1,2,3}","{2,4,6}",true},
        {"line","{1,2,3}","{-2,-4,-6}",true},
        {"line","{1,2,3}","{1,2,4}",false},
        {"line","{1,2,3}","{1,2,3.0000001}",true},
        {"line","{NaN,2,3}","{NaN,2,3}",true},
        {"line","{NaN,2,3}","{NaN,2,4}",false},
        {"lseg","[(0,0),(1,1)]","[(3,3),(4,4)]",false},
        {"lseg","[(0,0),(1,1)]","[(1,1),(0,0)]",false},
        {"lseg","[(0,0),(1,1)]","[(0,0),(1.0000001,1)]",true},
        {"lseg","[(NaN,0),(1,1)]","[(NaN,0),(1,1)]",true}}) {
        const auto left="CAST('"+item.left+"' AS "+item.type+")";
        const auto right="CAST('"+item.right+"' AS "+item.type+")";
        auto query=prepareQuery("SELECT CASE "+left+" WHEN "+right+" THEN 1 ELSE 2 END",{},metadata);
        auto* select=dynamic_cast<SelectStmt*>(query.ast.get());
        ExprEvaluator evaluator;
        const auto value=evaluator.eval(select->selectList[0].expr.get(),RowContext{});
        if(value.value!=(item.equal?"1":"2")) std::cerr<<item.type<<" actual="<<value.value<<" expected="<<(item.equal?"1":"2")<<"\n";
        assert(value.value==(item.equal?"1":"2"));
    }
    std::cout<<"geometric equality uses resolved shape semantics, not display strings\n";
}

