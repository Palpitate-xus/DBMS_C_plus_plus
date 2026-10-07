#include "parser/query_binding.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t failures=0,descriptions=0;
    QueryBindingMetadata metadata;
    metadata.relation=[](const std::string& name) {
        if(name!="source")throw DbError("42P01","missing source");
        return QueryRelationMetadata{"public","source",{{"id","integer"},{"wide","bigint"},{"t","text"}}, {}};
    };
    metadata.functionType=[&](const FunctionCallExpr* call) {
        ++descriptions;
        if(call->funcName!="writer")throw DbError("42883","missing function");
        return "integer";
    };
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"SELECT 1 UNION ALL SELECT 2147483648","bigint"},
        {"SELECT NULL UNION ALL SELECT 1","integer"},
        {"SELECT 1 UNION ALL SELECT NULL","integer"},
        {"SELECT NULL UNION ALL SELECT NULL","text"},
        {"SELECT '1' UNION ALL SELECT 2","integer"},
        {"SELECT 1 UNION ALL SELECT '2'","integer"},
        {"SELECT CAST(1 AS SMALLINT) UNION ALL SELECT 2","integer"},
        {"SELECT CAST(1 AS REAL) UNION ALL SELECT 2","real"},
        {"SELECT ARRAY[1] UNION ALL SELECT ARRAY[2147483648]","bigint[]"},
        {"SELECT id FROM source UNION ALL SELECT wide FROM source","bigint"},
        {"SELECT writer(1) UNION ALL SELECT 2147483648","bigint"},
        {"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 2147483648","bigint"},
    }) {
        std::string actual;
        try{auto query=prepareQuery(control.first,{},metadata);actual=query.output.at(0).type;}
        catch(const DbError& error){actual=error.sqlState();}
        std::cout<<"SET BINDING "<<control.first<<" actual="<<actual<<" expected="<<control.second<<std::endl;
        if(actual!=control.second)++failures;
    }
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"SELECT 1 UNION ALL SELECT 'bad'","22P02"},
        {"SELECT 'bad' UNION ALL SELECT 1","22P02"},
        {"SELECT 1 UNION ALL SELECT CAST('bad' AS TEXT)","42804"},
        {"SELECT TRUE UNION ALL SELECT 1","42804"},
        {"SELECT 1 UNION ALL SELECT 2,3","42601"},
        {"SELECT 1 UNION ALL SELECT 'bad' WHERE missing_set_function(1)=1","42883"},
    }) {
        std::string actual;
        try{(void)prepareQuery(control.first,{},metadata);}catch(const DbError& error){actual=error.sqlState();}
        std::cout<<"SET BINDING ERROR "<<control.first<<" actual="<<actual<<" expected="<<control.second<<std::endl;
        if(actual!=control.second)++failures;
    }
    // This API has only metadata callbacks: no execution callback or source
    // cursor is available to discover a result's type/value by evaluation.
    assert(descriptions==2);
    {
        auto query=prepareQuery("SELECT NULL AS retained UNION ALL SELECT 1",{},metadata);
        const auto* select=dynamic_cast<const SelectStmt*>(query.ast.get());
        assert(select && !select->setOpLhs && select->setOpRhs);
        const auto& inputs=query.setOperationInputs.at(select);
        assert(inputs.left.at(0).type=="integer" && inputs.right.at(0).type=="integer");
        assert(query.output.at(0).name=="retained");
        const auto* cast=dynamic_cast<const CastExpr*>(select->selectList.front().expr.get());
        assert(cast && cast->implicit && cast->typeName=="integer");
        assert(query.projectionBindings.at(select).at(0).expression==cast);
        assert(!query.projectionBindings.at(select).at(0).column);
        assert(cast->sourceBegin==cast->operand->sourceBegin && cast->sourceEnd==cast->operand->sourceEnd);
    }
    {
        auto query=prepareQuery("SELECT p AS original UNION ALL SELECT 2147483648",
            {{"parameter:p","p","integer",{},true,"7",1}},metadata);
        const auto* select=static_cast<const SelectStmt*>(query.ast.get());
        assert(query.output.at(0).type=="bigint" && query.output.at(0).name=="original");
        const auto& inputs=query.setOperationInputs.at(select);
        assert(inputs.left.at(0).type=="integer" && inputs.right.at(0).type=="bigint");
        const auto* parameter=dynamic_cast<const ParameterExpr*>(select->selectList.front().expr.get());
        assert(parameter && parameter->slot==0 && parameter->declaredType=="integer");
        assert(query.parameters.size()==1 && query.parameters.front().value=="7");
        assert(query.uses.size()==1 && query.source.substr(query.uses[0].begin,query.uses[0].end-query.uses[0].begin)=="p");
    }
    {
        auto query=prepareQuery("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 2147483648",{},metadata);
        const auto* select=static_cast<const SelectStmt*>(query.ast.get());
        assert(select->setOpLhs && select->setOpRhs && query.setOperationInputs.size()==2);
        assert(query.setOperationInputs.at(select).left.at(0).type=="integer");
        assert(query.setOperationInputs.at(select).right.at(0).type=="bigint");
    }
    std::cout<<"SET BINDING failures="<<failures<<std::endl;
    assert(failures==0);
}
