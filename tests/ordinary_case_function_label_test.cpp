#include "parser/parser.h"
#include "parser/query_binding.h"
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include "catalog/systables.h"
#include <functional>
#include <iostream>
#include <stdexcept>

namespace {
void require(bool good,const std::string& message){if(!good)throw std::runtime_error(message);}
std::string identifier(const std::string& input){std::string out="\"";for(char ch:input){out+=ch;if(ch=='\"')out+=ch;}return out+'\"';}
struct Case{std::string sql,label,type;uint32_t oid;int strength;};
}

int main(){
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    size_t controls=0,failures=0,metadataCalls=0;
    const auto check=[&](const std::string& label,const std::function<void()>& action){
        ++controls;try{action();}catch(const std::exception& error){++failures;std::cerr<<"ORDINARY_CASE_FUNCTION_LABEL_FAIL "<<label<<" : "<<error.what()<<'\n';}
    };
    QueryBindingMetadata metadata;
    metadata.relation=[&](const std::string&){++metadataCalls;return QueryRelationMetadata{"public","source",{{"n","integer",false,0,23,-1}}, {}};};
    metadata.functionType=[&](const FunctionCallExpr*){++metadataCalls;return std::string("integer");};
    metadata.declaredType=[](const std::string& spelling){const auto type=ExprHelper::canonicalResultTypeName(spelling);return QueryOutputColumn{"",type,false,0,mapBuiltinTypeNameToOid(type),-1};};
    std::vector<Case> cases;
    const std::vector<std::string> callees={"case_when","\"case_when\"","meta.case_when","\"meta\".\"case_when\"","CASE_WHEN","meta.CASE_WHEN","\"CASE_WHEN\"","meta.\"CASE_WHEN\""};
    for(const auto& callee:callees)for(size_t arity=0;arity<4;++arity)for(size_t form=0;form<4;++form)for(size_t scope=0;scope<4;++scope){
        const bool upper=callee.find("\"CASE_WHEN\"")!=std::string::npos;
        const std::string label=upper?"CASE_WHEN":"case_when";
        const auto args=scope?std::vector<std::string>{"","n","n,1","n,1,n"}:std::vector<std::string>{"","-2","-2,4","-2,1,4"};
        const auto tails=std::vector<std::string>{""," FROM source"," FROM source WHERE false"," FROM source LIMIT 0"};
        const std::string call=callee+'('+args[arity]+')';
        const std::string expression=form==0?call:form==1?"CAST("+call+" AS bigint)":form==2?'('+call+")::text":'('+call+')';
        cases.push_back({"SELECT "+expression+tails[scope],label,form==1?"bigint":form==2?"text":"integer",form==1?20u:form==2?25u:23u,2});
    }
    require(cases.size()==512,"function case inventory");
    const std::vector<Case> real={
        {"SELECT CASE WHEN true THEN 1 ELSE 2 END","case","integer",23,1},
        {"SELECT CAST(CASE WHEN true THEN 1 ELSE 2 END AS bigint)","int8","bigint",20,1},
        {"SELECT CASE WHEN true THEN 1 ELSE abs(-2) END","abs","integer",23,2},
        {"SELECT CASE WHEN true THEN 1 ELSE case_when(-2) END","case_when","integer",23,2},
        {"SELECT CASE WHEN false THEN 1 ELSE CAST(case_when(-2) AS bigint) END","case_when","bigint",20,2},
        {"SELECT CASE 1 WHEN 1 THEN 1 ELSE case_when(-2) END","case_when","integer",23,2},
        {"SELECT CASE WHEN true THEN 1 ELSE n END FROM source","n","integer",23,2},
        {"SELECT CASE WHEN true THEN 1 ELSE CAST(n AS bigint) END FROM source","n","bigint",20,2},
        {"SELECT CASE WHEN true THEN 1 ELSE (CASE WHEN false THEN 1 ELSE case_when(-2) END) END","case_when","integer",23,2},
        {"SELECT CAST(CASE WHEN true THEN 1 ELSE case_when(-2) END AS bigint)","case_when","bigint",20,2},
    };
    cases.insert(cases.end(),real.begin(),real.end());
    require(cases.size()==522,"full case inventory");
    for(const auto& item:cases){
        check("pure syntax label "+item.sql,[&]{
            const auto parsed=SQLParser().parseForBinding(item.sql);require(parsed.success,parsed.error);
            const auto* select=dynamic_cast<const SelectStmt*>(parsed.stmt.get());require(select && select->selectList.size()==1,"one query output");
            const auto before=metadataCalls;
            const auto label=ExprHelper::projectionLabel(select->selectList[0].expr.get(),[&](const std::string& type){return metadata.declaredType(type).type;});
            require(label.first==item.label && label.second==item.strength,"wrong label or strength: "+label.first);
            require(metadataCalls==before,"label lookup consulted routine/relation metadata");
        });
        check("prepared owner "+item.sql,[&]{
            auto query=prepareQuery(item.sql,{},metadata);const auto* select=static_cast<const SelectStmt*>(query.ast.get());
            const auto& output=query.output;require(output.size()==1 && output[0].name==item.label && output[0].type==item.type && output[0].typeOid==item.oid,"prepared name/type/OID");
            const auto& owned=query.statementOutputs.at(select);require(owned.size()==1 && owned[0].name==item.label && owned[0].typeOid==item.oid,"statement output owner");
            require(query.projectionBindings.at(select).size()==1 && query.projectionBindings.at(select)[0].expression==select->selectList[0].expr.get(),"expression owner");
        });
        for(const bool cte:{false,true})check(std::string(cte?"CTE lookup ":"derived lookup ")+item.sql,[&]{
            const auto column=identifier(item.label);
            const auto sql=cte?"WITH named AS ("+item.sql+") SELECT "+column+" FROM named":"SELECT "+column+" FROM ("+item.sql+") AS named";
            const auto query=prepareQuery(sql,{},metadata);
            require(query.output.size()==1 && query.output[0].name==item.label && query.output[0].type==item.type && query.output[0].typeOid==item.oid,"unusable derived/CTE field name");
        });
    }
    for(size_t arity=0;arity<4;++arity)check("manually constructed function "+std::to_string(arity),[&]{
        FunctionCallExpr call;call.funcName="case_when";
        for(size_t i=0;i<arity;++i){auto column=std::make_unique<ColumnRefExpr>();column->column="not_the_function_name";call.args.push_back(std::move(column));}
        require(ExprHelper::projectionLabel(&call)==std::make_pair(std::string("case_when"),2),"function identity guessed from argument count/value");
    });
    if(controls!=2092){++failures;std::cerr<<"ORDINARY_CASE_FUNCTION_LABEL_INVENTORY "<<controls<<'\n';}
    std::cout<<"ORDINARY_CASE_FUNCTION_LABEL_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
