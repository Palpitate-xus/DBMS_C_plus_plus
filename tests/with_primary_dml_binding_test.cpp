#include "parser/query_binding.h"
#include "parser/parser.h"
#include "expression/prepared_query_execution.h"
#include "common/DbError.h"
#include "expression/assignment_input.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    QueryBindingMetadata metadata;
    size_t physicalLookups=0,functionLookups=0;
    metadata.relation=[&](const std::string& name) {
        ++physicalLookups;
        assert(name=="target" || name=="\"Target\"");
        return QueryRelationMetadata{"public",name=="target"?"target":"Target",{{"id","integer"},{"ID","bigint"}}, {}};
    };
    metadata.functionType=[&](const FunctionCallExpr* call) {
        ++functionLookups;
        if (call->funcName!="writer") throw DbError("42883","missing function");
        return std::string("integer"); // metadata, never execute a callback
    };
    const std::string sql="/*head*/ WITH target AS(SELECT 'UPDATE ) INSERT' AS wrong_name) "
        "INSERT INTO target VALUES(2,2147483648) RETURNING id,\"ID\"";
    assert(SQLParser::classify(sql)==SqlCommand::Insert);
    auto prepared=prepareQuery(sql,{},metadata);
    const auto* envelope=dynamic_cast<const WithStmt*>(prepared.ast.get());
    assert(envelope && envelope->command==SqlCommand::Insert && envelope->ctes.size()==1);
    assert(dynamic_cast<const InsertStmt*>(envelope->statement.get()));
    assert(prepared.output.size()==2 && prepared.output[0].type=="integer" && prepared.output[1].type=="bigint");
    assert(physicalLookups==1 && functionLookups==0);
    const auto& cte=envelope->ctes.front();
    assert(sql.substr(cte.queryBegin,cte.queryEnd-cte.queryBegin)=="SELECT 'UPDATE ) INSERT' AS wrong_name");
    assert(sql.substr(envelope->statementBegin,envelope->statementEnd-envelope->statementBegin)==
        "INSERT INTO target VALUES(2,2147483648) RETURNING id,\"ID\"");
    for (const auto& text : {
        "WITH target AS(SELECT 'x' AS wrong_name) UPDATE target SET id=id+1 RETURNING id",
        "WITH target AS(SELECT 'x' AS wrong_name) DELETE FROM target WHERE id=1 RETURNING id"}) {
        auto query=prepareQuery(text,{},metadata);
        assert(dynamic_cast<const WithStmt*>(query.ast.get()));
        assert(query.output.size()==1 && query.output[0].name=="id" && query.output[0].type=="integer");
    }
    auto parameters=prepareQuery("WITH c AS(SELECT $1 AS value) INSERT INTO target VALUES($2,2147483648) RETURNING id",
        {{"first","arg1","integer",{},true,"1",1},{"second","arg2","integer",{},true,"2",2}},metadata);
    assert(parameters.parameters.size()==2 && parameters.uses.size()==2);
    assert(parameters.legacySql().find("CAST('1' AS integer)")!=std::string::npos);
    metadata.assignmentInput=validateAssignmentInput;
    metadata.relation=[](const std::string& name) {
        return QueryRelationMetadata{"public",name,{{"id","integer"},{"v","interval"}}, {}};
    };
    for (const auto& [text,state] : std::vector<std::pair<std::string,std::string>>{
        {"WITH ins AS(INSERT INTO target VALUES(writer(1),'1 day') RETURNING id) INSERT INTO target VALUES(2,'2147483648 months')","22015"},
        {"INSERT INTO target VALUES(2,'2147483648 months'),(missing_column,'1 day')","22015"},
        {"INSERT INTO target VALUES(missing_column,'2147483648 months')","42703"},
        {"INSERT INTO target SELECT 2,'2147483648 months' WHERE unknown()=1","42883"},
        {"INSERT INTO target SELECT 2,CAST('2147483648 months' AS INTERVAL) WHERE unknown()=1","22015"},
        {"INSERT INTO target SELECT 2,INTERVAL '1 fortnight' WHERE unknown()=1","22007"},
        {"INSERT INTO target VALUES(2,CAST('1 fortnight' AS INTERVAL),3)","22007"},
        {"INSERT INTO target VALUES(2,'1 fortnight',unknown())","42883"},
        {"INSERT INTO target VALUES(2,'1 fortnight',3)","42601"},
        {"WITH c AS(SELECT '1 day' AS v) INSERT INTO target SELECT 2,v FROM c WHERE false","42804"},
        {"WITH c AS(SELECT NULL AS v) INSERT INTO target SELECT 2,v FROM c WHERE false","42804"},
        {"WITH c AS(INSERT INTO target VALUES(2,'1 day')) INSERT INTO target SELECT id,'1 day' FROM c","0A000"}}) {
        bool failed=false;
        try {(void)prepareQuery(text,{},metadata);}catch(const DbError& error){
            if(error.sqlState()!=state)std::cerr<<text<<" actual="<<error.sqlState()<<" "<<error.message()<<'\n';
            failed=error.sqlState()==state;
        }
        assert(failed);
    }
    for (const auto& [text,state] : std::vector<std::pair<std::string,std::string>>{
        {"WITH c AS(SELECT unknown()) INSERT INTO target VALUES(writer(1),2)","42883"},
        {"WITH c AS(SELECT 1) UPDATE target SET id=missing_column", "42703"},
        {"SELECT( WITH c AS(SELECT 1) INSERT INTO target VALUES(2,3) RETURNING id)","42601"}}) {
        bool failed=false;
        try {(void)prepareQuery(text,{},metadata);}catch(const DbError& error){failed=error.sqlState()==state;}
        assert(failed);
    }
    metadata.relation=[](const std::string& name) {
        if(name=="source")return QueryRelationMetadata{"public",name,{{"id","integer"},{"t","text"}}, {}};
        return QueryRelationMetadata{"public",name,{{"id","integer"},{"t","text"},{"v","interval"}}, {}};
    };
    bool starLiteralInputRejected=false;
    try{(void)prepareQuery("INSERT INTO target SELECT *, '2147483648 months' FROM source WHERE false",{},metadata);}
    catch(const DbError& error){starLiteralInputRejected=error.sqlState()=="22015";}
    if(!starLiteralInputRejected)std::cerr<<"star+direct unknown literal lost assignment provenance\n";
    assert(starLiteralInputRejected);
    std::cout<<"[WITH PRIMARY DML BINDING] passed\n";
}
