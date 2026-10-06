#include "parser/query_binding.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    size_t lookups=0;
    metadata.relation=[](const std::string&) {return QueryRelationMetadata{"public","unary_rows",{{"i","integer"},{"m","money"},{"iv","interval"},{"tm","time"}}, ""};};
    metadata.functionType=[&](const FunctionCallExpr* call) {++lookups;return call->funcName=="money_writer"?"money":"integer";};
    const auto state=[&](const std::string& sql,const std::vector<QueryBindingDatum>& datums={}) {
        try { (void)prepareQuery(sql,datums,metadata);return std::string(); }
        catch(const DbError& error) {return error.sqlState();}
    };
    for(const auto& expression:std::vector<std::string>{
        "-(CAST('-92233720368547758.08' AS MONEY))","+CAST(1 AS MONEY)","-CAST(NULL AS MONEY)",
        "+CAST(NULL AS TEXT)","-CAST(NULL AS BOOLEAN)","-POINT '(1,2)'",
        "+INTERVAL '1 day'","+CAST(1 AS OID)","-ARRAY[1,2]"}) {
        const auto actual=state("SELECT "+expression+" WHERE false");
        if(actual!="42883")std::cerr<<expression<<" actual="<<actual<<" expected=42883\n";
        assert(actual=="42883");
        assert(state("VALUES("+expression+")")=="42883");
        assert(state("SELECT CASE true WHEN true THEN 1 ELSE "+expression+" END")=="42883");
    }
    assert(state("SELECT -m FROM unary_rows WHERE false")=="42883");
    assert(state("WITH r AS (SELECT m FROM unary_rows) SELECT -m FROM r WHERE false")=="42883");
    assert(state("SELECT -m",{{"money-parameter","m","money",{},true,std::nullopt,1}})=="42883");
    assert(state("SELECT -$1",{{"money-parameter","m","money",{},true,std::nullopt,1}})=="42883");
    assert(state("SELECT -money_writer() WHERE false")=="42883" && lookups==1);
    assert(state("SELECT -'1'")=="42725");assert(state("SELECT -NULL")=="42725");
    assert(state("SELECT +'bad'")=="22P02");
    for(const auto& type:std::vector<std::string>{"smallint","integer","bigint","real","double precision","numeric"}) {
        for(const auto& op:std::vector<std::string>{"+","-"}) {
            const auto query=prepareQuery("SELECT "+op+"CAST(NULL AS "+type+")",{},metadata);
            assert(query.output.at(0).type==type);
        }
    }
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"+'1'","double precision"},{"+NULL","double precision"},
        {"-INTERVAL '1 month 2 days'","interval"},{"-TIME '01:02:03'","interval"},
        {"-2147483648","integer"},{"-9223372036854775808","bigint"},
        {"-(-9223372036854775808)","numeric"}}) {
        const auto query=prepareQuery("SELECT "+item.first,{},metadata);
        assert(query.output.at(0).type==item.second);
    }
    std::cout<<"builtin unary binding uses actual signatures and pure declared types\n";
}
