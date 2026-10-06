#include "parser/query_binding.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    metadata.relation=[](const std::string& name) {
        return QueryRelationMetadata{"public",name,{{"e","integer"},{"E","integer"}}, {}};
    };
    metadata.functionType=[](const FunctionCallExpr*) { return std::string("integer"); };
    size_t failures=0;
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"SELECT CASE true WHEN true THEN 1 ELSE t.e END FROM t","e"},
        {"SELECT CASE true WHEN true THEN 1 ELSE t.\"E\" END FROM t","E"},
        {"SELECT CASE WHEN true THEN 1 ELSE abs(2) END","abs"},
        {"SELECT CAST(CASE WHEN true THEN 1 ELSE abs(2) END AS BIGINT)","abs"},
        {"SELECT CASE true WHEN true THEN 1 ELSE CAST(t.e AS BIGINT) END FROM t","e"},
        {"SELECT CASE true WHEN true THEN 1 ELSE CAST(2 AS BIGINT) END","case"},
        {"SELECT CASE true WHEN true THEN 1 ELSE (CASE WHEN false THEN 1 ELSE t.e END) END FROM t","e"},
        {"SELECT CASE true WHEN true THEN 1 ELSE t.e END AS \"Explicit\" FROM t","Explicit"}}) {
        const auto prepared=prepareQuery(item.first,{},metadata);
        if(prepared.output.at(0).name!=item.second) {
            ++failures;std::cerr<<"CASE_ELSE_LABEL_FAILURE "<<item.first<<" actual="<<prepared.output.at(0).name<<" expected="<<item.second<<'\n';
        }
    }
    std::cerr<<"CASE ELSE label failures="<<failures<<'\n';assert(!failures);
}
