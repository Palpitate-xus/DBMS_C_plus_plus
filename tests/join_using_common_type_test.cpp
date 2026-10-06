#include "parser/query_binding.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    QueryBindingMetadata metadata;
    std::string left="integer",right="bigint";
    metadata.relation=[&](const std::string& name){return QueryRelationMetadata{"public",name,{{"k",name=="a"?left:right}},""};};
    const auto type=[&](const std::string& sql){auto prepared=prepareQuery(sql,{},metadata);assert(prepared.output.size()==1);return prepared.output[0].type;};
    for(const auto& kind:{"JOIN","LEFT JOIN","RIGHT JOIN","FULL JOIN"})
        assert(type(std::string("SELECT k FROM a ")+kind+" b USING(k)")=="bigint");
    left="bigint";right="integer";
    assert(type("SELECT k FROM a FULL JOIN b USING(k)")=="bigint");
    left="integer";right="numeric";assert(type("SELECT k FROM a NATURAL FULL JOIN b")=="numeric");
    left="integer";right="text";
    bool exact=false;try{type("SELECT k FROM a JOIN b USING(k)");}catch(const DbError& error){exact=error.sqlState()=="42804";}assert(exact);
    std::cout<<"[JOIN USING TYPE] copied descriptors choose a shared SQL type without executing rows\n";
}
