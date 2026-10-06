#include "parser/query_binding.h"
#include "catalog/catalog.h"
#include "common/DbError.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    QueryBindingMetadata metadata;
    size_t metadataCalls=0;
    metadata.relation=[&](const std::string& spelling) {
        ++metadataCalls;
        CatalogManager::QualifiedName name;
        assert(CatalogManager::parseQualifiedName(spelling,name,true));
        return QueryRelationMetadata{name.schema.empty()?"public":name.schema,name.name,{{"id","integer"},{"v","integer"}}, {}};
    };
    const auto failure=[&](const std::string& sql,const std::string& state) {
        bool rejected=false;
        try{(void)prepareQuery(sql,{},metadata);}catch(const DbError& error){
            std::cerr<<sql<<" actual="<<error.sqlState()<<" expected="<<state<<'\n';rejected=error.sqlState()==state;
        }
        assert(rejected);
    };
    for(const auto& sql:{
        "UPDATE r SET v=1 FROM r WHERE false",
        "UPDATE r t SET v=1 FROM s t WHERE false",
        "UPDATE r AS \"T\" SET v=1 FROM s AS \"T\" WHERE false",
        "DELETE FROM r USING r WHERE false",
        "DELETE FROM r t USING s t WHERE false",
        "WITH r AS(SELECT 1 AS id) UPDATE r SET v=1 FROM r WHERE false",
        "WITH r AS(SELECT 1 AS id) DELETE FROM r USING r WHERE false",
        "SELECT 1 FROM r t JOIN s t ON true"})failure(sql,"42712");
    failure("UPDATE r t SET v=r.v FROM s u WHERE false","42P01");
    failure("DELETE FROM r t USING s u WHERE r.id=u.id","42P01");
    for(const auto& sql:{
        "UPDATE r t SET v=u.v FROM r u WHERE t.id=u.id",
        "DELETE FROM r t USING r u WHERE t.id=u.id",
        "UPDATE r AS \"T\" SET v=\"t\".v FROM s AS \"t\" WHERE \"T\".id=\"t\".id",
        "UPDATE one.r SET v=two.r.v FROM two.r WHERE one.r.id=two.r.id",
        "DELETE FROM one.r USING two.r WHERE one.r.id=two.r.id",
        "SELECT one.r.id,two.r.id FROM one.r JOIN two.r ON one.r.id=two.r.id"}) {
        const auto query=prepareQuery(sql,{},metadata);
        assert(query.sourceRanges.size()==2 && query.sourceRanges[0].ordinal!=query.sourceRanges[1].ordinal);
    }
    failure("SELECT id FROM one.r JOIN two.r ON true","42702");
    assert(metadataCalls>0);
    std::cout<<"[QUERY RANGE OCCURRENCES] canonical qualifiers/aliases and physical identities passed\n";
}
