#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"COMPARISON_CATALOG_IDENTITY "<<role<<" pass="<<pass<<'\n';};
    const auto database=testDbPath("comparison_catalog_identity");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    (void)g_engine.catalogService().get(database);
    const auto metadata=g_engine.catalogService().metadataSnapshot(database);
    Oid booleanOid=INVALID_OID;
    for(const auto& type:metadata.types)if(type.typnamespace==11 && type.typname=="bool")booleanOid=type.oid;
    require(booleanOid==16,"actual copied pg_catalog boolean identity");
    for(const auto& expression:{"1 = 1","1 <> 2","1 < 2","1 > 2","1 <= 2","1 >= 2",
        "TRUE AND FALSE","NULL::boolean OR true"})for(const auto& suffix:{""," WHERE false"}) {
        const auto query=g_engine.prepareBoundQuery(database,std::string("SELECT ")+expression+" AS value"+suffix);
        require(query.output.size()==1 && query.output[0].type=="boolean" && query.output[0].typeOid==booleanOid,
            std::string(expression)+suffix);
    }
    std::cout<<"COMPARISON_CATALOG_IDENTITY_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==18 && !failed?0:1;
}
