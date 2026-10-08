#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("declared_type_cold_binding");
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    auto* previous=currentSession();Session session;session.currentDB=database;session.username="testuser";
    setCurrentSession(&session);
    struct Restore {Session* prior;~Restore(){setCurrentSession(prior);}} restore{previous};
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;
        std::cout<<"DECLARED_TYPE_COLD "<<role<<" pass="<<pass<<'\n';
    };
    const auto before=g_engine.catalogService().metadataSnapshot(database);
    require(before.types.empty() && before.namespaces.empty() && !g_engine.catalogService().has(database),"actual uninitialized catalog");
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"VARBIT 'b01'","bit varying"},{"pg_catalog.varbit 'b01'","bit varying"},
        {"CAST('b01' AS varbit)","bit varying"},{"NULL::varbit","bit varying"},
        {"0::bigint","bigint"},{"CAST(NULL AS integer)","integer"},
        {"BIT 'b01'","bit"},{"TEXT 'x'","text"}}) {
        bool pass=false;
        try {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT "+item.first+" AS value WHERE false");
            const auto oid=mapBuiltinTypeNameToOid(item.second);
            pass=query.output.size()==1 && oid && query.output[0].type==item.second && query.output[0].typeOid==oid;
        } catch(const DbError& error){std::cerr<<error.sqlState()<<' '<<error.what()<<'\n';}
        require(pass,item.first);
    }
    for(const auto& sql:{"SELECT not_a_type 'x'", "SELECT NULL::not_a_type"}) {
        bool pass=false;
        try {(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){pass=error.sqlState()=="42704";}
        require(pass,sql);
    }
    const auto after=g_engine.catalogService().metadataSnapshot(database);
    require(after.types.empty() && after.namespaces.empty() && !g_engine.catalogService().has(database),"preparation did not initialize/write the catalog");
    std::cout<<"DECLARED_TYPE_COLD_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==12 && !failed?0:1;
}
