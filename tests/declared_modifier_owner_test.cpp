#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    size_t checks=0,failures=0;
    const auto require=[&](bool pass,const std::string& role){++checks;failures+=!pass;std::cout<<"DECLARED_MODIFIER_OWNER "<<role<<" pass="<<pass<<'\n';};
    const auto db=testDbPath("declared_modifier_owner");
    require(g_engine.createDatabase(db)==DBStatus::OK,"create isolated database");
    const std::vector<std::string> invalid={"regtype(3)","pg_catalog.regtype(3)","regproc(3)","regclass(3)","regrole(3)","regnamespace(3)",
        "regtype(3)[]","pg_catalog._regtype(3)","pg_catalog._regclass(3)","pg_catalog._regnamespace(3)","pg_catalog._regrole(3)",
        "name(3)","pg_catalog.\"char\"(3)","integer(3)[]"};
    for(bool warm:{false,true}) {
        if(warm)(void)g_engine.catalogService().get(db);
        for(const auto& type:invalid) {
            bool pass=false;try{(void)g_engine.prepareBoundQuery(db,"SELECT CAST(NULL AS "+type+") WHERE false");}
            catch(const DbError& error){pass=error.sqlState()=="42601";}
            require(pass,std::string(warm?"warm ":"cold ")+type+" rejects before NULL/empty demand");
        }
        if(!warm)require(!g_engine.catalogService().has(db) && !std::filesystem::exists(g_engine.dbPath(db)/"pg_catalog"),"cold rejection has no cache/disk allocation");
    }
    require(g_engine.catalogService().persistAll(),"persist actual metadata");
    const auto disk=CatalogManager::readMetadataSnapshot((g_engine.dbPath(db)/"pg_catalog").string());
    for(const auto& type:invalid) {
        bool pass=false;try{(void)resolveDeclaredTypeName(type,&disk,nullptr);}catch(const DbError& error){pass=error.sqlState()=="42601";}
        require(pass,"direct persisted owner rejects "+type);
    }
    for(const auto& type:{"varchar(3)[]","numeric(5,2)[]","regtype[]","regtype","regclass"})
        require(g_engine.prepareBoundQuery(db,std::string("SELECT CAST(NULL AS ")+type+") WHERE false").output.at(0).typeOid!=0,std::string("valid genuine declaration ")+type);
    std::cout<<"DECLARED_MODIFIER_OWNER_CHECKED="<<checks<<" FAILED="<<failures<<'\n';
    return checks==50 && !failures?0:1;
}
