#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="text_search_catalog_identity",database=testDbPath(name);
    cleanupTestDb(name);assert(g_engine.createDatabase(database)==DBStatus::OK);
    struct Type {const char* name;Oid oid,array;char storage;};
    const Type types[]={{"tsvector",3614,3643,'x'},{"tsquery",3615,3645,'p'}};
    size_t checked=0,failures=0;
    const auto require=[&](bool valid){++checked;failures+=!valid;};
    const auto metadata=[&](const auto& rows) {
        for(const auto& type:types)for(bool array:{false,true}) {
            const auto oid=array?type.array:type.oid;
            const auto found=std::find_if(rows.begin(),rows.end(),[&](const auto& row){return row.oid==oid;});
            require(found!=rows.end() && found->typname==(array?"_":"")+std::string(type.name) &&
                found->typnamespace==11 && found->typlen==-1 && !found->typbyval && found->typtype=='b' &&
                found->typcategory==(array?'A':'U') && found->typelem==(array?type.oid:0) &&
                found->typarray==(array?0:type.array) && found->typalign=='i' && found->typstorage==(array?'x':type.storage));
        }
    };
    metadata(CatalogManager::builtinTypeRows());
    for(const auto& type:types)for(bool array:{false,true}) {
        const std::string spelling=std::string(type.name)+(array?"[]":"");
        require(mapBuiltinTypeNameToOid(spelling)==(array?type.array:type.oid));
    }
    const auto bind=[&](bool standalone) {
        for(const auto& type:types)for(bool array:{false,true})for(const auto& prefix:{"","pg_catalog.","\"pg_catalog\"."}) {
            const std::string spelling=std::string(prefix)+type.name+(array?"[]":"");
            bool valid=false;
            try {
                if(standalone)valid=resolveDeclaredTypeName(spelling,nullptr,nullptr).typeOid==(array?type.array:type.oid);
                else {
                    const auto query=g_engine.prepareBoundQuery(database,"SELECT CAST(NULL AS "+spelling+") LIMIT 0");
                    valid=query.output.size()==1 && query.output.front().typeOid==(array?type.array:type.oid);
                }
            }catch(const DbError&){}
            require(valid);
        }
    };
    bind(true);bind(false);
    require(!g_engine.catalogService().has(database) && !std::filesystem::exists(g_engine.dbPath(database)/"pg_catalog"));
    (void)g_engine.catalogService().get(database);
    metadata(g_engine.catalogService().metadataSnapshot(database).types);bind(false);
    require(g_engine.catalogService().persistAll());g_engine.catalogService().evict(database);
    metadata(CatalogManager::readMetadataSnapshot((g_engine.dbPath(database)/"pg_catalog").string()).types);bind(false);
    require(g_engine.dropDatabase(database)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"TEXT_SEARCH_CATALOG_IDENTITY_COMPLETE "<<checked<<" FAILURES "<<failures<<'\n';
    return checked==67 && !failures?0:1;
}
