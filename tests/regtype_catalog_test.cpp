#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "catalog/systables.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    size_t checked=0,failures=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failures+=!pass;std::cout<<"REGTYPE_CATALOG "<<role<<" pass="<<pass<<'\n';};
    const auto check=[&](const CatalogManager::MetadataSnapshot& metadata,const std::string& mode) {
        const PgTypeRow* scalar=nullptr;const PgTypeRow* array=nullptr;
        for(const auto& row:metadata.types){if(row.oid==2206)scalar=&row;if(row.oid==2211)array=&row;}
        require(scalar && scalar->typname=="regtype" && scalar->typnamespace==11 &&
            scalar->typlen==4 && scalar->typbyval && scalar->typalign=='i' && scalar->typstorage=='p' &&
            scalar->typtype=='b' && scalar->typcategory=='N' && scalar->typarray==2211,mode+" actual scalar row");
        require(array && array->typname=="_regtype" && array->typnamespace==11 && array->typlen==-1 &&
            array->typcategory=='A' && array->typelem==2206 && array->typalign=='i' && array->typstorage=='x',mode+" array/backlink");
        require(resolveDeclaredTypeName("pg_catalog.regtype",&metadata,nullptr).typeOid==2206,mode+" scalar resolution");
        require(resolveDeclaredTypeName("pg_catalog.regtype[]",&metadata,nullptr).typeOid==2211,mode+" array resolution");
    };
    const auto database=testDbPath("regtype_catalog");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    CatalogManager::MetadataSnapshot cold;cold.types=CatalogManager::builtinTypeRows();cold.namespaces=CatalogManager::builtinNamespaceRows();
    check(cold,"cold");
    require(!g_engine.catalogService().has(database) && !std::filesystem::exists(g_engine.dbPath(database)/"pg_catalog"),"cold producer performs no cache/disk writes");
    auto& catalog=g_engine.catalogService().get(database);
    check(g_engine.catalogService().metadataSnapshot(database),"warm");
    require(g_engine.catalogService().persistAll(),"persist actual rows");
    check(CatalogManager::readMetadataSnapshot((g_engine.dbPath(database)/"pg_catalog").string()),"direct disk");
    require(mapBuiltinTypeNameToOid("regtype")==2206 && mapBuiltinTypeNameToOid("regtype[]")==2211,"physical OID mapping");
    require(catalog.dropNamespace(2200),"drop actual public namespace");
    catalog.bootstrapSystemNamespaces();
    const auto dropped=g_engine.catalogService().metadataSnapshot(database);
    bool hasPublic=false;for(const auto& space:dropped.namespaces)hasPublic|=space.nspname=="public";
    require(!hasPublic,"bootstrap does not resurrect dropped public");
    std::cout<<"REGTYPE_CATALOG_CHECKED="<<checked<<" FAILED="<<failures<<'\n';
    return checked==18 && !failures?0:1;
}
