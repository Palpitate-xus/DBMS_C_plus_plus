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
    struct Identity {Oid scalar,array;std::string name;};
    const std::vector<Identity> identities={{2205,2210,"regclass"},{4089,4090,"regnamespace"},{4096,4097,"regrole"}};
    size_t checks=0,failures=0;
    const auto require=[&](bool pass,const std::string& role){++checks;failures+=!pass;std::cout<<"REGTYPE_ALIAS_CATALOG "<<role<<" pass="<<pass<<'\n';};
    const auto check=[&](const CatalogManager::MetadataSnapshot& metadata,const std::string& mode) {
        for(const auto& identity:identities) {
            const PgTypeRow* scalar=nullptr;const PgTypeRow* array=nullptr;
            for(const auto& row:metadata.types){if(row.oid==identity.scalar)scalar=&row;if(row.oid==identity.array)array=&row;}
            require(scalar && scalar->typname==identity.name && scalar->typnamespace==11 && scalar->typlen==4 &&
                scalar->typbyval && scalar->typtype=='b' && scalar->typcategory=='N' && scalar->typarray==identity.array &&
                scalar->typalign=='i' && scalar->typstorage=='p',mode+" actual scalar "+identity.name);
            require(array && array->typname=="_"+identity.name && array->typnamespace==11 && array->typlen==-1 &&
                !array->typbyval && array->typcategory=='A' && array->typelem==identity.scalar &&
                array->typalign=='i' && array->typstorage=='x',mode+" actual array "+identity.name);
            require(resolveDeclaredTypeName(identity.name,&metadata,nullptr).typeOid==identity.scalar,mode+" scalar resolution");
            require(resolveDeclaredTypeName(identity.name+"[]",&metadata,nullptr).typeOid==identity.array,mode+" array resolution");
        }
    };
    const auto db=testDbPath("regtype_alias_catalog");
    require(g_engine.createDatabase(db)==DBStatus::OK,"create isolated database");
    CatalogManager::MetadataSnapshot cold;cold.types=CatalogManager::builtinTypeRows();cold.namespaces=CatalogManager::builtinNamespaceRows();
    check(cold,"cold");
    require(!g_engine.catalogService().has(db) && !std::filesystem::exists(g_engine.dbPath(db)/"pg_catalog"),"read-only cold identities");
    (void)g_engine.catalogService().get(db);check(g_engine.catalogService().metadataSnapshot(db),"warm");
    require(g_engine.catalogService().persistAll(),"persist actual aliases");
    check(CatalogManager::readMetadataSnapshot((g_engine.dbPath(db)/"pg_catalog").string()),"direct disk");
    bool maps=true;for(const auto& identity:identities)maps&=mapBuiltinTypeNameToOid(identity.name)==identity.scalar && mapBuiltinTypeNameToOid(identity.name+"[]")==identity.array;
    require(maps,"actual wire identity maps");
    std::cout<<"REGTYPE_ALIAS_CATALOG_CHECKED="<<checks<<" FAILED="<<failures<<'\n';
    return checks==40 && !failures?0:1;
}
