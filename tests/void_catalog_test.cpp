#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const auto check=[](const CatalogManager::MetadataSnapshot& snapshot) {
        const PgTypeRow* found=nullptr;
        for(const auto& type:snapshot.types)if(type.oid==2278)found=&type;
        assert(found && found->typname=="void" && found->typnamespace==11 &&
            found->typlen==4 && found->typbyval && found->typtype=='p' &&
            found->typcategory=='P' && found->typalign=='i' && found->typstorage=='p' &&
            !found->typelem && !found->typarray && !found->typcollation);
        const auto declared=resolveDeclaredTypeName("pg_catalog.void",&snapshot,nullptr);
        assert(declared.typeName=="void" && declared.typeOid==2278);
    };
    const auto database=testDbPath("void_catalog");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    CatalogManager::MetadataSnapshot cold;
    cold.types=CatalogManager::builtinTypeRows();
    cold.namespaces=CatalogManager::builtinNamespaceRows();
    check(cold);
    assert(!g_engine.catalogService().has(database) &&
        !std::filesystem::exists(g_engine.dbPath(database)/"pg_catalog"));
    const auto query=g_engine.prepareBoundQuery(database,"SELECT pg_notify('cold_void','payload')");
    assert(query.output.size()==1 && query.output[0].type=="void" && query.output[0].typeOid==2278);
    assert(!g_engine.catalogService().has(database) &&
        !std::filesystem::exists(g_engine.dbPath(database)/"pg_catalog"));
    (void)g_engine.catalogService().get(database);
    check(g_engine.catalogService().metadataSnapshot(database));
    assert(g_engine.catalogService().persistAll());
    check(CatalogManager::readMetadataSnapshot((g_engine.dbPath(database)/"pg_catalog").string()));
    std::cout<<"[VOID CATALOG] exact cold/warm/disk metadata and pure routine output passed\n";
}
