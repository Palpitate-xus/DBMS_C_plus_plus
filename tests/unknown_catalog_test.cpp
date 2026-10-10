#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>

int main() {
    using namespace dbms;
    const auto check=[](const CatalogManager::MetadataSnapshot& metadata) {
        const auto found=std::find_if(metadata.types.begin(),metadata.types.end(),
            [](const PgTypeRow& row){return row.oid==705;});
        assert(found!=metadata.types.end());
        const auto& row=*found;
        assert(row.typname=="unknown" && row.typnamespace==11 && row.typowner==10);
        assert(row.typlen==-2 && !row.typbyval && row.typtype=='p' && row.typcategory=='X');
        assert(row.typalign=='c' && row.typstorage=='p' && !row.typelem && !row.typarray);
        assert(row.typinput==109 && row.typoutput==110 && row.typreceive==2416 && row.typsend==2417);
        assert(!row.typispreferred && row.typisdefined && !row.typcollation && !row.typbasetype && row.typtypmod==-1);
        assert(resolveDeclaredTypeName("pg_catalog.unknown",&metadata,nullptr).typeOid==705);
    };
    StorageEngine local;
    const auto database=testDbPath("unknown_catalog");
    assert(local.createDatabase(database)==DBStatus::OK);
    CatalogManager::MetadataSnapshot cold;
    cold.types=CatalogManager::builtinTypeRows();cold.namespaces=CatalogManager::builtinNamespaceRows();
    check(cold);
    assert(!local.catalogService().has(database) && !std::filesystem::exists(local.dbPath(database)/"pg_catalog"));
    (void)local.catalogService().get(database);
    check(local.catalogService().metadataSnapshot(database));
    assert(local.catalogService().persistAll());
    check(CatalogManager::readMetadataSnapshot((local.dbPath(database)/"pg_catalog").string()));
    StorageEngine other;
    check(other.catalogService().metadataSnapshot(database));
    assert(resolveDeclaredTypeName("unknown",nullptr,nullptr).typeOid==705);
    assert(local.dropDatabase(database)==DBStatus::OK);
    std::cout<<"[UNKNOWN CATALOG] verified PG18 physical row, pure cold lookup, warm, disk and fresh owner passed\n";
}
