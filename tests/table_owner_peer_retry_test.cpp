#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include "catalog_generation_test_view.h"
#include "permissions.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("table_owner_peer_retry");
    auto& roles = authCatalog();
    PgAuthIdRow oldRole;
    oldRole.rolname = "owner_peer_old";
    PgAuthIdRow newRole;
    newRole.rolname = "owner_peer_new";
    const Oid oldOwner = roles.createAuthId(oldRole);
    const Oid newOwner = roles.createAuthId(newRole);
    assert(oldOwner && newOwner && roles.persistAll());

    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema schema;
    schema.tablename = "rows";
    schema.owner = oldRole.rolname;
    schema.append(makeIntColumn("id", true, 2));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    dbms_test::CatalogGenerationView currentCatalog(g_engine,database);
    auto& catalog = currentCatalog();
    const auto nameSpace = catalog.getNamespaceByName("public");
    assert(nameSpace);
    PgClassRow relation;
    relation.relname = schema.tablename;
    relation.relnamespace = nameSpace->oid;
    relation.relowner = oldOwner;
    relation.relnatts = 1;
    const Oid relationOid = catalog.createClass(relation);
    assert(relationOid && catalog.persistAll());

    {
        CatalogManager peer((g_engine.dbPath(database) / "pg_catalog").string());
        peer.setDescription(relationOid, 1259, 0, "ordinary peer metadata");
        assert(peer.persistAll());
        assert(g_engine.alterTableOwner(database, "rows", newRole.rolname) == DBStatus::OK);
        assert(currentCatalog().getDescription(relationOid, 1259, 0) == "ordinary peer metadata");
        assert(currentCatalog().getClass(relationOid)->relowner == newOwner);
        assert(g_engine.getTableSchema(database, "rows").owner == newRole.rolname);
        // A real unsaved local conflict must not be merged or silently lost,
        // and must fail before even the schema owner is published.
        assert(peer.persistAll());
        const auto conflictModel=g_engine.catalogService().acquire(database);
        conflictModel->setDescription(relationOid, 1259, 0, "local unsaved metadata");
        const auto schemaPath=g_engine.dbPath(database)/"rows.stc";
        const auto schemaBytes=[&] {
            std::ifstream input(schemaPath,std::ios::binary);assert(input);
            return std::string(std::istreambuf_iterator<char>(input),std::istreambuf_iterator<char>());
        };
        const auto originalSchema=schemaBytes();
        peer.setDescription(relationOid, 1259, 0, "new peer metadata");
        assert(peer.persistAll());
        assert(g_engine.alterTableOwner(database, "rows", oldRole.rolname) == DBStatus::IO_ERROR);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
        assert(conflictModel->getDescription(relationOid, 1259, 0) == "local unsaved metadata");
        assert(conflictModel->getClass(relationOid)->relowner == newOwner);
        assert(schemaBytes()==originalSchema);
        std::string conflictState;
        try{(void)g_engine.getTableSchema(database,"rows");}
        catch(const DbError& error){conflictState=error.sqlState();}
        assert(conflictState=="58030");
        const auto disk = CatalogManager::readMetadataSnapshot((g_engine.dbPath(database) / "pg_catalog").string());
        assert(disk.relations.size() == 1 && disk.relations[0].relowner == newOwner);
        CatalogManager verifier((g_engine.dbPath(database) / "pg_catalog").string());
        assert(verifier.getDescription(relationOid, 1259, 0) == "new peer metadata");
        conflictModel->loadAll();
        assert(g_engine.getTableSchema(database, "rows").owner == newRole.rolname);
        assert(g_engine.alterTableOwner(database, "rows", oldRole.rolname) == DBStatus::OK);
        assert(currentCatalog().getClass(relationOid)->relowner == oldOwner);
        assert(currentCatalog().getDescription(relationOid, 1259, 0) == "new peer metadata");
    }
    g_engine.catalogService().evict(database);
    assert(g_engine.catalogService().get(database).getClass(relationOid)->relowner == oldOwner);
    assert(g_engine.getTableSchema(database, "rows").owner == oldRole.rolname);
    g_engine.catalogService().evict(database);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    assert(roles.dropAuthId(oldOwner) && roles.dropAuthId(newOwner) && roles.persistAll());
    std::cout << "[TABLE OWNER PEER RETRY] clean refresh, real conflict, durable healthy retry OK\n";
}
