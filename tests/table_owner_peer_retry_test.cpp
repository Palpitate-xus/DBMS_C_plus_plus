#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("table_owner_peer_retry");
    auto& roles = g_engine.catalogService().get("info");
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
    auto& catalog = g_engine.catalogService().get(database);
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
        assert(catalog.getDescription(relationOid, 1259, 0) == "ordinary peer metadata");
        assert(catalog.getClass(relationOid)->relowner == newOwner);
        assert(g_engine.getTableSchema(database, "rows").owner == newRole.rolname);
        // A real unsaved local conflict must not be merged or silently lost,
        // and must fail before even the schema owner is published.
        assert(peer.persistAll());
        catalog.setDescription(relationOid, 1259, 0, "local unsaved metadata");
        peer.setDescription(relationOid, 1259, 0, "new peer metadata");
        assert(peer.persistAll());
        assert(g_engine.alterTableOwner(database, "rows", oldRole.rolname) == DBStatus::IO_ERROR);
        assert(catalog.getDescription(relationOid, 1259, 0) == "local unsaved metadata");
        assert(catalog.getClass(relationOid)->relowner == newOwner);
        assert(g_engine.getTableSchema(database, "rows").owner == newRole.rolname);
        const auto disk = CatalogManager::readMetadataSnapshot((g_engine.dbPath(database) / "pg_catalog").string());
        assert(disk.relations.size() == 1 && disk.relations[0].relowner == newOwner);
        CatalogManager verifier((g_engine.dbPath(database) / "pg_catalog").string());
        assert(verifier.getDescription(relationOid, 1259, 0) == "new peer metadata");
        catalog.loadAll();
        assert(g_engine.alterTableOwner(database, "rows", oldRole.rolname) == DBStatus::OK);
        assert(catalog.getClass(relationOid)->relowner == oldOwner);
        assert(catalog.getDescription(relationOid, 1259, 0) == "new peer metadata");
    }
    g_engine.catalogService().evict(database);
    assert(g_engine.catalogService().get(database).getClass(relationOid)->relowner == oldOwner);
    assert(g_engine.getTableSchema(database, "rows").owner == oldRole.rolname);
    g_engine.catalogService().evict(database);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    assert(roles.dropAuthId(oldOwner) && roles.dropAuthId(newOwner) && roles.persistAll());
    std::cout << "[TABLE OWNER PEER RETRY] clean refresh, real conflict, durable healthy retry OK\n";
}
