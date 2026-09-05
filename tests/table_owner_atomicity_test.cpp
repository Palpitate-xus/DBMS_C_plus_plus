#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static dbms::Oid createRole(dbms::CatalogManager& catalog,
                            const std::string& name) {
    dbms::PgAuthIdRow role;
    role.rolname = name;
    role.rolcanlogin = true;
    const dbms::Oid oid = catalog.createAuthId(role);
    assert(oid != dbms::INVALID_OID);
    return oid;
}

static dbms::Oid relationOwner(dbms::CatalogManager& catalog,
                               const std::string& schema,
                               const std::string& relation) {
    const auto* namespaceRow = catalog.findNamespaceByName(schema);
    assert(namespaceRow != nullptr);
    const auto* classRow = catalog.findClassByName(relation, namespaceRow->oid);
    assert(classRow != nullptr);
    return classRow->relowner;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("table_owner_atomicity");
    fs::remove_all(db);

    auto& auth = g_engine.catalogService().get("info");
    const std::string oldOwnerName = "owner_atomicity_old";
    const std::string newOwnerName = "owner_atomicity_new";
    const dbms::Oid oldOwner = createRole(auth, oldOwnerName);
    const dbms::Oid newOwner = createRole(auth, newOwnerName);
    assert(auth.persistAll());

    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "tenant__items";
    table.owner = oldOwnerName;
    table.append(dbms::makeIntColumn("id", true, 2));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);

    auto& catalog = g_engine.catalogService().get(db);
    const dbms::Oid tenantNamespace =
        catalog.createNamespace("tenant", oldOwner);
    dbms::PgClassRow relation;
    relation.relname = "items";
    relation.relnamespace = tenantNamespace;
    relation.relowner = oldOwner;
    relation.relkind = 'r';
    relation.relnatts = 1;
    assert(catalog.createClass(relation) != dbms::INVALID_OID);
    assert(catalog.persistAll());

    const fs::path schemaPath = fs::path(db) / "tenant__items.stc";
    const fs::path savedSchemaPath = schemaPath.string() + ".saved";

    // A failed schema publication must not reach pg_class.
    assert(g_engine.getTableSchema(db, table.tablename).owner == oldOwnerName);
    const auto schemaTimestamp = fs::last_write_time(schemaPath);
    fs::rename(schemaPath, savedSchemaPath);
    assert(fs::create_directory(schemaPath));
    fs::last_write_time(schemaPath, schemaTimestamp);
    assert(g_engine.alterTableOwner(db, table.tablename, newOwnerName) ==
           dbms::DBStatus::IO_ERROR);
    assert(relationOwner(catalog, "tenant", "items") == oldOwner);
    fs::remove(schemaPath);
    fs::rename(savedSchemaPath, schemaPath);
    assert(g_engine.getTableSchema(db, table.tablename).owner == oldOwnerName);

    // persistAll writes pg_class before pg_attribute. Force the latter to
    // fail after the new owner could have reached disk; ALTER TABLE must put
    // both the catalog and .stc owner back to their original values.
    const fs::path attributePath =
        fs::path(db) / "pg_catalog" / "pg_attribute.cat";
    const fs::path savedAttributePath = attributePath.string() + ".saved";
    fs::rename(attributePath, savedAttributePath);
    assert(fs::create_directory(attributePath));
    assert(g_engine.alterTableOwner(db, table.tablename, newOwnerName) ==
           dbms::DBStatus::IO_ERROR);
    assert(g_engine.getTableSchema(db, table.tablename).owner == oldOwnerName);
    assert(relationOwner(catalog, "tenant", "items") == oldOwner);
    fs::remove(attributePath);
    fs::rename(savedAttributePath, attributePath);
    assert(catalog.persistAll());

    g_engine.catalogService().evict(db);
    auto& failedReload = g_engine.catalogService().get(db);
    assert(relationOwner(failedReload, "tenant", "items") == oldOwner);
    dbms::StorageEngine schemaReload;
    assert(schemaReload.getTableSchema(db, table.tablename).owner ==
           oldOwnerName);

    assert(g_engine.alterTableOwner(db, table.tablename, newOwnerName) ==
           dbms::DBStatus::OK);
    assert(g_engine.getTableSchema(db, table.tablename).owner == newOwnerName);
    assert(relationOwner(failedReload, "tenant", "items") == newOwner);

    g_engine.catalogService().evict(db);
    auto& successReload = g_engine.catalogService().get(db);
    assert(relationOwner(successReload, "tenant", "items") == newOwner);
    dbms::StorageEngine finalSchemaReload;
    assert(finalSchemaReload.getTableSchema(db, table.tablename).owner ==
           newOwnerName);

    g_engine.catalogService().evict(db);
    fs::remove_all(db);
    assert(auth.dropAuthId(oldOwner));
    assert(auth.dropAuthId(newOwner));
    assert(auth.persistAll());
    std::cout << "[TABLE OWNER] atomic schema/catalog update OK" << std::endl;
    return 0;
}
