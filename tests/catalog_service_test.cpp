#include "test_utils.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void test_bootstrap_and_cache() {
    std::string db = testDbPath("catalog_service_t1");
    cleanup(db);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::CatalogService& svc = engine.catalogService();
    dbms::CatalogManager& cat = svc.get(db);

    const auto* nsPublic = cat.findNamespaceByName("public");
    const auto* nsCat = cat.findNamespaceByName("pg_catalog");
    assert(nsPublic != nullptr);
    assert(nsCat != nullptr);

    dbms::CatalogManager& cat2 = svc.get(db);
    assert(&cat == &cat2); // cache hit returns same instance

    cleanup(db);
    std::cout << "[CATALOG-SVC] bootstrap and cache OK" << std::endl;
}

static void test_storage_only_metadata_is_not_imported() {
    std::string db = testDbPath("catalog_service_t2");
    cleanup(db);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    // The low-level storage API intentionally does not create catalog rows.
    // Catalog registration belongs to the current DDL executor; old .stc
    // metadata is not imported implicitly on first catalog access.
    dbms::TableSchema tbl;
    tbl.tablename = "storage_only_tbl";
    dbms::Column col;
    col.dataName = "id";
    col.dataType = "integer";
    col.dsize = 4;
    tbl.append(col);
    assert(engine.createTable(db, tbl) == dbms::DBStatus::OK);

    dbms::CatalogManager& cat = engine.catalogService().get(db);
    const auto* nsPublic = cat.findNamespaceByName("public");
    assert(nsPublic != nullptr);
    assert(cat.findClassByName("storage_only_tbl", nsPublic->oid) == nullptr);
    assert(!std::filesystem::exists(std::filesystem::path(db) /
                                    "pg_catalog" / ".migrated"));

    cleanup(db);
    std::cout << "[CATALOG-SVC] storage-only metadata stays outside catalog OK" << std::endl;
}

static void test_checkpoint_persists_catalog() {
    std::string db = testDbPath("catalog_service_t2");
    cleanup(db);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::CatalogManager& cat = engine.catalogService().get(db);
    const auto* nsPublic = cat.findNamespaceByName("public");
    assert(nsPublic != nullptr);

    dbms::PgClassRow cls;
    cls.relname = "checkpoint_tbl";
    cls.relnamespace = nsPublic->oid;
    cls.relkind = 'r';
    cls.relnatts = 3;
    cat.createClass(cls);

    // Checkpoint should persist the catalog (among other things).
    engine.checkpoint(db);

    // Evict and reload from disk — the namespace system table should survive.
    engine.catalogService().evict(db);
    dbms::CatalogManager& cat2 = engine.catalogService().get(db);

    // After reload, the public namespace must be found.
    const auto* ns2 = cat2.findNamespaceByName("public");
    assert(ns2 != nullptr);
    // The class we created should be visible (checkpoint persists catalog).
    const auto* cls2 = cat2.findClassByName("checkpoint_tbl", ns2->oid);
    assert(cls2 != nullptr);
    assert(cls2->relnatts == 3);

    cleanup(db);
    std::cout << "[CATALOG-SVC] checkpoint persists catalog OK" << std::endl;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    test_bootstrap_and_cache();
    test_storage_only_metadata_is_not_imported();
    test_checkpoint_persists_catalog();
    std::cout << "[CATALOG-SVC] all passed" << std::endl;
    return 0;
}
