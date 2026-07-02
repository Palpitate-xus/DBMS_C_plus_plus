#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

namespace fs = std::filesystem;

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

static void test_migration_of_existing_db() {
    std::string db = testDbPath("catalog_service_t2");
    cleanup(db);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    // Create a table using the legacy StorageEngine path (produces a .stc file).
    dbms::TableSchema tbl;
    tbl.tablename = "legacy_tbl";
    dbms::Column col;
    col.dataName = "id";
    col.dataType = "integer";
    col.dsize = 4;
    tbl.append(col);
    assert(engine.createTable(db, tbl) == dbms::DBStatus::OK);

    // Now access the catalog for the first time: it should migrate the .stc table.
    dbms::CatalogManager& cat = engine.catalogService().get(db);
    const auto* nsPublic = cat.findNamespaceByName("public");
    assert(nsPublic != nullptr);
    const auto* cls = cat.findClassByName("legacy_tbl", nsPublic->oid);
    assert(cls != nullptr);
    assert(cls->relnatts == 1);

    // Marker should exist now; re-get should not re-migrate.
    assert(fs::exists(fs::path(db) / "pg_catalog" / ".migrated"));

    cleanup(db);
    std::cout << "[CATALOG-SVC] migration of existing DB OK" << std::endl;
}

static void test_checkpoint_persists_catalog() {
    std::string db = testDbPath("catalog_service_t3");
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

    // Evict and reload from disk.
    engine.catalogService().evict(db);
    dbms::CatalogManager& cat2 = engine.catalogService().get(db);

    const auto* cls2 = cat2.findClassByName("checkpoint_tbl", nsPublic->oid);
    assert(cls2 != nullptr);
    assert(cls2->relnatts == 3);

    cleanup(db);
    std::cout << "[CATALOG-SVC] checkpoint persists catalog OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_bootstrap_and_cache();
    test_migration_of_existing_db();
    test_checkpoint_persists_catalog();
    std::cout << "[CATALOG-SVC] all passed" << std::endl;
    return 0;
}
