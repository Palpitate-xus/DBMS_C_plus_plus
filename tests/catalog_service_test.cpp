#include "test_utils.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <fstream>
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
    cls.reltype = 50001;
    cls.reloftype = 50002;
    cls.relowner = 50003;
    cls.relam = 50004;
    cls.relfilenode = 50005;
    cls.reltablespace = 50006;
    cls.relpages = 17;
    cls.reltuples = 23.5f;
    cls.relallvisible = 11;
    cls.reltoastrelid = 50007;
    cls.relhasindex = true;
    cls.relisshared = true;
    cls.relpersistence = 'u';
    cls.relkind = 'm';
    cls.relnatts = 3;
    cls.relchecks = 2;
    cls.relhasrules = true;
    cls.relhastriggers = true;
    cls.relhassubclass = true;
    cls.relrowsecurity = true;
    cls.relforcerowsecurity = true;
    cls.relispopulated = false;
    cls.relreplident = 'i';
    cls.relispartition = true;
    cls.relrewrite = 50008;
    cls.relfrozenxid = 1234;
    cls.relminmxid = 5678;
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
    assert(cls2->reltype == 50001);
    assert(cls2->reloftype == 50002);
    assert(cls2->relowner == 50003);
    assert(cls2->relam == 50004);
    assert(cls2->relfilenode == 50005);
    assert(cls2->reltablespace == 50006);
    assert(cls2->relpages == 17);
    assert(cls2->reltuples == 23.5f);
    assert(cls2->relallvisible == 11);
    assert(cls2->reltoastrelid == 50007);
    assert(cls2->relhasindex);
    assert(cls2->relisshared);
    assert(cls2->relpersistence == 'u');
    assert(cls2->relkind == 'm');
    assert(cls2->relnatts == 3);
    assert(cls2->relchecks == 2);
    assert(cls2->relhasrules);
    assert(cls2->relhastriggers);
    assert(cls2->relhassubclass);
    assert(cls2->relrowsecurity);
    assert(cls2->relforcerowsecurity);
    assert(!cls2->relispopulated);
    assert(cls2->relreplident == 'i');
    assert(cls2->relispartition);
    assert(cls2->relrewrite == 50008);
    assert(cls2->relfrozenxid == 1234);
    assert(cls2->relminmxid == 5678);

    cleanup(db);
    std::cout << "[CATALOG-SVC] checkpoint persists catalog OK" << std::endl;
}

static void test_legacy_pg_class_prefix_still_loads() {
    const std::string path = testDbPath("catalog_legacy_class_prefix");
    cleanup(path);
    std::filesystem::create_directories(path);
    {
        std::ofstream out(
            std::filesystem::path(path) / "pg_class.cat",
            std::ios::trunc);
        assert(out);
        out << "71001,\"legacy_relation\",11,0,12,0,0,r,2,t,u,f\n";
    }
    {
        dbms::CatalogManager catalog(path);
        const auto* relation = catalog.findClass(71001);
        assert(relation != nullptr);
        assert(relation->relname == "legacy_relation");
        assert(relation->relnatts == 2);
        assert(relation->relhasindex);
        assert(relation->relpersistence == 'u');
        assert(!relation->relrowsecurity);
        assert(relation->relispopulated);
        assert(relation->relreplident == 'd');
    }
    cleanup(path);
    std::cout << "[CATALOG-SVC] legacy pg_class prefix loads OK"
              << std::endl;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    test_bootstrap_and_cache();
    test_storage_only_metadata_is_not_imported();
    test_checkpoint_persists_catalog();
    test_legacy_pg_class_prefix_still_loads();
    std::cout << "[CATALOG-SVC] all passed" << std::endl;
    return 0;
}
