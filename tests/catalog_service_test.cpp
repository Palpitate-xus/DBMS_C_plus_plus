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
    const dbms::Oid classOid = cat.createClass(cls);

    dbms::PgAttributeRow attribute;
    attribute.attrelid = classOid;
    attribute.attname = "complete_column";
    attribute.atttypid = 23;
    attribute.attstattarget = 321;
    attribute.attlen = 8;
    attribute.attnum = 1;
    attribute.attndims = 2;
    attribute.attcacheoff = 64;
    attribute.atttypmod = 42;
    attribute.attbyval = true;
    attribute.attstorage = 'm';
    attribute.attalign = 'd';
    attribute.attnotnull = true;
    attribute.atthasdef = true;
    attribute.attidentity = 'a';
    attribute.attgenerated = 'v';
    attribute.attisdropped = true;
    attribute.attislocal = false;
    attribute.attinhcount = 3;
    attribute.attcollation = 50009;
    cat.addAttribute(attribute);

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
    const auto* attribute2 =
        cat2.findAttribute(classOid, "complete_column");
    assert(attribute2 != nullptr);
    assert(attribute2->atttypid == 23);
    assert(attribute2->attstattarget == 321);
    assert(attribute2->attlen == 8);
    assert(attribute2->attnum == 1);
    assert(attribute2->attndims == 2);
    assert(attribute2->attcacheoff == 64);
    assert(attribute2->atttypmod == 42);
    assert(attribute2->attbyval);
    assert(attribute2->attstorage == 'm');
    assert(attribute2->attalign == 'd');
    assert(attribute2->attnotnull);
    assert(attribute2->atthasdef);
    assert(attribute2->attidentity == 'a');
    assert(attribute2->attgenerated == 'v');
    assert(attribute2->attisdropped);
    assert(!attribute2->attislocal);
    assert(attribute2->attinhcount == 3);
    assert(attribute2->attcollation == 50009);

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
        std::ofstream out(
            std::filesystem::path(path) / "pg_attribute.cat",
            std::ios::trunc);
        assert(out);
        out << "71001,\"legacy_column\",23,1,4,-1,t,f,p,i\n";
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
        const auto* attribute =
            catalog.findAttribute(71001, "legacy_column");
        assert(attribute != nullptr);
        assert(attribute->attnum == 1);
        assert(attribute->attlen == 4);
        assert(attribute->attnotnull);
        assert(attribute->attstattarget == -1);
        assert(attribute->attndims == 0);
        assert(attribute->attcacheoff == -1);
        assert(!attribute->attbyval);
        assert(attribute->attidentity == '\0');
        assert(attribute->attgenerated == '\0');
        assert(!attribute->attisdropped);
        assert(attribute->attislocal);
        assert(attribute->attinhcount == 0);
        assert(attribute->attcollation == dbms::INVALID_OID);
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
