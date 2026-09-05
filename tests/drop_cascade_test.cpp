#include "commands/DdlExecutor.h"
#include "parser/parser.h"
#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_drop_table_cascade_removes_dependents() {
    std::string db = testDbPath("drop_cascade_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    ddl.executeSql("CREATE TABLE parent (id INT PRIMARY KEY)", s);
    ddl.executeSql("CREATE TABLE child (id INT, pid INT)", s);
    assert(!ddl.executeSql("CREATE INDEX child_id_idx ON child (id)", s));

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    const auto* nsPublic = cat.findNamespaceByName("public");
    assert(nsPublic != nullptr);

    const auto* parentCls = cat.findClassByName("parent", nsPublic->oid);
    const auto* childCls = cat.findClassByName("child", nsPublic->oid);
    assert(parentCls != nullptr);
    assert(childCls != nullptr);

    // Record an artificial dependency: child -> parent.
    dbms::PgDependRow dep;
    dep.classid = dbms::PgClassOid_Class;
    dep.objid = childCls->oid;
    dep.objsubid = 0;
    dep.refclassid = dbms::PgClassOid_Class;
    dep.refobjid = parentCls->oid;
    dep.refobjsubid = 0;
    dep.deptype = 'n';
    cat.addDepend(dep);

    // RESTRICT should fail because child depends on parent.
    bool err = ddl.executeSql("DROP TABLE parent", s);
    assert(err);
    assert(g_engine.tableExists(db, "parent"));
    assert(cat.findClassByName("parent", nsPublic->oid) != nullptr);

    // CASCADE must remove dependent catalog rows and their physical relation
    // and index files before publishing the catalog deletion plan.
    err = ddl.executeSql("DROP TABLE parent CASCADE", s);
    assert(!err);
    assert(!g_engine.tableExists(db, "parent"));
    assert(!g_engine.tableExists(db, "child"));
    assert(!g_engine.getNamedIndex(db, "child", "child_id_idx"));
    assert(cat.findClassByName("parent", nsPublic->oid) == nullptr);
    assert(cat.findClassByName("child", nsPublic->oid) == nullptr);

    // Evict the catalog so the global engine destructor does not recreate
    // the database directory after cleanup().
    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] CASCADE removes dependents OK" << std::endl;
}

static void test_schema_table_cascade_resolves_index_owner() {
    const std::string db = testDbPath("drop_cascade_schema_index");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA inventory", s));
    assert(!ddl.executeSql(
        "CREATE TABLE inventory.items (id INT, sku VARCHAR(20))", s));
    assert(!ddl.executeSql(
        "CREATE INDEX items_sku_idx ON inventory.items (sku)", s));
    assert(g_engine.getNamedIndex(
        db, "inventory__items", "items_sku_idx"));

    assert(!ddl.executeSql("DROP TABLE inventory.items CASCADE", s));
    assert(!g_engine.tableExists(db, "inventory__items"));
    assert(!g_engine.getNamedIndex(
        db, "inventory__items", "items_sku_idx"));
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* inventory = catalog.findNamespaceByName("inventory");
    assert(inventory != nullptr);
    assert(catalog.findClassByName("items", inventory->oid) == nullptr);
    assert(catalog.findClassByName("items_sku_idx", inventory->oid) == nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] schema index owner resolution OK"
              << std::endl;
}

static void test_multi_table_drop_fails_before_mutation() {
    const std::string db = testDbPath("drop_multiple_preflight");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE first_table (id INT)", s));
    assert(!ddl.executeSql("CREATE TABLE second_table (id INT)", s));
    assert(ddl.executeSql(
        "DROP TABLE first_table, second_table", s));
    assert(g_engine.tableExists(db, "first_table"));
    assert(g_engine.tableExists(db, "second_table"));

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] multi-target DROP fails before mutation OK"
              << std::endl;
}

static void test_drop_removes_named_table_sidecars() {
    std::string db = testDbPath("drop_sidecars");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "reusable";
    table.append(dbms::makeIntColumn("id", false, 2));
    table.append(dbms::makeVarCharColumn("tag", false, 64));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.setStorageParams(
               db, "reusable", {{"fillfactor", "61"}}) ==
           dbms::DBStatus::OK);

    dbms::StorageEngine::RowPolicy policy;
    policy.name = "old_policy";
    policy.cmd = "SELECT";
    policy.usingExpr = "id > 0";
    assert(g_engine.createPolicy(db, "reusable", policy) ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(db, "reusable", "tag") ==
           dbms::DBStatus::OK);

    const auto paramsPath = fs::path(db) / "reusable.params";
    const auto policyPath = fs::path(db) / "reusable.rls";
    const auto bloomMetaPath = fs::path(db) / "reusable.bloomidx";
    assert(fs::is_regular_file(paramsPath));
    assert(fs::is_regular_file(policyPath));
    assert(fs::is_regular_file(bloomMetaPath));

    assert(g_engine.dropTable(db, "reusable") == dbms::DBStatus::OK);
    assert(!fs::exists(paramsPath));
    assert(!fs::exists(policyPath));
    assert(!fs::exists(bloomMetaPath));

    // A new relation with the same name must not inherit any definition from
    // the dropped object.
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.getStorageParams(db, "reusable").empty());
    assert(g_engine.getPolicies(db, "reusable").empty());
    assert(g_engine.getBloomIndexedColumns(db, "reusable").empty());
    assert(g_engine.createBloomIndex(db, "reusable", "tag") ==
           dbms::DBStatus::OK);

    assert(g_engine.dropTable(db, "reusable") == dbms::DBStatus::OK);
    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] DROP removes named sidecars OK" << std::endl;
}

static void test_drop_purges_authorization_state() {
    std::string db = testDbPath("drop_authorization");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema reusable;
    reusable.tablename = "reusable_acl";
    reusable.append(dbms::makeIntColumn("id", false, 2));
    assert(g_engine.createTable(db, reusable) == dbms::DBStatus::OK);

    dbms::TableSchema survivor = reusable;
    survivor.tablename = "survivor_acl";
    assert(g_engine.createTable(db, survivor) == dbms::DBStatus::OK);

    g_engine.grant(db, "reusable_acl", "alice",
                   dbms::StorageEngine::TablePrivilege::Select, {}, true,
                   "owner");
    g_engine.grant(db, "reusable_acl", "bob",
                   dbms::StorageEngine::TablePrivilege::Select, {}, false,
                   "alice");
    g_engine.grant(db, "survivor_acl", "carol",
                   dbms::StorageEngine::TablePrivilege::Select, {}, true,
                   "owner");
    assert(g_engine.hasPermission(
        db, "reusable_acl", "alice",
        dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasPermission(
        db, "reusable_acl", "bob",
        dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasGrantOption(
        db, "reusable_acl", "alice",
        dbms::StorageEngine::TablePrivilege::Select));

    assert(g_engine.dropTable(db, "reusable_acl") == dbms::DBStatus::OK);
    assert(g_engine.createTable(db, reusable) == dbms::DBStatus::OK);
    assert(!g_engine.hasPermission(
        db, "reusable_acl", "alice",
        dbms::StorageEngine::TablePrivilege::Select));
    assert(!g_engine.hasPermission(
        db, "reusable_acl", "bob",
        dbms::StorageEngine::TablePrivilege::Select));
    assert(!g_engine.hasGrantOption(
        db, "reusable_acl", "alice",
        dbms::StorageEngine::TablePrivilege::Select));

    // Grants belonging to other relations remain intact in both files.
    assert(g_engine.hasPermission(
        db, "survivor_acl", "carol",
        dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasGrantOption(
        db, "survivor_acl", "carol",
        dbms::StorageEngine::TablePrivilege::Select));
    std::ifstream chain(fs::path(db) / ".grant_chain");
    const std::string chainContents{
        std::istreambuf_iterator<char>(chain),
        std::istreambuf_iterator<char>()};
    assert(chainContents.find(" reusable_acl ") == std::string::npos);
    assert(chainContents.find(" survivor_acl ") != std::string::npos);

    assert(g_engine.dropTable(db, "reusable_acl") == dbms::DBStatus::OK);
    assert(g_engine.dropTable(db, "survivor_acl") == dbms::DBStatus::OK);
    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] DROP purges authorization state OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_drop_table_cascade_removes_dependents();
    test_schema_table_cascade_resolves_index_owner();
    test_multi_table_drop_fails_before_mutation();
    test_drop_removes_named_table_sidecars();
    test_drop_purges_authorization_state();
    std::cout << "[DROP-CASCADE] all passed" << std::endl;
    return 0;
}
