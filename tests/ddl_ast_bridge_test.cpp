#include "commands/DdlExecutor.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>
#include "test_utils.h"

// Stubs for main.cpp helpers referenced by DdlExecutor (provided by tests/test_stubs.cpp)
extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_create_drop_table() {
    std::string db = testDbPath("ddl_bridge_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name VARCHAR(100))", s);
    assert(!err);
    assert(g_engine.tableExists(db, "t1"));

    err = ddl.executeSql("DROP TABLE t1", s);
    assert(!err);
    assert(!g_engine.tableExists(db, "t1"));

    err = ddl.executeSql("CREATE TABLE t2 (x INT)", s);
    assert(!err);
    err = ddl.executeSql("CREATE TABLE IF NOT EXISTS t2 (x INT)", s);
    assert(!err);

    cleanup(db);
    std::cout << "[DDL] create/drop table OK" << std::endl;
}

static void test_create_table_registers_in_catalog() {
    std::string db = testDbPath("ddl_bridge_t1_cat");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TABLE cat_t1 (id INT, name VARCHAR(100))", s);
    assert(!err);

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    const auto* nsPublic = cat.findNamespaceByName("public");
    assert(nsPublic != nullptr);
    const auto* cls = cat.findClassByName("cat_t1", nsPublic->oid);
    assert(cls != nullptr);
    assert(cls->relnatts == 2);
    assert(cls->relkind == 'r');

    cleanup(db);
    std::cout << "[DDL] CREATE TABLE registers in catalog OK" << std::endl;
}

static void test_create_table_requires_existing_schema() {
    const std::string db = testDbPath("ddl_missing_table_schema");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE source_rows (id INT)", s));
    assert(g_engine.insert(db, "source_rows", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(!g_engine.schemaExists(db, "missing_schema"));
    assert(ddl.executeSql(
        "CREATE TABLE missing_schema.plain_copy (id INT)", s));
    assert(ddl.executeSql(
        "CREATE TABLE missing_schema.ctas_copy AS SELECT id FROM source_rows",
        s));
    assert(ddl.executeSql(
        "CREATE TABLE missing_schema.like_copy (LIKE source_rows)", s));

    for (const auto& name : {"plain_copy", "ctas_copy", "like_copy"}) {
        assert(!g_engine.tableExists(
            db, std::string("missing_schema.") + name));
        assert(!g_engine.tableExists(
            db, std::string("missing_schema__") + name));
    }
    assert(!g_engine.schemaExists(db, "missing_schema"));
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    assert(catalog.findNamespaceByName("missing_schema") == nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] CREATE TABLE requires an existing schema OK"
              << std::endl;
}

static void test_alter_table_rename_updates_catalog() {
    std::string db = testDbPath("ddl_bridge_rename_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE rename_source (id INT)", s));

    dbms::Oid originalOid = dbms::INVALID_OID;
    {
        dbms::CatalogManager& initialCatalog =
            g_engine.catalogService().get(db);
        const auto* initialPublic =
            initialCatalog.findNamespaceByName("public");
        assert(initialPublic != nullptr);
        const auto* original = initialCatalog.findClassByName(
            "rename_source", initialPublic->oid);
        assert(original != nullptr);
        originalOid = original->oid;
    }

    // Relation names share a catalog namespace with indexes.  Storage alone
    // cannot see this collision, so the catalog rejection must roll the
    // physical rename back to its original name.
    assert(!ddl.executeSql(
        "CREATE INDEX rename_collision ON rename_source (id)", s));
    assert(ddl.executeSql(
        "ALTER TABLE rename_source RENAME TO rename_collision", s));
    assert(g_engine.tableExists(db, "rename_source"));
    assert(!g_engine.tableExists(db, "rename_collision"));
    // Snapshot rollback evicts the catalog cache, so reacquire it before
    // inspecting the restored state.
    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    const auto* nsPublic = cat.findNamespaceByName("public");
    assert(nsPublic != nullptr);
    const auto* original = cat.findClassByName(
        "rename_source", nsPublic->oid);
    assert(original != nullptr && original->oid == originalOid);

    assert(!ddl.executeSql(
        "ALTER TABLE rename_source RENAME TO rename_target", s));
    assert(!g_engine.tableExists(db, "rename_source"));
    assert(g_engine.tableExists(db, "rename_target"));
    assert(cat.findClassByName("rename_source", nsPublic->oid) == nullptr);
    const auto* renamed = cat.findClassByName("rename_target", nsPublic->oid);
    assert(renamed != nullptr && renamed->oid == originalOid);

    // The rename must survive a catalog cache reload, not merely update the
    // current process's name index.
    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    const auto* reloadedPublic = reloaded.findNamespaceByName("public");
    assert(reloadedPublic != nullptr);
    assert(reloaded.findClassByName("rename_source", reloadedPublic->oid) == nullptr);
    renamed = reloaded.findClassByName("rename_target", reloadedPublic->oid);
    assert(renamed != nullptr && renamed->oid == originalOid);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] ALTER TABLE RENAME updates catalog OK" << std::endl;
}

static void test_alter_column_rename_updates_catalog() {
    const std::string db = testDbPath("ddl_bridge_column_rename_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE rename_columns (old_name INT, untouched TEXT)", s));

    dbms::Oid relationOid = dbms::INVALID_OID;
    {
        dbms::CatalogManager& catalog =
            g_engine.catalogService().get(db);
        const auto* relation =
            catalog.resolveRelation("rename_columns", {"public"});
        assert(relation != nullptr);
        relationOid = relation->oid;
        const auto* oldAttribute =
            catalog.findAttribute(relationOid, "old_name");
        assert(oldAttribute != nullptr && oldAttribute->attnum == 1);
    }

    assert(!ddl.executeSql(
        "ALTER TABLE rename_columns RENAME COLUMN old_name TO new_name", s));
    const auto schema = g_engine.getTableSchema(db, "rename_columns");
    assert(schema.cols[0].dataName == "new_name");

    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    assert(catalog.findAttribute(relationOid, "old_name") == nullptr);
    const auto* renamedAttribute =
        catalog.findAttribute(relationOid, "new_name");
    assert(renamedAttribute != nullptr && renamedAttribute->attnum == 1);

    // The pg_attribute update must be durable, not just an in-memory rename.
    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    const auto* relation =
        reloaded.resolveRelation("rename_columns", {"public"});
    assert(relation != nullptr && relation->oid == relationOid);
    assert(reloaded.findAttribute(relationOid, "old_name") == nullptr);
    renamedAttribute = reloaded.findAttribute(relationOid, "new_name");
    assert(renamedAttribute != nullptr && renamedAttribute->attnum == 1);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] ALTER TABLE RENAME COLUMN updates catalog OK"
              << std::endl;
}

static void test_alter_column_definitions_update_catalog() {
    const std::string db = testDbPath("ddl_bridge_column_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE catalog_columns (removed INT, retained VARCHAR(20))", s));

    dbms::CatalogManager& initialCatalog =
        g_engine.catalogService().get(db);
    const auto* relation =
        initialCatalog.resolveRelation("catalog_columns", {"public"});
    assert(relation != nullptr && relation->relnatts == 2);
    const dbms::Oid relationOid = relation->oid;
    const auto* retained =
        initialCatalog.findAttribute(relationOid, "retained");
    assert(retained != nullptr && retained->attnum == 2);
    const dbms::Oid originalType = retained->atttypid;

    assert(!ddl.executeSql(
        "ALTER TABLE catalog_columns ADD COLUMN added BIGINT DEFAULT 7", s));
    relation = initialCatalog.findClass(relationOid);
    assert(relation != nullptr && relation->relnatts == 3);
    const auto* added = initialCatalog.findAttribute(relationOid, "added");
    assert(added != nullptr && added->attnum == 3 && added->atthasdef);

    assert(!ddl.executeSql(
        "ALTER TABLE catalog_columns ALTER COLUMN retained TYPE BIGINT", s));
    retained = initialCatalog.findAttribute(relationOid, "retained");
    assert(retained != nullptr && retained->atttypid != originalType &&
           retained->attlen == 8);

    assert(!ddl.executeSql(
        "ALTER TABLE catalog_columns ALTER COLUMN retained SET DEFAULT 9", s));
    assert(!ddl.executeSql(
        "ALTER TABLE catalog_columns ALTER COLUMN retained SET NOT NULL", s));
    retained = initialCatalog.findAttribute(relationOid, "retained");
    assert(retained != nullptr && retained->atthasdef && retained->attnotnull);

    assert(!ddl.executeSql(
        "ALTER TABLE catalog_columns ALTER COLUMN retained DROP DEFAULT", s));
    assert(!ddl.executeSql(
        "ALTER TABLE catalog_columns ALTER COLUMN retained DROP NOT NULL", s));
    retained = initialCatalog.findAttribute(relationOid, "retained");
    assert(retained != nullptr && !retained->atthasdef &&
           !retained->attnotnull);

    assert(!ddl.executeSql(
        "ALTER TABLE catalog_columns DROP COLUMN removed", s));
    relation = initialCatalog.findClass(relationOid);
    assert(relation != nullptr && relation->relnatts == 2);
    assert(initialCatalog.findAttribute(relationOid, "removed") == nullptr);
    retained = initialCatalog.findAttribute(relationOid, "retained");
    added = initialCatalog.findAttribute(relationOid, "added");
    assert(retained != nullptr && retained->attnum == 1);
    assert(added != nullptr && added->attnum == 2 && added->atthasdef);

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    relation = reloaded.findClass(relationOid);
    assert(relation != nullptr && relation->relnatts == 2);
    assert(reloaded.findAttribute(relationOid, "removed") == nullptr);
    retained = reloaded.findAttribute(relationOid, "retained");
    added = reloaded.findAttribute(relationOid, "added");
    assert(retained != nullptr && retained->attnum == 1 &&
           retained->attlen == 8 && !retained->atthasdef &&
           !retained->attnotnull);
    assert(added != nullptr && added->attnum == 2 && added->atthasdef);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] ALTER column definitions update catalog OK"
              << std::endl;
}

static void test_alter_logged_state_updates_catalog() {
    const std::string db = testDbPath("ddl_bridge_logged_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE persistence_catalog (id INT)", s));

    dbms::CatalogManager& initial = g_engine.catalogService().get(db);
    const auto* relation =
        initial.resolveRelation("persistence_catalog", {"public"});
    assert(relation != nullptr && relation->relpersistence == 'p');
    const dbms::Oid relationOid = relation->oid;

    assert(!ddl.executeSql(
        "ALTER TABLE persistence_catalog SET UNLOGGED", s));
    assert(g_engine.getTableSchema(db, "persistence_catalog").isUnlogged);
    relation = initial.findClass(relationOid);
    assert(relation != nullptr && relation->relpersistence == 'u');

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& unlogged = g_engine.catalogService().get(db);
    relation = unlogged.findClass(relationOid);
    assert(relation != nullptr && relation->relpersistence == 'u');

    assert(!ddl.executeSql(
        "ALTER TABLE persistence_catalog SET LOGGED", s));
    assert(!g_engine.getTableSchema(db, "persistence_catalog").isUnlogged);
    relation = unlogged.findClass(relationOid);
    assert(relation != nullptr && relation->relpersistence == 'p');

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& logged = g_engine.catalogService().get(db);
    relation = logged.findClass(relationOid);
    assert(relation != nullptr && relation->relpersistence == 'p');

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] ALTER logged state updates catalog OK" << std::endl;
}

static void test_alter_rls_state_updates_catalog() {
    const std::string db = testDbPath("ddl_bridge_rls_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE rls_catalog (id INT)", s));

    dbms::CatalogManager& initial = g_engine.catalogService().get(db);
    const auto* relation =
        initial.resolveRelation("rls_catalog", {"public"});
    assert(relation != nullptr && !relation->relrowsecurity &&
           !relation->relforcerowsecurity);
    const dbms::Oid relationOid = relation->oid;

    // FORCE and ENABLE are independent relation properties.  In particular,
    // forcing a disabled table must not silently enable policy enforcement.
    assert(!ddl.executeSql(
        "ALTER TABLE rls_catalog FORCE ROW LEVEL SECURITY", s));
    auto schema = g_engine.getTableSchema(db, "rls_catalog");
    assert(!schema.rowLevelSecurity && schema.forceRowLevelSecurity);
    relation = initial.findClass(relationOid);
    assert(relation != nullptr && !relation->relrowsecurity &&
           relation->relforcerowsecurity);

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& forced = g_engine.catalogService().get(db);
    relation = forced.findClass(relationOid);
    assert(relation != nullptr && !relation->relrowsecurity &&
           relation->relforcerowsecurity);

    // ENABLE preserves FORCE, and DISABLE likewise leaves FORCE configured.
    assert(!ddl.executeSql(
        "ALTER TABLE rls_catalog ENABLE ROW LEVEL SECURITY", s));
    schema = g_engine.getTableSchema(db, "rls_catalog");
    assert(schema.rowLevelSecurity && schema.forceRowLevelSecurity);
    relation = forced.findClass(relationOid);
    assert(relation != nullptr && relation->relrowsecurity &&
           relation->relforcerowsecurity);

    assert(!ddl.executeSql(
        "ALTER TABLE rls_catalog DISABLE ROW LEVEL SECURITY", s));
    schema = g_engine.getTableSchema(db, "rls_catalog");
    assert(!schema.rowLevelSecurity && schema.forceRowLevelSecurity);
    relation = forced.findClass(relationOid);
    assert(relation != nullptr && !relation->relrowsecurity &&
           relation->relforcerowsecurity);

    assert(!ddl.executeSql(
        "ALTER TABLE rls_catalog NO FORCE ROW LEVEL SECURITY", s));
    schema = g_engine.getTableSchema(db, "rls_catalog");
    assert(!schema.rowLevelSecurity && !schema.forceRowLevelSecurity);
    relation = forced.findClass(relationOid);
    assert(relation != nullptr && !relation->relrowsecurity &&
           !relation->relforcerowsecurity);

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    relation = reloaded.findClass(relationOid);
    assert(relation != nullptr && !relation->relrowsecurity &&
           !relation->relforcerowsecurity);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] ALTER RLS state updates catalog OK" << std::endl;
}

static void test_check_constraint_count_updates_catalog() {
    const std::string db = testDbPath("ddl_bridge_check_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE check_catalog ("
        "a INT CHECK (a > 0) CHECK (a < 100), "
        "b INT, c INT CHECK (c <> 10), "
        "CONSTRAINT b_nonnegative CHECK (b >= 0))", s));

    dbms::CatalogManager& initial = g_engine.catalogService().get(db);
    const auto* relation =
        initial.resolveRelation("check_catalog", {"public"});
    assert(relation != nullptr && relation->relchecks == 4);
    const dbms::Oid relationOid = relation->oid;

    assert(!ddl.executeSql(
        "ALTER TABLE check_catalog DROP COLUMN c", s));
    relation = initial.findClass(relationOid);
    assert(relation != nullptr && relation->relchecks == 3);

    assert(!ddl.executeSql(
        "ALTER TABLE check_catalog ADD COLUMN c INT", s));
    relation = initial.findClass(relationOid);
    assert(relation != nullptr && relation->relchecks == 3);

    assert(!ddl.executeSql(
        "ALTER TABLE check_catalog DROP COLUMN c", s));
    relation = initial.findClass(relationOid);
    assert(relation != nullptr && relation->relchecks == 3);

    assert(!ddl.executeSql(
        "ALTER TABLE check_catalog ADD CONSTRAINT a_not_50 "
        "CHECK (a <> 50)", s));
    relation = initial.findClass(relationOid);
    assert(relation != nullptr && relation->relchecks == 4);

    assert(!ddl.executeSql(
        "ALTER TABLE check_catalog DROP CONSTRAINT a_not_50", s));
    relation = initial.findClass(relationOid);
    assert(relation != nullptr && relation->relchecks == 3);

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    relation = reloaded.findClass(relationOid);
    assert(relation != nullptr && relation->relchecks == 3);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] CHECK constraint count updates catalog OK"
              << std::endl;
}

static void test_schema_qualified_rename_preserves_schema() {
    const std::string db = testDbPath("ddl_bridge_schema_rename");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA analytics", s));
    assert(!ddl.executeSql(
        "CREATE TABLE analytics.rename_source (id INT)", s));
    assert(g_engine.tableExists(db, "analytics__rename_source"));

    assert(!ddl.executeSql(
        "ALTER TABLE analytics.rename_source RENAME TO rename_target", s));
    assert(!g_engine.tableExists(db, "analytics__rename_source"));
    assert(g_engine.tableExists(db, "analytics__rename_target"));
    assert(!g_engine.tableExists(db, "rename_target"));

    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* analytics = catalog.findNamespaceByName("analytics");
    assert(analytics != nullptr);
    assert(catalog.findClassByName("rename_source", analytics->oid) == nullptr);
    assert(catalog.findClassByName("rename_target", analytics->oid) != nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] schema-qualified RENAME preserves schema OK" << std::endl;
}

static void test_create_index_sequence() {
    std::string db = testDbPath("ddl_bridge_t2");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    ddl.executeSql("CREATE TABLE idx_tbl (a INT, b VARCHAR(50))", s);
    bool err = ddl.executeSql("CREATE INDEX idx_a ON idx_tbl (a)", s);
    assert(!err);

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    const auto* nsPublic = cat.findNamespaceByName("public");
    assert(nsPublic != nullptr);
    const auto* idx = cat.findClassByName("idx_a", nsPublic->oid);
    assert(idx != nullptr);
    assert(idx->relkind == 'i');

    err = ddl.executeSql("CREATE SEQUENCE seq1 START 10 INCREMENT 2", s);
    assert(!err);
    assert(g_engine.sequenceExists(db, "seq1"));

    const auto* seq = cat.findClassByName("seq1", nsPublic->oid);
    assert(seq != nullptr);
    assert(seq->relkind == 'S');

    err = ddl.executeSql("DROP SEQUENCE seq1", s);
    assert(!err);
    assert(cat.findClassByName("seq1", nsPublic->oid) == nullptr);

    // Dropping the table with CASCADE should remove the dependent index.
    err = ddl.executeSql("DROP TABLE idx_tbl CASCADE", s);
    assert(!err);
    assert(cat.findClassByName("idx_a", nsPublic->oid) == nullptr);

    cleanup(db);
    std::cout << "[DDL] index/sequence OK" << std::endl;
}

static void test_table_index_flag_updates_catalog() {
    const std::string db = testDbPath("ddl_bridge_table_index_flag");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE indexed_table (a INT, b INT)", s));

    dbms::CatalogManager& initial = g_engine.catalogService().get(db);
    const auto* relation =
        initial.resolveRelation("indexed_table", {"public"});
    assert(relation != nullptr && !relation->relhasindex);
    const dbms::Oid tableOid = relation->oid;

    assert(!ddl.executeSql(
        "CREATE INDEX indexed_table_a_idx ON indexed_table (a)", s));
    assert(!ddl.executeSql(
        "CREATE INDEX indexed_table_b_idx ON indexed_table (b)", s));
    relation = initial.findClass(tableOid);
    assert(relation != nullptr && relation->relhasindex);

    // Read through an independent manager rather than evicting the live one:
    // eviction's destructor persists dirty memory and would mask a CREATE
    // INDEX path that forgot to make its catalog update durable.
    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* durableNamespace =
            durable.findNamespaceByName("public");
        assert(durableNamespace != nullptr);
        const auto* durableTable = durable.findClass(tableOid);
        assert(durableTable != nullptr && durableTable->relhasindex);
        const auto* durableIndex = durable.findClassByName(
            "indexed_table_a_idx", durableNamespace->oid);
        assert(durableIndex != nullptr && durableIndex->relkind == 'i');
        const auto dependencies = durable.findDepends(
            dbms::PgClassOid_Class, durableIndex->oid);
        bool ownsIndex = false;
        for (const auto& dependency : dependencies) {
            if (dependency.refclassid == dbms::PgClassOid_Class &&
                dependency.refobjid == tableOid) {
                ownsIndex = true;
                break;
            }
        }
        assert(ownsIndex);
    }

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& indexed = g_engine.catalogService().get(db);
    relation = indexed.findClass(tableOid);
    assert(relation != nullptr && relation->relhasindex);

    assert(!ddl.executeSql("DROP INDEX indexed_table_a_idx", s));
    relation = indexed.findClass(tableOid);
    assert(relation != nullptr && relation->relhasindex);
    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* durableNamespace =
            durable.findNamespaceByName("public");
        assert(durableNamespace != nullptr);
        const auto* durableTable = durable.findClass(tableOid);
        assert(durableTable != nullptr && durableTable->relhasindex);
        assert(durable.findClassByName(
                   "indexed_table_a_idx", durableNamespace->oid) == nullptr);
        const auto* durableIndex = durable.findClassByName(
            "indexed_table_b_idx", durableNamespace->oid);
        assert(durableIndex != nullptr && durableIndex->relkind == 'i');
    }

    assert(!ddl.executeSql("DROP INDEX indexed_table_b_idx", s));
    relation = indexed.findClass(tableOid);
    assert(relation != nullptr && !relation->relhasindex);
    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* durableNamespace =
            durable.findNamespaceByName("public");
        assert(durableNamespace != nullptr);
        const auto* durableTable = durable.findClass(tableOid);
        assert(durableTable != nullptr && !durableTable->relhasindex);
        assert(durable.findClassByName(
                   "indexed_table_a_idx", durableNamespace->oid) == nullptr);
        assert(durable.findClassByName(
                   "indexed_table_b_idx", durableNamespace->oid) == nullptr);
    }

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& unindexed = g_engine.catalogService().get(db);
    relation = unindexed.findClass(tableOid);
    assert(relation != nullptr && !relation->relhasindex);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] table index flag updates catalog OK" << std::endl;
}

static void test_create_database_schema() {
    std::string db = testDbPath("ddl_bridge_t3");
    cleanup(db);

    Session s;
    setupSession(s, "");
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE DATABASE " + db, s);
    assert(!err);
    assert(g_engine.databaseExists(db));

    s.currentDB = db;
    err = ddl.executeSql("CREATE SCHEMA myschema", s);
    assert(!err);
    assert(g_engine.schemaExists(db, "myschema"));

    // CREATE SCHEMA is not silently idempotent; the explicit PostgreSQL
    // spelling is required when an existing namespace should be accepted.
    assert(ddl.executeSql("CREATE SCHEMA myschema", s));
    assert(!ddl.executeSql("CREATE SCHEMA IF NOT EXISTS myschema", s));

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    assert(cat.findNamespaceByName("myschema") != nullptr);

    err = ddl.executeSql("DROP SCHEMA myschema RESTRICT", s);
    assert(!err);
    assert(!g_engine.schemaExists(db, "myschema"));
    assert(cat.findNamespaceByName("myschema") == nullptr);

    cleanup(db);
    std::cout << "[DDL] database/schema OK" << std::endl;
}

static void test_drop_index_uses_sql_name() {
    std::string db = testDbPath("ddl_drop_index");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE drop_idx_tbl (id INT, value INT, location POINT)", s));
    assert(g_engine.insert(db, "drop_idx_tbl",
        {{"id", "1"}, {"value", "10"}, {"location", "1,1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "drop_idx_tbl",
        {{"id", "2"}, {"value", "20"}, {"location", "2,2"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE INDEX idx_drop_value ON drop_idx_tbl (value)", s));

    auto named = g_engine.getNamedIndex(db, "drop_idx_tbl", "idx_drop_value");
    assert(named.has_value());
    assert(named->accessMethod == "btree");
    assert(named->key == "value");

    assert(!ddl.executeSql("DROP INDEX idx_drop_value", s));
    assert(!g_engine.getNamedIndex(db, "drop_idx_tbl", "idx_drop_value").has_value());
    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    const auto* ns = cat.findNamespaceByName("public");
    assert(ns != nullptr);
    assert(cat.findClassByName("idx_drop_value", ns->oid) == nullptr);

    assert(!ddl.executeSql("CREATE INDEX idx_drop_composite ON drop_idx_tbl (id, value)", s));
    assert(!ddl.executeSql("DROP INDEX idx_drop_composite ON drop_idx_tbl", s));
    assert(!ddl.executeSql("DROP INDEX IF EXISTS idx_missing", s));

    assert(!ddl.executeSql("CREATE INDEX idx_duplicate ON drop_idx_tbl (id)", s));
    assert(!ddl.executeSql("CREATE INDEX IF NOT EXISTS idx_duplicate ON drop_idx_tbl (id)", s));
    assert(ddl.executeSql("CREATE INDEX idx_duplicate ON drop_idx_tbl (id)", s));
    assert(!ddl.executeSql("DROP INDEX idx_duplicate", s));

    // Standard PostgreSQL access-method syntax must use the real specialized
    // StorageEngine implementation, not the former B-tree compatibility path.
    assert(!ddl.executeSql("CREATE INDEX idx_gin ON drop_idx_tbl USING GIN (value)", s));
    assert(!ddl.executeSql("CREATE INDEX idx_gist ON drop_idx_tbl (id) USING GiST", s));
    assert(!ddl.executeSql("CREATE INDEX idx_brin ON drop_idx_tbl USING BRIN (id)", s));
    assert(!ddl.executeSql(
        "CREATE INDEX idx_spgist ON drop_idx_tbl USING SPGIST (location)", s));
    assert(g_engine.getNamedIndex(db, "drop_idx_tbl", "idx_gin")->accessMethod == "gin");
    assert(g_engine.getNamedIndex(db, "drop_idx_tbl", "idx_gist")->accessMethod == "gist");
    assert(g_engine.getNamedIndex(db, "drop_idx_tbl", "idx_brin")->accessMethod == "brin");
    assert(g_engine.getNamedIndex(db, "drop_idx_tbl", "idx_spgist")->accessMethod == "spgist");
    assert(!g_engine.ginSearch(db, "drop_idx_tbl", "value", "10").empty());
    assert(!g_engine.brinSearchRange(db, "drop_idx_tbl", "id", "=", "1").empty());
    {
        std::ofstream broken(db + "/drop_idx_tbl_value.gin", std::ios::trunc);
        broken << "corrupt posting not-a-rid\n";
    }
    assert(g_engine.ginSearch(db, "drop_idx_tbl", "value", "10").empty());
    {
        std::ofstream broken(db + "/drop_idx_tbl_id.brin", std::ios::binary | std::ios::trunc);
        broken << "corrupt brin";
    }
    assert(g_engine.brinSearchRange(db, "drop_idx_tbl", "id", "=", "1").empty());
    assert(!ddl.executeSql("DROP INDEX idx_gin, idx_gist, idx_brin, idx_spgist", s));

    cleanup(db);
    std::cout << "[DDL] standard DROP INDEX name resolution OK" << std::endl;
}

static void test_drop_schema_qualified_index() {
    const std::string db = testDbPath("ddl_drop_schema_index");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA inventory", s));
    assert(!ddl.executeSql(
        "CREATE TABLE inventory.items (id INT, sku INT)", s));
    assert(!ddl.executeSql(
        "CREATE INDEX items_sku_idx ON inventory.items (sku)", s));

    const std::string physicalTable = "inventory__items";
    assert(g_engine.getNamedIndex(
        db, physicalTable, "items_sku_idx").has_value());
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* inventory = catalog.findNamespaceByName("inventory");
    assert(inventory != nullptr);
    assert(catalog.findClassByName(
        "items_sku_idx", inventory->oid) != nullptr);

    assert(ddl.executeSql(
        "CREATE INDEX inventory.invalid_idx ON inventory.items (id)", s));
    assert(!g_engine.getNamedIndex(
        db, physicalTable, "invalid_idx").has_value());
    assert(catalog.findClassByName(
        "invalid_idx", inventory->oid) == nullptr);

    assert(!ddl.executeSql("DROP INDEX inventory.items_sku_idx", s));
    assert(!g_engine.getNamedIndex(
        db, physicalTable, "items_sku_idx").has_value());
    assert(catalog.findClassByName(
        "items_sku_idx", inventory->oid) == nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] schema-qualified DROP INDEX OK" << std::endl;
}

static void test_comment_on() {
    std::string db = testDbPath("ddl_bridge_t4");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    s.pid = 515151;
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE cmt_tbl (id INT)", s));
    bool err = ddl.executeSql(
        "COMMENT ON TABLE cmt_tbl IS 'A Mixed-Case table'", s);
    assert(!err);
    assert(g_engine.getTableComment(db, "cmt_tbl") == "A Mixed-Case table");

    err = ddl.executeSql("COMMENT ON COLUMN cmt_tbl.id IS 'primary key'", s);
    assert(!err);
    assert(g_engine.getColumnComment(db, "cmt_tbl", "id") == "primary key");

    assert(!ddl.executeSql("CREATE SCHEMA comment_schema", s));
    assert(!ddl.executeSql(
        "CREATE TABLE comment_schema.notes (id INT, body VARCHAR(20))", s));
    assert(!ddl.executeSql(
        "COMMENT ON TABLE comment_schema.notes IS 'schema table'", s));
    assert(!ddl.executeSql(
        "COMMENT ON COLUMN comment_schema.notes.body IS 'schema column'", s));
    assert(g_engine.getTableComment(
               db, "comment_schema__notes") == "schema table");
    assert(g_engine.getColumnComment(
               db, "comment_schema__notes", "body") == "schema column");

    assert(!ddl.executeSql(
        "CREATE TEMP TABLE session_notes (id INT, body VARCHAR(20))", s));
    const std::string temporaryName = tempTablePrefix(s, "session_notes");
    assert(!ddl.executeSql(
        "COMMENT ON TABLE session_notes IS 'temporary table'", s));
    assert(!ddl.executeSql(
        "COMMENT ON COLUMN session_notes.body IS 'temporary column'", s));
    assert(g_engine.getTableComment(db, temporaryName) == "temporary table");
    assert(g_engine.getColumnComment(
               db, temporaryName, "body") == "temporary column");

    cleanup(db);
    std::cout << "[DDL] comment namespace resolution OK" << std::endl;
}

static void test_alter_table_metadata_actions() {
    std::string db = testDbPath("ddl_alter_metadata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE alter_meta (id INT, v INT)", s));
    assert(!ddl.executeSql("CREATE INDEX alter_meta_id_idx ON alter_meta (id)", s));

    assert(!ddl.executeSql("ALTER TABLE alter_meta CLUSTER ON alter_meta_id_idx", s));
    auto params = g_engine.getStorageParams(db, "alter_meta");
    assert(params.at("cluster_on") == "alter_meta_id_idx");
    assert(!ddl.executeSql("ALTER TABLE alter_meta SET WITHOUT CLUSTER", s));
    params = g_engine.getStorageParams(db, "alter_meta");
    assert(params.count("cluster_on") == 0);
    assert(ddl.executeSql("ALTER TABLE alter_meta CLUSTER ON missing_idx", s));

    assert(!ddl.executeSql("ALTER TABLE alter_meta REPLICA IDENTITY FULL", s));
    auto& cat = g_engine.catalogService().get(db);
    const auto* ns = cat.findNamespaceByName("public");
    assert(ns != nullptr);
    const auto* relation = cat.findClassByName("alter_meta", ns->oid);
    assert(relation != nullptr && relation->relreplident == 'f');
    assert(!ddl.executeSql("ALTER TABLE alter_meta REPLICA IDENTITY USING INDEX alter_meta_id_idx", s));
    relation = cat.findClassByName("alter_meta", ns->oid);
    assert(relation != nullptr && relation->relreplident == 'i');
    params = g_engine.getStorageParams(db, "alter_meta");
    assert(params.at("replica_identity_index") == "alter_meta_id_idx");
    assert(!ddl.executeSql("ALTER TABLE alter_meta REPLICA IDENTITY DEFAULT", s));
    relation = cat.findClassByName("alter_meta", ns->oid);
    assert(relation != nullptr && relation->relreplident == 'd');

    assert(!ddl.executeSql(
        "ALTER TABLE alter_meta ADD CONSTRAINT alter_meta_positive CHECK (id > 0) NOT VALID", s));
    auto constraintParams = g_engine.getStorageParams(db, "alter_meta");
    assert(constraintParams.at("constraint.alter_meta_positive.not_valid") == "1");
    assert(!ddl.executeSql(
        "ALTER TABLE alter_meta ALTER CONSTRAINT alter_meta_positive DEFERRABLE INITIALLY DEFERRED", s));
    auto schema = g_engine.getTableSchema(db, "alter_meta");
    assert(schema.cols[0].deferrable);
    assert(schema.cols[0].initiallyDeferred);
    assert(!ddl.executeSql("ALTER TABLE alter_meta VALIDATE CONSTRAINT alter_meta_positive", s));
    params = g_engine.getStorageParams(db, "alter_meta");
    assert(params.at("constraint.alter_meta_positive.validated") == "1");
    assert(g_engine.getTableSchema(db, "alter_meta").cols[0].checkConstraintName ==
           "alter_meta_positive");

    cleanup(db);
    std::cout << "[DDL] ALTER TABLE metadata actions OK" << std::endl;
}

static void test_schema_replica_identity_updates_catalog() {
    const std::string db = testDbPath("ddl_schema_replica_identity");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA replication", s));
    assert(!ddl.executeSql(
        "CREATE TABLE replication.events (id INT, payload TEXT)", s));
    assert(!ddl.executeSql(
        "ALTER TABLE replication.events REPLICA IDENTITY FULL", s));

    auto params = g_engine.getStorageParams(db, "replication__events");
    assert(params.at("replica_identity") == "full");
    dbms::CatalogManager& initial = g_engine.catalogService().get(db);
    const auto* relation = initial.resolveRelation(
        "events", {"replication"});
    assert(relation != nullptr && relation->relreplident == 'f');
    const dbms::Oid relationOid = relation->oid;

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    relation = reloaded.findClass(relationOid);
    assert(relation != nullptr && relation->relreplident == 'f');

    assert(!ddl.executeSql(
        "ALTER TABLE replication.events REPLICA IDENTITY NOTHING", s));
    relation = reloaded.findClass(relationOid);
    assert(relation != nullptr && relation->relreplident == 'n');
    params = g_engine.getStorageParams(db, "replication__events");
    assert(params.at("replica_identity") == "nothing");

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] schema replica identity updates catalog OK"
              << std::endl;
}

static void test_long_identifiers_round_trip() {
    std::string db = testDbPath("ddl_long_identifiers");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    const std::string tableName = "production_identifier_table";
    const std::string columnName = "production_identifier_column";
    const std::string constraintName = "production_identifier_check";
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE " + tableName + " (" + columnName + " INT)", s));
    auto schema = g_engine.getTableSchema(db, tableName);
    assert(schema.tablename == tableName);
    assert(schema.len == 1 && schema.cols[0].dataName == columnName);
    assert(!ddl.executeSql(
        "ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName +
        " CHECK (" + columnName + " > 0)", s));
    schema = g_engine.getTableSchema(db, tableName);
    assert(schema.cols[0].checkConstraintName == constraintName);

    auto& cat = g_engine.catalogService().get(db);
    const auto* ns = cat.findNamespaceByName("public");
    assert(ns != nullptr);
    assert(cat.findClassByName(tableName, ns->oid) != nullptr);

    cleanup(db);
    std::cout << "[DDL] long identifier round-trip OK" << std::endl;
}

static void test_drop_database_evicts_catalog() {
    std::string db = testDbPath("ddl_bridge_t5");
    cleanup(db);

    Session s;
    setupSession(s, "");
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE DATABASE " + db, s);
    assert(!err);

    // Touch the catalog so it is cached for this database.
    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    assert(cat.findNamespaceByName("public") != nullptr);
    assert(g_engine.catalogService().has(db));

    err = ddl.executeSql("DROP DATABASE " + db, s);
    assert(!err);
    assert(!g_engine.catalogService().has(db));

    cleanup(db);
    std::cout << "[DDL] DROP DATABASE evicts catalog OK" << std::endl;
}

static void test_database_storage_errors_are_not_success() {
    const std::string parent = testDbPath("database_error_parent");
    const std::string nested = parent + "/child";
    cleanup(parent);

    Session s;
    setupSession(s, "");
    dbms::DdlExecutor ddl;
    auto createStmt = std::make_unique<dbms::CreateObjectStmt>(
        dbms::SqlCommand::CreateDatabase);
    createStmt->objectName = nested;
    dbms::StmtPtr create = std::move(createStmt);

    // The parent does not exist, so StorageEngine returns IO_ERROR.  The DDL
    // layer must not report a false successful CREATE DATABASE.
    assert(ddl.execute(create, s));
    assert(!std::filesystem::exists(parent));

    cleanup(parent);
    std::cout << "[DDL] database storage errors propagate OK" << std::endl;
}

static void test_domain_storage_errors_are_not_success() {
    const std::string db = testDbPath("domain_storage_error");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    std::filesystem::create_directory(std::filesystem::path(db) / ".domains");

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(ddl.executeSql("CREATE DOMAIN broken_domain AS INT", s));

    cleanup(db);
    std::cout << "[DDL] domain storage errors propagate OK" << std::endl;
}

static void test_index_metadata_failures_are_not_success() {
    const std::string secidxDb = testDbPath("index_secidx_storage_error");
    cleanup(secidxDb);
    assert(g_engine.createDatabase(secidxDb, "utf8") == dbms::DBStatus::OK);
    Session secidxSession;
    setupSession(secidxSession, secidxDb);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", secidxSession));
    std::filesystem::create_directory(std::filesystem::path(secidxDb) / "t.secidx");
    assert(ddl.executeSql("CREATE INDEX t_id_idx ON t (id)", secidxSession));
    assert(g_engine.getIndexedColumns(secidxDb, "t").empty());
    cleanup(secidxDb);

    const std::string namesDb = testDbPath("index_names_storage_error");
    cleanup(namesDb);
    assert(g_engine.createDatabase(namesDb, "utf8") == dbms::DBStatus::OK);
    Session namesSession;
    setupSession(namesSession, namesDb);
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", namesSession));
    std::filesystem::create_directory(std::filesystem::path(namesDb) / "t.idxnames");
    assert(ddl.executeSql("CREATE INDEX t_id_idx ON t (id)", namesSession));
    assert(g_engine.getIndexedColumns(namesDb, "t").empty());
    assert(!g_engine.getNamedIndex(namesDb, "t", "t_id_idx").has_value());
    cleanup(namesDb);

    std::cout << "[DDL] index metadata storage errors propagate OK" << std::endl;
}

static void test_table_catalog_persistence_failure_rolls_back() {
    const std::string db = testDbPath("table_catalog_persist_error");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    const fs::path blockedTemporary =
        fs::path(db) / "pg_catalog" /
        ("pg_class.cat.tmp." +
         std::to_string(static_cast<unsigned long long>(::getpid())));
    fs::create_directories(blockedTemporary);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(ddl.executeSql("CREATE TABLE must_rollback (id INT)", s));
    assert(!g_engine.tableExists(db, "must_rollback"));

    fs::remove_all(blockedTemporary);
    g_engine.catalogService().evict(db);
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    assert(catalog.resolveRelation("must_rollback", {"public"}) == nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DDL] table catalog persistence failure rollback OK"
              << std::endl;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    test_create_drop_table();
    test_create_table_registers_in_catalog();
    test_create_table_requires_existing_schema();
    test_alter_table_rename_updates_catalog();
    test_alter_column_rename_updates_catalog();
    test_alter_column_definitions_update_catalog();
    test_alter_logged_state_updates_catalog();
    test_alter_rls_state_updates_catalog();
    test_check_constraint_count_updates_catalog();
    test_schema_qualified_rename_preserves_schema();
    test_create_index_sequence();
    test_table_index_flag_updates_catalog();
    test_drop_index_uses_sql_name();
    test_drop_schema_qualified_index();
    test_create_database_schema();
    test_drop_database_evicts_catalog();
    test_comment_on();
    test_alter_table_metadata_actions();
    test_schema_replica_identity_updates_catalog();
    test_long_identifiers_round_trip();
    test_database_storage_errors_are_not_success();
    test_domain_storage_errors_are_not_success();
    test_index_metadata_failures_are_not_success();
    test_table_catalog_persistence_failure_rolls_back();
    std::cout << "[DDL] all passed" << std::endl;
    return 0;
}
