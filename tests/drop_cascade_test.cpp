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

static void test_drop_table_restrict_removes_automatic_dependents() {
    const std::string db = testDbPath("drop_automatic_dependents");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE owner (id INT)", s));
    assert(!ddl.executeSql("CREATE INDEX owner_id_idx ON owner (id)", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE owner_id_seq OWNED BY owner.id", s));

    auto& catalog = g_engine.catalogService().get(db);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace != nullptr);
    assert(catalog.findClassByName(
               "owner_id_idx", publicNamespace->oid) != nullptr);
    assert(catalog.findClassByName(
               "owner_id_seq", publicNamespace->oid) != nullptr);

    // Neither an index nor an OWNED BY sequence should force users to spell
    // CASCADE when dropping their owning table.
    assert(!ddl.executeSql("DROP TABLE owner", s));
    assert(!g_engine.tableExists(db, "owner"));
    assert(!g_engine.getNamedIndex(db, "owner", "owner_id_idx"));
    assert(!g_engine.sequenceExists(db, "owner_id_seq"));
    assert(catalog.findClassByName(
               "owner_id_idx", publicNamespace->oid) == nullptr);
    assert(catalog.findClassByName(
               "owner_id_seq", publicNamespace->oid) == nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] RESTRICT drops automatic dependents OK"
              << std::endl;
}

static void test_owned_sequence_defaults_obey_drop_behavior() {
    const std::string db = testDbPath("drop_owned_sequence_defaults");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE TABLE app.owner (id INT, keep INT)", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE app.owner_id_seq OWNED BY app.owner.id", s));
    assert(!ddl.executeSql(
        "ALTER TABLE app.owner ALTER COLUMN id "
        "SET DEFAULT nextval('app.owner_id_seq')", s));
    assert(!ddl.executeSql(
        "CREATE TABLE sequence_consumer "
        "(value INT DEFAULT nextval('app.owner_id_seq'))", s));
    s.sequenceLastValues["app.owner_id_seq"] = 41;

    // The owner's own default disappears with the table, but a default on a
    // surviving table is a normal dependency and must block RESTRICT.
    assert(ddl.executeSql("DROP TABLE app.owner RESTRICT", s));
    assert(g_engine.tableExists(db, "app__owner"));
    assert(g_engine.sequenceExists(db, "app.owner_id_seq"));
    assert(!g_engine.getTableSchema(
        db, "sequence_consumer").cols[0].defaultValue.empty());
    assert(s.sequenceLastValues["app.owner_id_seq"] == 41);

    assert(!ddl.executeSql("DROP TABLE app.owner CASCADE", s));
    assert(!g_engine.tableExists(db, "app__owner"));
    assert(!g_engine.sequenceExists(db, "app.owner_id_seq"));
    assert(g_engine.getTableSchema(
        db, "sequence_consumer").cols[0].defaultValue.empty());
    assert(s.sequenceLastValues.count("app.owner_id_seq") == 0);

    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        const auto* publicNamespace =
            durable.findNamespaceByName("public");
        const auto* appNamespace = durable.findNamespaceByName("app");
        assert(publicNamespace && appNamespace);
        assert(durable.findClassByName(
                   "owner", appNamespace->oid) == nullptr);
        assert(durable.findClassByName(
                   "owner_id_seq", appNamespace->oid) == nullptr);
        const auto* consumer = durable.findClassByName(
            "sequence_consumer", publicNamespace->oid);
        const auto* column = consumer
            ? durable.findAttribute(consumer->oid, "value") : nullptr;
        assert(column && !column->atthasdef);
    }

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] owned sequence defaults obey behavior OK"
              << std::endl;
}

static void test_drop_schema_cascade_removes_relation_storage() {
    const std::string db = testDbPath("drop_schema_relations");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", s));
    assert(!ddl.executeSql(
        "CREATE TABLE app.items (id INT, value INT)", s));
    assert(!ddl.executeSql(
        "CREATE INDEX items_value_idx ON app.items (value)", s));
    assert(!ddl.executeSql(
        "CREATE SEQUENCE app.items_id_seq OWNED BY app.items.id", s));
    assert(!ddl.executeSql(
        "ALTER TABLE app.items ALTER COLUMN id "
        "SET DEFAULT nextval('app.items_id_seq')", s));
    assert(!ddl.executeSql(
        "CREATE VIEW app.items_view AS SELECT * FROM app.items", s));
    assert(!ddl.executeSql(
        "CREATE MATERIALIZED VIEW app.items_mv "
        "AS SELECT id, value FROM app.items", s));
    assert(!ddl.executeSql(
        "CREATE VIEW public_items_view AS SELECT * FROM app.items", s));
    assert(!ddl.executeSql(
        "CREATE TABLE public_sequence_consumer "
        "(id INT DEFAULT nextval('app.items_id_seq'))", s));
    s.sequenceLastValues["app.items_id_seq"] = 52;

    assert(ddl.executeSql("DROP SCHEMA app RESTRICT", s));
    assert(g_engine.schemaExists(db, "app"));
    assert(g_engine.tableExists(db, "app__items"));
    assert(g_engine.sequenceExists(db, "app.items_id_seq"));
    assert(g_engine.viewExists(db, "app.items_view"));
    assert(g_engine.isMaterializedView(db, "app.items_mv"));
    assert(g_engine.viewExists(db, "public_items_view"));
    assert(!g_engine.getTableSchema(
        db, "public_sequence_consumer").cols[0].defaultValue.empty());

    assert(!ddl.executeSql("DROP SCHEMA app CASCADE", s));
    assert(!g_engine.schemaExists(db, "app"));
    assert(!g_engine.tableExists(db, "app__items"));
    assert(!g_engine.getNamedIndex(
        db, "app__items", "items_value_idx"));
    assert(!g_engine.sequenceExists(db, "app.items_id_seq"));
    assert(!g_engine.viewExists(db, "app.items_view"));
    assert(!g_engine.isMaterializedView(db, "app.items_mv"));
    assert(!g_engine.tableExists(
        db, dbms::StorageEngine::materializedViewPrefix("app.items_mv")));
    assert(!g_engine.viewExists(db, "public_items_view"));
    assert(g_engine.getTableSchema(
        db, "public_sequence_consumer").cols[0].defaultValue.empty());
    assert(s.sequenceLastValues.count("app.items_id_seq") == 0);

    {
        dbms::CatalogManager durable(
            (fs::path(g_engine.dbPath(db)) / "pg_catalog").string());
        assert(durable.findNamespaceByName("app") == nullptr);
        const auto* publicNamespace =
            durable.findNamespaceByName("public");
        assert(publicNamespace);
        assert(durable.findClassByName(
                   "public_items_view", publicNamespace->oid) == nullptr);
        const auto* consumer = durable.findClassByName(
            "public_sequence_consumer", publicNamespace->oid);
        const auto* column = consumer
            ? durable.findAttribute(consumer->oid, "id") : nullptr;
        assert(column && !column->atthasdef);
    }

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] schema CASCADE removes relation storage OK"
              << std::endl;
}

static void test_drop_schema_catalog_preflight_fails_closed() {
    const std::string db = testDbPath("drop_schema_catalog_preflight");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA guarded", s));
    assert(!ddl.executeSql("CREATE TABLE guarded.items (id INT)", s));

    // Force CatalogManager construction to throw. DROP SCHEMA must not treat
    // an unavailable dependency catalog as an empty one and remove the
    // namespace marker anyway.
    g_engine.catalogService().evict(db);
    const fs::path catalogPath = fs::path(db) / "pg_catalog";
    const fs::path savedCatalogPath = fs::path(db) / "pg_catalog.saved";
    fs::rename(catalogPath, savedCatalogPath);
    {
        std::ofstream blocker(catalogPath);
        assert(blocker.good());
    }

    assert(ddl.executeSql("DROP SCHEMA guarded CASCADE", s));
    assert(g_engine.schemaExists(db, "guarded"));
    assert(g_engine.tableExists(db, "guarded__items"));

    fs::remove(catalogPath);
    fs::rename(savedCatalogPath, catalogPath);
    auto& reloadedCatalog = g_engine.catalogService().get(db);
    assert(reloadedCatalog.findNamespaceByName("guarded") != nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] schema catalog preflight fails closed OK"
              << std::endl;
}

static void test_drop_schema_auxiliary_preflight_fails_closed() {
    const std::string db = testDbPath("drop_schema_aux_preflight");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app_aux", s));

    const fs::path shellPath = fs::path(db) / ".shell_types";
    {
        std::ofstream duplicateShell(shellPath, std::ios::binary);
        duplicateShell << "app_aux.value\napp_aux.value\n";
        assert(duplicateShell.good());
    }
    assert(ddl.executeSql("DROP SCHEMA app_aux CASCADE", s));
    assert(g_engine.schemaExists(db, "app_aux"));

    {
        std::ofstream validShell(shellPath, std::ios::binary | std::ios::trunc);
        validShell << "app_aux.value\n";
        assert(validShell.good());
        std::ofstream corruptUdt(
            fs::path(db) / ".udt_meta", std::ios::binary);
        corruptUdt << "malformed record\n";
        assert(corruptUdt.good());
    }
    assert(ddl.executeSql("DROP SCHEMA app_aux CASCADE", s));
    assert(g_engine.schemaExists(db, "app_aux"));

    fs::remove(fs::path(db) / ".udt_meta");
    assert(!ddl.executeSql("DROP SCHEMA app_aux CASCADE", s));
    assert(!g_engine.schemaExists(db, "app_aux"));

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] auxiliary preflight fails closed OK"
              << std::endl;
}

static bool lineFileContains(const fs::path& path,
                             const std::string& expected) {
    std::ifstream input(path);
    std::string line;
    while (std::getline(input, line)) {
        if (line == expected ||
            line.find("|" + expected + "|") != std::string::npos) {
            return true;
        }
        if (line.rfind("DBMS_UDT_V2:", 0) == 0) {
            static constexpr char hex[] = "0123456789abcdef";
            std::string encoded;
            encoded.reserve(expected.size() * 2);
            for (unsigned char byte : expected) {
                encoded.push_back(hex[byte >> 4]);
                encoded.push_back(hex[byte & 0x0f]);
            }
            if (line.find("|" + encoded + "|") != std::string::npos) {
                return true;
            }
        }
    }
    return false;
}

static void test_drop_schema_handles_auxiliary_objects() {
    const std::string db = testDbPath("drop_schema_auxiliary");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app_aux", s));
    assert(!ddl.executeSql("CREATE SCHEMA app_auxiliary", s));

    dbms::StorageEngine::DomainInfo domain;
    domain.name = "app_aux.positive";
    domain.baseType = "int";
    assert(g_engine.createDomain(db, domain) == dbms::DBStatus::OK);
    dbms::StorageEngine::CompositeType composite;
    composite.name = "app_aux.coordinate";
    composite.fields = {{"x", "int"}, {"y", "int"}};
    assert(g_engine.createCompositeType(db, composite) == dbms::DBStatus::OK);
    dbms::StorageEngine::EnumType enumeration;
    enumeration.name = "app_aux.mood";
    enumeration.labels = {"ok", "sad"};
    assert(g_engine.createEnumType(db, enumeration) == dbms::DBStatus::OK);
    assert(g_engine.createUDF(db, "app_aux.bump", "x", "x + 1") ==
           dbms::DBStatus::OK);
    assert(g_engine.createTVF(
               db, "app_aux.rows", "x", "SELECT x") ==
           dbms::DBStatus::OK);
    assert(g_engine.createProcedure(
               db, "app_aux.work", {}, {"SELECT 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createCollation(
               db, "app_aux__locale", "libc", "C") ==
           dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TYPE app_aux.shell_value", s));
    assert(!ddl.executeSql(
        "CREATE TYPE app_aux.int_range AS RANGE (subtype = int4)", s));
    assert(lineFileContains(
        fs::path(db) / ".shell_types", "app_aux.shell_value"));
    assert(lineFileContains(
        fs::path(db) / ".udt_meta", "app_aux.int_range"));

    // A longer neighboring schema name must not match either namespace
    // delimiter used by the current storage formats.
    dbms::StorageEngine::DomainInfo survivorDomain = domain;
    survivorDomain.name = "app_auxiliary.positive";
    assert(g_engine.createDomain(db, survivorDomain) == dbms::DBStatus::OK);
    assert(g_engine.createUDF(
               db, "app_auxiliary.bump", "x", "x + 2") ==
           dbms::DBStatus::OK);
    assert(g_engine.createCollation(
               db, "app_auxiliary__locale", "libc", "C") ==
           dbms::DBStatus::OK);

    assert(ddl.executeSql("DROP SCHEMA app_aux RESTRICT", s));
    assert(g_engine.schemaExists(db, "app_aux"));
    assert(!g_engine.getDomain(db, domain.name).name.empty());
    assert(g_engine.udfExists(db, "app_aux.bump"));
    assert(g_engine.procedureExists(db, "app_aux.work"));

    assert(!ddl.executeSql("DROP SCHEMA app_aux CASCADE", s));
    assert(!g_engine.schemaExists(db, "app_aux"));
    assert(g_engine.getDomain(db, domain.name).name.empty());
    assert(!g_engine.isCompositeType(db, composite.name));
    assert(g_engine.getEnumType(db, enumeration.name).name.empty());
    assert(!g_engine.udfExists(db, "app_aux.bump"));
    assert(!g_engine.tvfExists(db, "app_aux.rows"));
    assert(!g_engine.procedureExists(db, "app_aux.work"));
    const auto collations = g_engine.getCollationNames(db);
    assert(std::find(collations.begin(), collations.end(),
                     "app_aux__locale") == collations.end());
    assert(!lineFileContains(
        fs::path(db) / ".shell_types", "app_aux.shell_value"));
    assert(!lineFileContains(
        fs::path(db) / ".udt_meta", "app_aux.int_range"));

    assert(!g_engine.getDomain(db, survivorDomain.name).name.empty());
    assert(g_engine.udfExists(db, "app_auxiliary.bump"));
    assert(std::find(collations.begin(), collations.end(),
                     "app_auxiliary__locale") != collations.end());

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] schema handles auxiliary objects OK"
              << std::endl;
}

static void test_drop_schema_validates_both_catalogs() {
    const std::string db = testDbPath("drop_schema_existence");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("DROP SCHEMA IF EXISTS never_created", s));
    assert(ddl.executeSql("DROP SCHEMA never_created", s));

    assert(!ddl.executeSql("CREATE SCHEMA inconsistent", s));
    auto& catalog = g_engine.catalogService().get(db);
    const auto* namespaceRow =
        catalog.findNamespaceByName("inconsistent");
    assert(namespaceRow != nullptr);
    const auto plan = catalog.planDrop(
        dbms::PgClassOid_Namespace, namespaceRow->oid,
        dbms::CatalogManager::DropBehavior::Restrict);
    assert(plan.ok());
    assert(catalog.applyDropPlan(plan));
    assert(catalog.persistAll());

    // A marker without its dependency catalog is corruption, not an empty
    // schema. Refuse to erase the remaining evidence even with IF EXISTS.
    assert(ddl.executeSql(
        "DROP SCHEMA IF EXISTS inconsistent CASCADE", s));
    assert(g_engine.schemaExists(db, "inconsistent"));

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] schema existence checks both catalogs OK"
              << std::endl;
}

static void test_multi_schema_drop_fails_before_mutation() {
    const std::string db = testDbPath("drop_multiple_schemas");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA first_schema", s));
    assert(!ddl.executeSql("CREATE SCHEMA second_schema", s));

    assert(ddl.executeSql(
        "DROP SCHEMA first_schema, second_schema CASCADE", s));
    assert(g_engine.schemaExists(db, "first_schema"));
    assert(g_engine.schemaExists(db, "second_schema"));
    auto& catalog = g_engine.catalogService().get(db);
    assert(catalog.findNamespaceByName("first_schema") != nullptr);
    assert(catalog.findNamespaceByName("second_schema") != nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] multi-schema DROP fails before mutation OK"
              << std::endl;
}

static void test_multi_table_drop_is_atomic() {
    const std::string db = testDbPath("drop_multiple_preflight");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE first_table (id INT)", s));
    assert(!ddl.executeSql("CREATE TABLE second_table (id INT)", s));
    assert(g_engine.insert(db, "first_table", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "second_table", {{"id", "2"}}) == dbms::DBStatus::OK);
    assert(ddl.executeSql("DROP TABLE first_table, missing_table", s));
    assert(g_engine.tableExists(db, "first_table"));
    assert(g_engine.tableExists(db, "second_table"));
    assert(g_engine.query(db, "first_table", {}, {"id"}) == std::vector<std::string>{"1 "});
    assert(g_engine.query(db, "second_table", {}, {"id"}) == std::vector<std::string>{"2 "});
    assert(!ddl.executeSql("DROP TABLE first_table, second_table", s));
    assert(!g_engine.tableExists(db, "first_table"));
    assert(!g_engine.tableExists(db, "second_table"));

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[DROP-CASCADE] multi-target DROP is atomic OK"
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
    test_drop_table_restrict_removes_automatic_dependents();
    test_owned_sequence_defaults_obey_drop_behavior();
    test_drop_schema_cascade_removes_relation_storage();
    test_drop_schema_catalog_preflight_fails_closed();
    test_drop_schema_auxiliary_preflight_fails_closed();
    test_drop_schema_handles_auxiliary_objects();
    test_drop_schema_validates_both_catalogs();
    test_multi_schema_drop_fails_before_mutation();
    test_multi_table_drop_is_atomic();
    test_drop_removes_named_table_sidecars();
    test_drop_purges_authorization_state();
    std::cout << "[DROP-CASCADE] all passed" << std::endl;
    return 0;
}
