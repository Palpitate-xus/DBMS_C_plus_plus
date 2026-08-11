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
#include <string>
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
    assert(!ddl.executeSql("CREATE TABLE drop_idx_tbl (id INT, value INT)", s));
    assert(g_engine.insert(db, "drop_idx_tbl", {{"id", "1"}, {"value", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "drop_idx_tbl", {{"id", "2"}, {"value", "20"}}) == dbms::DBStatus::OK);
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

    // Standard PostgreSQL access-method syntax must use the real specialized
    // StorageEngine implementation, not the former B-tree compatibility path.
    assert(!ddl.executeSql("CREATE INDEX idx_gin ON drop_idx_tbl USING GIN (value)", s));
    assert(!ddl.executeSql("CREATE INDEX idx_gist ON drop_idx_tbl (id) USING GiST", s));
    assert(!ddl.executeSql("CREATE INDEX idx_brin ON drop_idx_tbl USING BRIN (id)", s));
    assert(!ddl.executeSql("CREATE INDEX idx_spgist ON drop_idx_tbl USING SPGIST (id)", s));
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

static void test_comment_on() {
    std::string db = testDbPath("ddl_bridge_t4");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    ddl.executeSql("CREATE TABLE cmt_tbl (id INT)", s);
    bool err = ddl.executeSql("COMMENT ON TABLE cmt_tbl IS 'a test table'", s);
    assert(!err);

    err = ddl.executeSql("COMMENT ON COLUMN cmt_tbl.id IS 'primary key'", s);
    assert(!err);

    cleanup(db);
    std::cout << "[DDL] comment OK" << std::endl;
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

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    test_create_drop_table();
    test_create_table_registers_in_catalog();
    test_create_index_sequence();
    test_drop_index_uses_sql_name();
    test_create_database_schema();
    test_drop_database_evicts_catalog();
    test_comment_on();
    test_alter_table_metadata_actions();
    test_long_identifiers_round_trip();
    std::cout << "[DDL] all passed" << std::endl;
    return 0;
}
