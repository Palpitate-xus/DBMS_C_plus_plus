#include "commands/DdlExecutor.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

// Stubs for main.cpp helpers referenced by DdlExecutor (provided by tests/test_stubs.cpp)
extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_create_drop_table() {
    std::string db = "ddl_bridge_t1";
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
    std::string db = "ddl_bridge_t1_cat";
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
    std::string db = "ddl_bridge_t2";
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
    std::string db = "ddl_bridge_t3";
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

static void test_comment_on() {
    std::string db = "ddl_bridge_t4";
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

static void test_drop_database_evicts_catalog() {
    std::string db = "ddl_bridge_t5";
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
    dbms::TypeRegistry::instance().bootstrap();
    test_create_drop_table();
    test_create_table_registers_in_catalog();
    test_create_index_sequence();
    test_create_database_schema();
    test_drop_database_evicts_catalog();
    test_comment_on();
    std::cout << "[DDL] all passed" << std::endl;
    return 0;
}
