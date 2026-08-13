#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/CatalogService.h"
#include "parser/parser.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_sequence_basic() {
    std::string db = testDbPath("seq_basic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE SEQUENCE s1 START 10 INCREMENT 2", s);
    assert(!err);
    assert(g_engine.sequenceExists(db, "s1"));

    assert(g_engine.nextval(db, "s1") == 10);
    assert(g_engine.nextval(db, "s1") == 12);
    assert(g_engine.currval(db, "s1") == 12);

    cleanup(db);
    std::cout << "[SEQUENCE] basic OK" << std::endl;
}

static void test_sequence_min_max_cycle() {
    std::string db = testDbPath("seq_cycle");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE SEQUENCE s1 START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 3 CYCLE", s);
    assert(!err);

    assert(g_engine.nextval(db, "s1") == 1);
    assert(g_engine.nextval(db, "s1") == 2);
    assert(g_engine.nextval(db, "s1") == 3);
    assert(g_engine.nextval(db, "s1") == 1); // cycle back to min

    cleanup(db);
    std::cout << "[SEQUENCE] min/max/cycle OK" << std::endl;
}

static void test_sequence_cache() {
    std::string db = testDbPath("seq_cache");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE SEQUENCE s1 START 1 INCREMENT 1 CACHE 5", s);
    assert(!err);

    for (int i = 0; i < 10; ++i) {
        assert(g_engine.nextval(db, "s1") == 1 + i);
    }

    cleanup(db);
    std::cout << "[SEQUENCE] cache OK" << std::endl;
}

static void test_sequence_alter() {
    std::string db = testDbPath("seq_alter");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE SEQUENCE s1 START 1 INCREMENT 1", s);
    assert(!err);
    assert(g_engine.nextval(db, "s1") == 1);

    err = ddl.executeSql("ALTER SEQUENCE s1 RESTART WITH 100 INCREMENT BY 10", s);
    assert(!err);
    assert(g_engine.nextval(db, "s1") == 100);
    assert(g_engine.nextval(db, "s1") == 110);

    cleanup(db);
    std::cout << "[SEQUENCE] alter OK" << std::endl;
}

static void test_sequence_rename() {
    std::string db = testDbPath("seq_rename");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE SEQUENCE s_old", s));
    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT DEFAULT nextval('s_old'), name VARCHAR(20))", s));
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* publicNamespace = catalog.findNamespaceByName("public");
    assert(publicNamespace != nullptr);
    const auto* oldRelation = catalog.findClassByName("s_old", publicNamespace->oid);
    assert(oldRelation != nullptr);
    const dbms::Oid sequenceOid = oldRelation->oid;

    dbms::SQLParser parser;
    const auto parsedRename = parser.parse("ALTER SEQUENCE s_old RENAME TO s_new");
    assert(parsedRename.success && parsedRename.stmt);
    assert(parsedRename.stmt->command == dbms::SqlCommand::AlterSequence);
    const auto* renameAst = dynamic_cast<const dbms::AlterObjectStmt*>(
        parsedRename.stmt.get());
    assert(renameAst != nullptr);
    assert(renameAst->subCommand == "RENAME TO s_new");

    assert(!ddl.executeSql("ALTER SEQUENCE s_old RENAME TO s_new", s));
    assert(!g_engine.sequenceExists(db, "s_old"));
    assert(g_engine.sequenceExists(db, "s_new"));
    assert(catalog.findClassByName("s_old", publicNamespace->oid) == nullptr);
    const auto* newRelation = catalog.findClassByName("s_new", publicNamespace->oid);
    assert(newRelation != nullptr && newRelation->oid == sequenceOid);

    const auto schema = g_engine.getTableSchema(db, "t");
    assert(schema.cols[0].defaultValue.find("s_new") != std::string::npos);
    assert(g_engine.insert(db, "t", {{"name", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"name", "b"}}) == dbms::DBStatus::OK);

    dbms::StorageEngine restarted;
    assert(restarted.sequenceExists(db, "s_new"));
    assert(!restarted.sequenceExists(db, "s_old"));
    const auto& restartedCatalog = restarted.catalogService().get(db);
    const auto* restartedPublic = restartedCatalog.findNamespaceByName("public");
    assert(restartedPublic != nullptr);
    const auto* restartedRelation = restartedCatalog.findClassByName(
        "s_new", restartedPublic->oid);
    assert(restartedRelation != nullptr && restartedRelation->oid == sequenceOid);
    assert(restarted.getTableSchema(db, "t").cols[0].defaultValue.find("s_new") !=
           std::string::npos);
    assert(restarted.nextval(db, "s_new") == 3);

    assert(!ddl.executeSql("CREATE SEQUENCE s_taken", s));
    assert(ddl.executeSql("ALTER SEQUENCE s_new RENAME TO s_taken", s));
    assert(g_engine.sequenceExists(db, "s_new"));
    assert(g_engine.sequenceExists(db, "s_taken"));
    cleanup(db);
    std::cout << "[SEQUENCE] rename/catalog/default/restart OK" << std::endl;
}

static void test_sequence_owned_by_drop_table() {
    std::string db = testDbPath("seq_owned");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY)", s);
    assert(!err);
    err = ddl.executeSql("CREATE SEQUENCE s1 OWNED BY t.id", s);
    assert(!err);
    assert(g_engine.sequenceExists(db, "s1"));

    dbms::CatalogManager& cat = g_engine.catalogService().get(db);
    const auto* seqRel = cat.resolveRelation("s1", {"public"});
    assert(seqRel != nullptr);

    err = ddl.executeSql("DROP TABLE t CASCADE", s);
    assert(!err);
    assert(!g_engine.sequenceExists(db, "s1"));

    cleanup(db);
    std::cout << "[SEQUENCE] owned by / drop table cascade OK" << std::endl;
}

static void test_sequence_identity_still_works() {
    std::string db = testDbPath("seq_identity");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    bool err = ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, msg VARCHAR(50))", s);
    assert(!err);
    assert(g_engine.insert(db, "t", {{"msg", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"msg", "b"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {}, {"id"});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[SEQUENCE] identity still works OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_sequence_basic();
    test_sequence_min_max_cycle();
    test_sequence_cache();
    test_sequence_alter();
    test_sequence_rename();
    test_sequence_owned_by_drop_table();
    test_sequence_identity_still_works();
    std::cout << "[SEQUENCE_FULL] all passed" << std::endl;
    return 0;
}
