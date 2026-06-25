#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include <cassert>
#include <filesystem>
#include <iostream>

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

static void test_sequence_basic() {
    std::string db = "seq_basic";
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
    std::string db = "seq_cycle";
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
    std::string db = "seq_cache";
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
    std::string db = "seq_alter";
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

static void test_sequence_owned_by_drop_table() {
    std::string db = "seq_owned";
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
    std::string db = "seq_identity";
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
    test_sequence_owned_by_drop_table();
    test_sequence_identity_still_works();
    std::cout << "[SEQUENCE_FULL] all passed" << std::endl;
    return 0;
}
