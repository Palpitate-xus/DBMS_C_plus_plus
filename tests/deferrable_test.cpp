#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
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

static void test_immediate_check_still_fails_at_insert() {
    std::string db = testDbPath("deferrable_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0))",
        s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "0"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "10"}}) == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[DEFERRABLE] immediate check rejects bad insert OK" << std::endl;
}

static void test_initially_deferred_check_blocks_commit() {
    std::string db = testDbPath("deferrable_t2");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0) DEFERRABLE INITIALLY DEFERRED)",
        s));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "0"}}) == dbms::DBStatus::OK);
    // Commit should fail because deferred CHECK catches the violation
    assert(g_engine.commitTransaction() == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"price"});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[DEFERRABLE] initially deferred check blocks commit OK" << std::endl;
}

static void test_initially_deferred_check_allows_valid_commit() {
    std::string db = testDbPath("deferrable_t3");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0) DEFERRABLE INITIALLY DEFERRED)",
        s));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"price"});
    assert(rows.size() == 1);

    cleanup(db);
    std::cout << "[DEFERRABLE] initially deferred check allows valid commit OK" << std::endl;
}

static void test_set_constraints_immediate_via_engine() {
    std::string db = testDbPath("deferrable_t4");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0) DEFERRABLE INITIALLY DEFERRED)",
        s));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    g_engine.setConstraintMode({"chk_price"}, false);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "0"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[DEFERRABLE] SET CONSTRAINTS IMMEDIATE via engine OK" << std::endl;
}

static void test_set_constraints_all_deferred_via_engine() {
    std::string db = testDbPath("deferrable_t5");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0) NOT DEFERRABLE)",
        s));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    g_engine.setConstraintMode({"all"}, true);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "0"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[DEFERRABLE] SET CONSTRAINTS ALL DEFERRED respects NOT DEFERRABLE OK" << std::endl;
}

static void test_deferred_check_on_update() {
    std::string db = testDbPath("deferrable_t6");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0) DEFERRABLE INITIALLY DEFERRED)",
        s));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"price", "0"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"price"});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[DEFERRABLE] deferred check on update blocks commit OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_immediate_check_still_fails_at_insert();
    test_initially_deferred_check_blocks_commit();
    test_initially_deferred_check_allows_valid_commit();
    test_set_constraints_immediate_via_engine();
    test_set_constraints_all_deferred_via_engine();
    test_deferred_check_on_update();
    std::cout << "[DEFERRABLE] all passed" << std::endl;
    return 0;
}
