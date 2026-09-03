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

static std::string trimRight(const std::string& value) {
    const size_t end = value.find_last_not_of(" \t\n\r");
    return end == std::string::npos ? "" : value.substr(0, end + 1);
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

    // With no transaction boundary available, an initially-deferred CHECK
    // must still be enforced by the autocommit statement itself.
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"price", "0"}}) ==
           dbms::DBStatus::INVALID_VALUE);
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

    assert(g_engine.insert(db, "t", {{"id", "2"}, {"price", "10"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"price", "0"}}, {"=id 2"}) ==
           dbms::DBStatus::INVALID_VALUE);
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

static void test_deferred_check_uses_final_row() {
    std::string db = testDbPath("deferrable_t8");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0) DEFERRABLE INITIALLY DEFERRED)",
        s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"price", "10"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"price", "0"}}, {"=id 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"price", "20"}}, {"=id 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {"=id 1"}, {"price"});
    assert(rows.size() == 1 && trimRight(rows[0]) == "20");

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"price", "0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.remove(db, "t", {"=id 2"}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(g_engine.query(db, "t", {"=id 2"}, {"price"}).empty());

    cleanup(db);
    std::cout << "[DEFERRABLE] deferred check validates final row state OK"
              << std::endl;
}

static void test_ddl_implicit_commit_failure_is_propagated() {
    std::string db = testDbPath("deferrable_t7");
    std::string targetDb = testDbPath("implicit_commit_target");
    cleanup(db);
    cleanup(targetDb);

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

    // CREATE DATABASE has an implicit-commit boundary.  The deferred CHECK
    // must fail that boundary and prevent the database creation from running.
    assert(ddl.executeSql("CREATE DATABASE " + targetDb, s));
    assert(!g_engine.inTransaction());
    assert(!g_engine.databaseExists(targetDb));
    assert(g_engine.query(db, "t", {"=id 1"}, {"price"}).empty());

    cleanup(db);
    cleanup(targetDb);
    std::cout << "[DEFERRABLE] implicit DDL commit failure propagates OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_immediate_check_still_fails_at_insert();
    test_initially_deferred_check_blocks_commit();
    test_initially_deferred_check_allows_valid_commit();
    test_set_constraints_immediate_via_engine();
    test_set_constraints_all_deferred_via_engine();
    test_deferred_check_on_update();
    test_deferred_check_uses_final_row();
    test_ddl_implicit_commit_failure_is_propagated();
    std::cout << "[DEFERRABLE] all passed" << std::endl;
    return 0;
}
