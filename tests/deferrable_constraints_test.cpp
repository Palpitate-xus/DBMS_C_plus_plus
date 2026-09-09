// ============================================================================
// deferrable_constraints_test — DEFERRABLE on UNIQUE, FOREIGN KEY and EXCLUDE:
//   UNIQUE ... DEFERRABLE INITIALLY DEFERRED: key swaps inside a transaction
//   commit cleanly; unresolved duplicates abort the commit.
//   FK ... DEFERRABLE INITIALLY DEFERRED: child-before-parent inserts commit
//   when the parent appears before COMMIT; a missing parent aborts it.
//   SET CONSTRAINTS ALL IMMEDIATE restores immediate checking.
// Driven through the engine API (beginTransaction / insert / commit).
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include "test_utils.h"

using namespace dbms;

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& d) {
    if (std::filesystem::exists(d)) std::filesystem::remove_all(d);
}

static void setupSession(Session& s, const std::string& d) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = d;
}

static std::string db;

static void setup() {
    db = testDbPath("deferrable_ck");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE parent (id INT PRIMARY KEY)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE child ("
        "  id INT PRIMARY KEY, "
        "  pid INT, "
        "  CONSTRAINT child_pid_fkey FOREIGN KEY (pid) REFERENCES parent(id) "
        "  DEFERRABLE INITIALLY DEFERRED)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE composite_parent ("
        "  a INT, b INT, PRIMARY KEY (a, b))", s));
    assert(!ddl.executeSql(
        "CREATE TABLE composite_child ("
        "  id INT PRIMARY KEY, a INT, b INT, "
        "  CONSTRAINT composite_child_fkey FOREIGN KEY (a, b) "
        "  REFERENCES composite_parent(a, b) "
        "  DEFERRABLE INITIALLY DEFERRED)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE uniq ("
        "  id INT PRIMARY KEY, "
        "  tag VARCHAR(20), "
        "  CONSTRAINT uniq_tag_key UNIQUE (tag) DEFERRABLE INITIALLY DEFERRED)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE immediate_uniq ("
        "  id INT PRIMARY KEY, "
        "  tag VARCHAR(20) UNIQUE)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE immediate_composite ("
        "  id INT PRIMARY KEY, a INT, b INT, UNIQUE (a, b))", s));
    assert(!ddl.executeSql(
        "CREATE TABLE excl ("
        "  id INT PRIMARY KEY, "
        "  rng INT, "
        "  CONSTRAINT excl_rng_key EXCLUDE (rng WITH =) "
        "  DEFERRABLE INITIALLY DEFERRED)", s));
    // Constraint metadata must be recorded for the deferred paths.
    assert(g_engine.isConstraintCurrentlyDeferred(db, "child", "child_pid_fkey"));
    assert(g_engine.isConstraintCurrentlyDeferred(
        db, "composite_child", "composite_child_fkey"));
    assert(g_engine.isConstraintCurrentlyDeferred(db, "uniq", "uniq_tag_key"));
    assert(g_engine.isConstraintCurrentlyDeferred(db, "excl", "excl_rng_key"));
}

static void test_exclude_deferred_violation() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    // Both inserts queue deferred EXCLUDE re-checks; the values conflict.
    assert(g_engine.insert(db, "excl", {{"id", "1"}, {"rng", "100"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "excl", {{"id", "2"}, {"rng", "100"}}) == DBStatus::OK);
    // Conflict never resolved -> COMMIT must fail.
    assert(g_engine.commitTransaction() == DBStatus::EXCLUSION_VIOLATION);
    std::cout << "[DEFER] EXCLUDE deferred conflicting commit rejected OK" << std::endl;
}

static void test_exclude_deferred_resolution() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "excl", {{"id", "3"}, {"rng", "300"}}) == DBStatus::OK);
    // Move out of the way before commit: row 3 no longer conflicts with 1.
    assert(g_engine.update(db, "excl", {{"rng", "301"}}, {"=id 3"}) == DBStatus::OK);
    assert(g_engine.insert(db, "excl", {{"id", "4"}, {"rng", "300"}}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    std::cout << "[DEFER] EXCLUDE deferred resolved-in-txn commit OK" << std::endl;
}

static void test_exclude_immediate_rejects() {
    // The autocommit statement supplies the commit point for the deferred
    // exclusion check and reports the constraint class precisely.
    assert(g_engine.insert(db, "excl", {{"id", "5"}, {"rng", "300"}})
               == DBStatus::EXCLUSION_VIOLATION);
    std::cout << "[DEFER] EXCLUDE autocommit conflict rejected OK" << std::endl;
}

static void test_fk_deferred_ok() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    // Child first: the immediate check is skipped, the check is queued.
    assert(g_engine.insert(db, "child", {{"id", "1"}, {"pid", "100"}}) == DBStatus::OK);
    // Parent appears later in the same transaction.
    assert(g_engine.insert(db, "parent", {{"id", "100"}}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    std::cout << "[DEFER] FK deferred insert-then-parent commit OK" << std::endl;
}

static void test_fk_deferred_violation() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "child", {{"id", "2"}, {"pid", "200"}}) == DBStatus::OK);
    // Parent never appears -> COMMIT must fail.
    assert(g_engine.commitTransaction() == DBStatus::FOREIGN_KEY_VIOLATION);
    std::cout << "[DEFER] FK deferred missing-parent commit rejected OK" << std::endl;
}

static void test_fk_autocommit_rejects_missing_parent() {
    assert(g_engine.insert(db, "child", {{"id", "3"}, {"pid", "300"}})
               == DBStatus::FOREIGN_KEY_VIOLATION);
    std::cout << "[DEFER] FK autocommit missing parent rejected OK" << std::endl;
}

static void test_fk_deferred_update() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.update(db, "child", {{"pid", "400"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.insert(db, "parent", {{"id", "400"}}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.update(db, "child", {{"pid", "401"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::FOREIGN_KEY_VIOLATION);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.update(db, "child", {{"pid", "999"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.update(db, "child", {{"pid", "400"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "child", {{"id", "4"}, {"pid", "999"}}) ==
           DBStatus::OK);
    assert(g_engine.remove(db, "child", {"=id 4"}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    std::cout << "[DEFER] FK deferred updates rechecked at commit OK" << std::endl;
}

static void test_composite_fk_deferred() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(
               db, "composite_child", {{"id", "1"}, {"a", "10"}, {"b", "20"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "composite_parent", {{"a", "10"}, {"b", "20"}}) ==
           DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.update(
               db, "composite_child", {{"b", "30"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.insert(
               db, "composite_parent", {{"a", "10"}, {"b", "30"}}) ==
           DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.update(
               db, "composite_child", {{"b", "40"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::FOREIGN_KEY_VIOLATION);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.update(
               db, "composite_child", {{"b", "40"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.update(
               db, "composite_child", {{"b", "30"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    std::cout << "[DEFER] composite FK insert/update commit checks OK" << std::endl;
}

static void test_unique_deferred_single_insert() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "uniq", {{"id", "20"}, {"tag", "solo"}})
               == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    std::cout << "[DEFER] UNIQUE deferred non-conflicting commit OK" << std::endl;
}

static void test_unique_autocommit_rejects_duplicate() {
    assert(g_engine.insert(db, "uniq", {{"id", "21"}, {"tag", "autocommit"}})
               == DBStatus::OK);
    assert(g_engine.insert(db, "uniq", {{"id", "22"}, {"tag", "autocommit"}})
               == DBStatus::UNIQUE_VIOLATION);
    std::cout << "[DEFER] UNIQUE autocommit duplicate rejected OK" << std::endl;
}

static void test_unique_deferred_swap() {
    assert(g_engine.insert(db, "uniq", {{"id", "1"}, {"tag", "a"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "uniq", {{"id", "2"}, {"tag", "b"}}) == DBStatus::OK);
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    // Move row 1 onto b's value: queued rather than rejected, then row 2
    // moves away before commit.
    assert(g_engine.update(db, "uniq", {{"tag", "b"}}, {"=id 1"}) == DBStatus::OK);
    assert(g_engine.update(db, "uniq", {{"tag", "c"}}, {"=id 2"}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    std::cout << "[DEFER] UNIQUE deferred key swap commit OK" << std::endl;
}

static void test_unique_deferred_update_violation() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.update(db, "uniq", {{"tag", "c"}}, {"=id 1"})
               == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::UNIQUE_VIOLATION);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.update(db, "uniq", {{"tag", "c"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.update(db, "uniq", {{"tag", "fixed"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "uniq", {{"id", "23"}, {"tag", "c"}}) ==
           DBStatus::OK);
    assert(g_engine.remove(db, "uniq", {"=id 23"}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    std::cout << "[DEFER] UNIQUE deferred conflicting update rejected OK"
              << std::endl;
}

static void test_unique_deferred_violation() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "uniq", {{"id", "10"}, {"tag", "dup"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "uniq", {{"id", "11"}, {"tag", "dup"}}) == DBStatus::OK);
    // Duplicate never resolved -> COMMIT must fail.
    assert(g_engine.commitTransaction() == DBStatus::UNIQUE_VIOLATION);
    std::cout << "[DEFER] UNIQUE deferred duplicate commit rejected OK" << std::endl;
}

static void test_immediate_unique_updates() {
    assert(g_engine.insert(
               db, "immediate_uniq", {{"id", "1"}, {"tag", "a"}})
               == DBStatus::OK);
    assert(g_engine.insert(
               db, "immediate_uniq", {{"id", "2"}, {"tag", "b"}})
               == DBStatus::OK);
    assert(g_engine.update(
               db, "immediate_uniq", {{"tag", "a"}}, {"=id 2"})
               == DBStatus::DUPLICATE_KEY);
    assert(g_engine.update(
               db, "immediate_uniq", {{"id", "1"}}, {"=id 2"})
               == DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               db, "immediate_composite",
               {{"id", "1"}, {"a", "10"}, {"b", "20"}})
               == DBStatus::OK);
    assert(g_engine.insert(
               db, "immediate_composite",
               {{"id", "2"}, {"a", "10"}, {"b", "30"}})
               == DBStatus::OK);
    assert(g_engine.update(
               db, "immediate_composite", {{"b", "20"}}, {"=id 2"})
               == DBStatus::DUPLICATE_KEY);
    std::cout << "[DEFER] immediate UNIQUE, composite UNIQUE and PRIMARY KEY updates reject duplicates OK"
              << std::endl;
}

static void test_set_constraints_immediate() {
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    g_engine.setConstraintMode({"all"}, false); // SET CONSTRAINTS ALL IMMEDIATE
    // FK check is immediate again: missing parent must fail the insert.
    assert(g_engine.insert(db, "child", {{"id", "50"}, {"pid", "500"}})
               == DBStatus::INVALID_VALUE);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    std::cout << "[DEFER] SET CONSTRAINTS ALL IMMEDIATE restores checks OK" << std::endl;
}

int main() {
    setup();
    test_exclude_deferred_violation();
    test_exclude_deferred_resolution();
    test_exclude_immediate_rejects();
    test_fk_deferred_ok();
    test_fk_deferred_violation();
    test_fk_autocommit_rejects_missing_parent();
    test_fk_deferred_update();
    test_composite_fk_deferred();
    test_unique_deferred_single_insert();
    test_unique_autocommit_rejects_duplicate();
    test_unique_deferred_swap();
    test_immediate_unique_updates();
    test_unique_deferred_update_violation();
    test_unique_deferred_violation();
    test_set_constraints_immediate();
    std::cout << "[DEFER] all tests passed" << std::endl;
    return 0;
}
