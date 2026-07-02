// ============================================================================
// ALTER TABLE RENAME CONSTRAINT test — Phase 4 Wave 4.27c
// Covers StorageEngine::alterTableRenameConstraint across CHECK, UNIQUE, and
// FOREIGN KEY constraints, plus not-found and name-collision rejection.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
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

static bool hasCheckName(const dbms::TableSchema& tbl, const std::string& n) {
    for (size_t i = 0; i < tbl.len; ++i)
        if (tbl.cols[i].checkConstraintName == n) return true;
    return false;
}
static bool hasUniqueName(const dbms::TableSchema& tbl, const std::string& n) {
    for (const auto& un : tbl.uniqueConstraintNames) if (un == n) return true;
    return false;
}
static bool hasFkName(const dbms::TableSchema& tbl, const std::string& n) {
    for (size_t i = 0; i < tbl.fkLen; ++i) if (tbl.fks[i].name == n) return true;
    return false;
}

static void test_rename_check_unique() {
    std::string db = testDbPath("rencon");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, age INT)", s));

    assert(g_engine.alterTableAddCheckConstraint(db, "t", "chk_age", "age > 0") == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddUniqueConstraint(db, "t", "uq_id", {"id"}) == dbms::DBStatus::OK);

    // Rename CHECK.
    assert(g_engine.alterTableRenameConstraint(db, "t", "chk_age", "chk_age_v2") == dbms::DBStatus::OK);
    {
        dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
        assert(hasCheckName(tbl, "chk_age_v2"));
        assert(!hasCheckName(tbl, "chk_age"));
    }
    // Rename UNIQUE.
    assert(g_engine.alterTableRenameConstraint(db, "t", "uq_id", "uq_id_v2") == dbms::DBStatus::OK);
    {
        dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
        assert(hasUniqueName(tbl, "uq_id_v2"));
        assert(!hasUniqueName(tbl, "uq_id"));
    }
    cleanup(db);
    std::cout << "[RENCON] check/unique rename OK" << std::endl;
}

static void test_rename_fk() {
    std::string db = testDbPath("rencon_fk");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE parent (pid INT PRIMARY KEY)", s));
    assert(!ddl.executeSql("CREATE TABLE child (cid INT, pref INT)", s));

    assert(g_engine.alterTableAddFKConstraint(db, "child", "fk_pref", {"pref"},
                                              "parent", {"pid"}) == dbms::DBStatus::OK);
    assert(g_engine.alterTableRenameConstraint(db, "child", "fk_pref", "fk_pref_v2") == dbms::DBStatus::OK);
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "child");
    assert(hasFkName(tbl, "fk_pref_v2"));
    assert(!hasFkName(tbl, "fk_pref"));
    cleanup(db);
    std::cout << "[RENCON] fk rename OK" << std::endl;
}

static void test_rename_rejections() {
    std::string db = testDbPath("rencon_rej");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, age INT)", s));
    assert(g_engine.alterTableAddCheckConstraint(db, "t", "chk_age", "age > 0") == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddUniqueConstraint(db, "t", "uq_id", {"id"}) == dbms::DBStatus::OK);

    // Unknown constraint -> INVALID_VALUE.
    assert(g_engine.alterTableRenameConstraint(db, "t", "ghost", "x") == dbms::DBStatus::INVALID_VALUE);
    // New name already in use -> TABLE_ALREADY_EXISTS.
    assert(g_engine.alterTableRenameConstraint(db, "t", "chk_age", "uq_id") == dbms::DBStatus::TABLE_ALREADY_EXISTS);
    // Missing table -> TABLE_NOT_FOUND.
    assert(g_engine.alterTableRenameConstraint(db, "ghost", "a", "b") == dbms::DBStatus::TABLE_NOT_FOUND);

    cleanup(db);
    std::cout << "[RENCON] rejection paths OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_rename_check_unique();
    test_rename_fk();
    test_rename_rejections();
    std::cout << "[RENCON] all passed" << std::endl;
    return 0;
}
