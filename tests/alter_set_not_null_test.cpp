// ============================================================================
// ALTER TABLE ALTER COLUMN SET NOT NULL validation test — Phase 4 Wave 4.27d
// SET NOT NULL now rejects the change when an existing row has NULL in the
// column (PostgreSQL behavior); succeeds when all rows are non-NULL. DROP NOT
// NULL always succeeds.
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

static bool colNotNull(const dbms::TableSchema& tbl, const std::string& col) {
    for (size_t i = 0; i < tbl.len; ++i)
        if (tbl.cols[i].dataName == col) return !tbl.cols[i].isNull;
    return false;
}

static void test_set_not_null_ok() {
    std::string db = testDbPath("snn_ok");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, code VARCHAR(10))", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"code", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"code", "b"}}) == dbms::DBStatus::OK);

    // All rows non-NULL -> SET NOT NULL succeeds.
    assert(g_engine.alterTableSetNotNull(db, "t", "code") == dbms::DBStatus::OK);
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    assert(colNotNull(tbl, "code"));

    cleanup(db);
    std::cout << "[SETNN] set-not-null success OK" << std::endl;
}

static void test_set_not_null_rejected() {
    std::string db = testDbPath("snn_rej");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, code VARCHAR(10))", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"code", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}}) == dbms::DBStatus::OK);  // code NULL

    // A NULL value blocks SET NOT NULL; the column stays nullable.
    assert(g_engine.alterTableSetNotNull(db, "t", "code") == dbms::DBStatus::INVALID_VALUE);
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    assert(!colNotNull(tbl, "code"));

    cleanup(db);
    std::cout << "[SETNN] set-not-null rejection OK" << std::endl;
}

static void test_drop_then_set() {
    std::string db = testDbPath("snn_cycle");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, code VARCHAR(10))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"code", "x"}}) == dbms::DBStatus::OK);

    assert(g_engine.alterTableSetNotNull(db, "t", "code") == dbms::DBStatus::OK);
    assert(colNotNull(g_engine.getTableSchema(db, "t"), "code"));
    // DROP NOT NULL always succeeds.
    assert(g_engine.alterTableDropNotNull(db, "t", "code") == dbms::DBStatus::OK);
    assert(!colNotNull(g_engine.getTableSchema(db, "t"), "code"));
    // Unknown column rejected.
    assert(g_engine.alterTableSetNotNull(db, "t", "ghost") == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[SETNN] drop/set cycle OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_set_not_null_ok();
    test_set_not_null_rejected();
    test_drop_then_set();
    std::cout << "[SETNN] all passed" << std::endl;
    return 0;
}
