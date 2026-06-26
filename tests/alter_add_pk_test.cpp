// ============================================================================
// ALTER TABLE ADD PRIMARY KEY test — Phase 4 Wave 4.27a
// Covers StorageEngine::alterTableAddPrimaryKey: success path (schema records
// pkColIndices + per-column isPrimaryKey/NOT NULL), and rejection on existing
// duplicate values, NULL values, a pre-existing primary key, and unknown
// columns.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (fs::exists(db)) fs::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static bool colIsPk(const dbms::TableSchema& tbl, const std::string& col) {
    for (size_t i = 0; i < tbl.len; ++i)
        if (tbl.cols[i].dataName == col) return tbl.cols[i].isPrimaryKey;
    return false;
}

static void test_add_pk_success() {
    std::string db = "addpk_ok";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "b"}}) == dbms::DBStatus::OK);

    auto res = g_engine.alterTableAddPrimaryKey(db, "t", "t_pkey", {"id"});
    assert(res == dbms::DBStatus::OK);

    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    assert(tbl.pkColIndices.size() == 1);
    assert(colIsPk(tbl, "id"));

    // After the PK exists, a duplicate insert is rejected.
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "dup"}}) != dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[ADDPK] success path OK" << std::endl;
}

static void test_add_pk_duplicate_data() {
    std::string db = "addpk_dup";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "b"}}) == dbms::DBStatus::OK);  // dup id

    auto res = g_engine.alterTableAddPrimaryKey(db, "t", "t_pkey", {"id"});
    assert(res == dbms::DBStatus::INVALID_VALUE);

    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    assert(tbl.pkColIndices.empty());  // unchanged

    cleanup(db);
    std::cout << "[ADDPK] duplicate-data rejection OK" << std::endl;
}

static void test_add_pk_null_data() {
    std::string db = "addpk_null";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    // PK candidate is a variable-length column so a NULL round-trips to "".
    assert(!ddl.executeSql("CREATE TABLE t (code VARCHAR(10), n INT)", s));

    // Row with NULL code (omitted).
    assert(g_engine.insert(db, "t", {{"n", "1"}}) == dbms::DBStatus::OK);

    auto res = g_engine.alterTableAddPrimaryKey(db, "t", "t_pkey", {"code"});
    assert(res == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[ADDPK] NULL-data rejection OK" << std::endl;
}

static void test_add_pk_rejections() {
    std::string db = "addpk_rej";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))", s));

    // Table already has a PK -> reject.
    assert(g_engine.alterTableAddPrimaryKey(db, "t", "pk2", {"name"}) == dbms::DBStatus::INVALID_VALUE);

    // Unknown column on a PK-less table -> reject.
    assert(!ddl.executeSql("CREATE TABLE u (a INT, b INT)", s));
    assert(g_engine.alterTableAddPrimaryKey(db, "u", "u_pk", {"nope"}) == dbms::DBStatus::INVALID_VALUE);

    // Missing table -> TABLE_NOT_FOUND.
    assert(g_engine.alterTableAddPrimaryKey(db, "ghost", "g_pk", {"a"}) == dbms::DBStatus::TABLE_NOT_FOUND);

    cleanup(db);
    std::cout << "[ADDPK] rejection paths OK" << std::endl;
}

static void test_add_pk_composite() {
    std::string db = "addpk_comp";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (a INT, b INT, c VARCHAR(10))", s));

    // (1,1) and (1,2) share a, but the (a,b) tuples are distinct.
    assert(g_engine.insert(db, "t", {{"a", "1"}, {"b", "1"}, {"c", "x"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"a", "1"}, {"b", "2"}, {"c", "y"}}) == dbms::DBStatus::OK);

    assert(g_engine.alterTableAddPrimaryKey(db, "t", "t_pk", {"a", "b"}) == dbms::DBStatus::OK);
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    assert(tbl.pkColIndices.size() == 2);

    cleanup(db);
    std::cout << "[ADDPK] composite PK OK" << std::endl;
}

static void test_drop_pk() {
    std::string db = "droppk";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "a"}}) == dbms::DBStatus::OK);
    // PK enforced before drop.
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "dup"}}) != dbms::DBStatus::OK);

    // Drop the PK (constraint name is not persisted; any name drops it).
    assert(g_engine.alterTableDropConstraint(db, "t", "t_pkey") == dbms::DBStatus::OK);
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    assert(tbl.pkColIndices.empty());
    assert(!colIsPk(tbl, "id"));

    // After the drop, a duplicate id is now allowed.
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "dup"}}) == dbms::DBStatus::OK);

    // Dropping again (no PK, no matching constraint) -> INVALID_VALUE.
    assert(g_engine.alterTableDropConstraint(db, "t", "nope") == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[ADDPK] drop PK OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_add_pk_success();
    test_add_pk_duplicate_data();
    test_add_pk_null_data();
    test_add_pk_rejections();
    test_add_pk_composite();
    test_drop_pk();
    std::cout << "[ADDPK] all passed" << std::endl;
    return 0;
}
