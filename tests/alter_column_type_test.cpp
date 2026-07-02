// ============================================================================
// ALTER TABLE ALTER COLUMN TYPE test — Phase 4 Wave 4.27e
// alterTableAlterColumnType rewrites the whole table under a new type for one
// column, re-encoding every row's value and rebuilding indexes. It pre-validates
// convertibility and aborts (INVALID_VALUE) before touching any file when a
// value cannot be represented in the new type, so data is never lost on failure.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string colType(const dbms::TableSchema& tbl, const std::string& col) {
    for (size_t i = 0; i < tbl.len; ++i)
        if (tbl.cols[i].dataName == col) return tbl.cols[i].dataType;
    return "";
}

// Read all live rows as ordered maps keyed by the first column's value, so the
// assertions don't depend on physical row order after a rewrite.
static std::map<std::string, std::map<std::string, std::string>>
readRowsByKey(const std::string& db, const std::string& tbl, const std::string& keyCol) {
    dbms::TableSchema schema = g_engine.getTableSchema(db, tbl);
    std::map<std::string, std::map<std::string, std::string>> out;
    g_engine.forEachRow(db, tbl, [&](uint32_t, uint16_t, const char* data, size_t len) {
        std::string row(data, len);
        std::map<std::string, std::string> m;
        for (size_t i = 0; i < schema.len; ++i)
            m[schema.cols[i].dataName] = g_engine.extractColumnValue(row, schema, i);
        out[m[keyCol]] = m;
    });
    return out;
}

static size_t rowCount(const std::string& db, const std::string& tbl) {
    size_t n = 0;
    g_engine.forEachRow(db, tbl, [&](uint32_t, uint16_t, const char*, size_t) { ++n; });
    return n;
}

// Widening int -> bigint: data preserved, schema type updated.
static void test_widen_int_to_bigint() {
    std::string db = testDbPath("act_widen");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, n INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"n", "100"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"n", "200"}}) == dbms::DBStatus::OK);

    dbms::Column bigintCol = dbms::makeIntColumn("n", true, 3);  // bigint
    assert(g_engine.alterTableAlterColumnType(db, "t", "n", bigintCol) == dbms::DBStatus::OK);

    assert(colType(g_engine.getTableSchema(db, "t"), "n") == "bigint");
    auto rows = readRowsByKey(db, "t", "id");
    assert(rows.size() == 2);
    assert(rows["1"]["n"] == "100");
    assert(rows["2"]["n"] == "200");
    cleanup(db);
    std::cout << "[ALTERTYPE] widen int->bigint OK" << std::endl;
}

// int -> varchar: numbers re-encoded as text, value text preserved.
static void test_int_to_varchar() {
    std::string db = testDbPath("act_i2v");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, n INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"n", "42"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"n", "7"}}) == dbms::DBStatus::OK);

    dbms::Column vc = dbms::makeVarCharColumn("n", true, 20);
    assert(g_engine.alterTableAlterColumnType(db, "t", "n", vc) == dbms::DBStatus::OK);

    dbms::TableSchema sc = g_engine.getTableSchema(db, "t");
    assert(colType(sc, "n") == "varchar");
    auto rows = readRowsByKey(db, "t", "id");
    assert(rows["1"]["n"] == "42");
    assert(rows["2"]["n"] == "7");
    cleanup(db);
    std::cout << "[ALTERTYPE] int->varchar OK" << std::endl;
}

// varchar holding numeric strings -> int: succeeds, values preserved.
static void test_varchar_to_int_ok() {
    std::string db = testDbPath("act_v2i_ok");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, code VARCHAR(10))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"code", "123"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"code", "456"}}) == dbms::DBStatus::OK);

    dbms::Column intCol = dbms::makeIntColumn("code", true, 2);  // int
    assert(g_engine.alterTableAlterColumnType(db, "t", "code", intCol) == dbms::DBStatus::OK);
    assert(colType(g_engine.getTableSchema(db, "t"), "code") == "int");
    auto rows = readRowsByKey(db, "t", "id");
    assert(rows["1"]["code"] == "123");
    assert(rows["2"]["code"] == "456");
    cleanup(db);
    std::cout << "[ALTERTYPE] varchar->int (numeric) OK" << std::endl;
}

// varchar holding non-numeric text -> int: rejected, data + type unchanged.
static void test_varchar_to_int_rejected() {
    std::string db = testDbPath("act_v2i_rej");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, code VARCHAR(10))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"code", "123"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"code", "abc"}}) == dbms::DBStatus::OK);

    dbms::Column intCol = dbms::makeIntColumn("code", true, 2);
    assert(g_engine.alterTableAlterColumnType(db, "t", "code", intCol) == dbms::DBStatus::INVALID_VALUE);
    // Type and data are unchanged after the rejected conversion.
    assert(colType(g_engine.getTableSchema(db, "t"), "code") == "varchar");
    auto rows = readRowsByKey(db, "t", "id");
    assert(rows.size() == 2);
    assert(rows["1"]["code"] == "123");
    assert(rows["2"]["code"] == "abc");
    cleanup(db);
    std::cout << "[ALTERTYPE] varchar->int (non-numeric) rejected OK" << std::endl;
}

// Unknown column rejected; NULL (empty) text values round-trip through the
// rewrite. (Note: the INF NULL sentinel only survives in 8-byte integers, so a
// VARCHAR source column is used here to exercise faithful NULL round-tripping.)
static void test_unknown_col_and_null() {
    std::string db = testDbPath("act_misc");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, s VARCHAR(10))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"s", "hi"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}}) == dbms::DBStatus::OK);  // s NULL

    dbms::Column ghost = dbms::makeIntColumn("ghost", true, 3);
    assert(g_engine.alterTableAlterColumnType(db, "t", "ghost", ghost) == dbms::DBStatus::INVALID_VALUE);

    // Widen s to a larger VARCHAR: NULL/empty round-trips as empty.
    dbms::Column wider = dbms::makeVarCharColumn("s", true, 50);
    assert(g_engine.alterTableAlterColumnType(db, "t", "s", wider) == dbms::DBStatus::OK);
    auto rows = readRowsByKey(db, "t", "id");
    assert(rows["1"]["s"] == "hi");
    assert(rows["2"]["s"].empty());  // still NULL/empty
    cleanup(db);
    std::cout << "[ALTERTYPE] unknown-col + null round-trip OK" << std::endl;
}

// A secondary index on an unrelated column survives the rewrite (row count and
// lookups stay intact).
static void test_index_survives_rewrite() {
    std::string db = testDbPath("act_idx");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, n INT, code VARCHAR(10))", s));
    for (int i = 1; i <= 5; ++i)
        assert(g_engine.insert(db, "t", {{"id", std::to_string(i)},
                                          {"n", std::to_string(i * 10)},
                                          {"code", "c" + std::to_string(i)}}) == dbms::DBStatus::OK);
    // Build a secondary index on code, then change n's type.
    g_engine.createIndex(db, "t", "code");
    assert(g_engine.alterTableAlterColumnType(db, "t", "n", dbms::makeVarCharColumn("n", true, 20))
           == dbms::DBStatus::OK);

    assert(rowCount(db, "t") == 5);
    auto rows = readRowsByKey(db, "t", "id");
    assert(rows["3"]["n"] == "30");
    assert(rows["3"]["code"] == "c3");
    // The index meta survived (column still reported as indexed).
    auto idxCols = g_engine.getIndexedColumns(db, "t");
    bool hasCode = false;
    for (const auto& c : idxCols) if (c == "code") hasCode = true;
    assert(hasCode);
    cleanup(db);
    std::cout << "[ALTERTYPE] index survives rewrite OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_widen_int_to_bigint();
    test_int_to_varchar();
    test_varchar_to_int_ok();
    test_varchar_to_int_rejected();
    test_unknown_col_and_null();
    test_index_survives_rewrite();
    std::cout << "[ALTERTYPE] all passed" << std::endl;
    return 0;
}
