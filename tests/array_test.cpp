// ============================================================================
// Array type test — Phase 4 Wave 4.14
// Fixes array columns being silently downgraded to scalars (TypeRegistry
// validateColumn overwrote the array flag), and adds array-literal validation:
// brace structure, rectangular multidimensional shape, per-element type checks
// for numeric element types, NULL elements, quoting, and whitespace
// canonicalization. INSERT and UPDATE both enforce it.
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

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string fetchOne(const std::string& db, const std::string& tbl,
                            const std::vector<std::string>& conds,
                            const std::string& col) {
    auto rows = g_engine.query(db, tbl, conds, {col}, {});
    assert(rows.size() == 1);
    std::string r = rows[0];
    while (!r.empty() && (r.back() == ' ' || r.back() == '\n')) r.pop_back();
    return r;
}

// Array columns are now recognized (regression: they were created as scalars).
static void test_array_recognized() {
    std::string db = "arr_reg";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INT[], b VARCHAR(20)[])", s));

    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == "a" || tbl.cols[i].dataName == "b") {
            assert(tbl.cols[i].isArray);
            assert(tbl.cols[i].isVariableLength);
        }
    }
    cleanup(db);
    std::cout << "[ARRAY] columns recognized OK" << std::endl;
}

// Integer-array literal validation and whitespace canonicalization.
static void test_int_array() {
    std::string db = "arr_int";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INT[])", s));

    // Whitespace collapses to canonical form.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","{ 1, 2 , 3 }"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "{1,2,3}");
    // Empty array.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","{}"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "a") == "{}");
    // NULL element preserved.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"a","{1,NULL,3}"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 3"}, "a") == "{1,NULL,3}");

    // Non-integer element rejected.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"a","{1,x,3}"}}) == dbms::DBStatus::INVALID_VALUE);
    // Unbalanced braces rejected.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"a","{1,2,3"}}) == dbms::DBStatus::INVALID_VALUE);
    // Missing braces rejected.
    assert(g_engine.insert(db, "t", {{"id","6"}, {"a","1,2,3"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[ARRAY] int array validation OK" << std::endl;
}

// Multidimensional arrays must be rectangular.
static void test_multidim() {
    std::string db = "arr_md";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INT[])", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","{{1,2},{3,4}}"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "{{1,2},{3,4}}");
    // Ragged dimensions rejected.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","{{1,2},{3}}"}}) == dbms::DBStatus::INVALID_VALUE);
    // Mixed scalar/sub-array rejected.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"a","{1,{2,3}}"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[ARRAY] multidimensional OK" << std::endl;
}

// Text arrays accept any element and quote those needing it.
static void test_text_array() {
    std::string db = "arr_txt";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a TEXT[])", s));

    // Unquoted simple words stay unquoted; quoted element with comma keeps quotes.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","{hello,world}"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "{hello,world}");
    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","{\"a,b\",c}"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "a") == "{\"a,b\",c}");

    cleanup(db);
    std::cout << "[ARRAY] text array OK" << std::endl;
}

static void test_array_update() {
    std::string db = "arr_upd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INT[])", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","{1,2,3}"}}) == dbms::DBStatus::OK);
    // Valid update canonicalizes.
    assert(g_engine.update(db, "t", {{"a","{ 4 , 5 }"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "{4,5}");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "t", {{"a","{4,bad}"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "{4,5}");

    cleanup(db);
    std::cout << "[ARRAY] update enforce/canonicalize OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_array_recognized();
    test_int_array();
    test_multidim();
    test_text_array();
    test_array_update();
    std::cout << "[ARRAY] all passed" << std::endl;
    return 0;
}
