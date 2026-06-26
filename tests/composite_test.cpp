// ============================================================================
// Composite type test — Phase 4 Wave 4.15
// A column whose type is a user CREATE TYPE ... AS (...) composite is now
// recognized (previously fell back to varchar). Its "(v1,v2,...)" row literal is
// validated against the type's field count and numeric field types, and
// canonicalized (whitespace trimmed, fields quoted only where needed).
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

static void test_composite_column() {
    std::string db = "comp_col";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE addr AS (street varchar(50), zip int)", s));
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a addr)", s));

    // Column is recognized as the composite type (not silently varchar).
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    bool found = false;
    for (size_t i = 0; i < tbl.len; ++i) {
        if (tbl.cols[i].dataName == "a") { found = true; assert(tbl.cols[i].dataType == "addr"); }
    }
    assert(found);

    // Valid row literal, whitespace trimmed.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","( main st , 12345 )"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "(main st,12345)");
    // NULL field (empty).
    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","(,99)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "a") == "(,99)");
    // Field needing quotes (contains comma) preserved.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"a","(\"a, b\",7)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 3"}, "a") == "(\"a, b\",7)");

    // Wrong field count rejected.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"a","(only one)"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","5"}, {"a","(a,1,extra)"}}) == dbms::DBStatus::INVALID_VALUE);
    // Non-integer in the int field rejected.
    assert(g_engine.insert(db, "t", {{"id","6"}, {"a","(street,notanumber)"}}) == dbms::DBStatus::INVALID_VALUE);
    // Missing parentheses rejected.
    assert(g_engine.insert(db, "t", {{"id","7"}, {"a","main st,12345"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[COMPOSITE] column validation OK" << std::endl;
}

static void test_composite_update() {
    std::string db = "comp_upd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TYPE pt AS (x int, y int)", s));
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, p pt)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"p","(1,2)"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"p","( 3 , 4 )"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "p") == "(3,4)");
    assert(g_engine.update(db, "t", {{"p","(3,x)"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "p") == "(3,4)");

    cleanup(db);
    std::cout << "[COMPOSITE] update enforce OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_composite_column();
    test_composite_update();
    std::cout << "[COMPOSITE] all passed" << std::endl;
    return 0;
}
