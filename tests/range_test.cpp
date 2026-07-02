// ============================================================================
// Range type test — Phase 4 Wave 4.16
// int4range / int8range / numrange / daterange / tsrange: validate the literal
// grammar, enforce lower <= upper, canonicalize discrete integer ranges to the
// [) form, fold degenerate ranges to `empty`, and make infinite bounds
// exclusive. Malformed literals are rejected. INSERT and UPDATE both enforce it.
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

static std::string fetchOne(const std::string& db, const std::string& tbl,
                            const std::vector<std::string>& conds,
                            const std::string& col) {
    auto rows = g_engine.query(db, tbl, conds, {col}, {});
    assert(rows.size() == 1);
    std::string r = rows[0];
    while (!r.empty() && (r.back() == ' ' || r.back() == '\n')) r.pop_back();
    return r;
}

// Discrete integer ranges fold to canonical [) form.
static void test_int_range_canonical() {
    std::string db = testDbPath("range_int");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, r INT4RANGE)", s));

    // [1,10) already canonical.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"r","[1,10)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "r") == "[1,10)");
    // [1,10] -> [1,11)
    assert(g_engine.insert(db, "t", {{"id","2"}, {"r","[1,10]"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "r") == "[1,11)");
    // (1,10) -> [2,10)
    assert(g_engine.insert(db, "t", {{"id","3"}, {"r","(1,10)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 3"}, "r") == "[2,10)");
    // whitespace tolerated.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"r","[ 5 , 8 )"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 4"}, "r") == "[5,8)");
    // degenerate -> empty.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"r","[5,5)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 5"}, "r") == "empty");
    assert(g_engine.insert(db, "t", {{"id","6"}, {"r","empty"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 6"}, "r") == "empty");

    cleanup(db);
    std::cout << "[RANGE] int4range canonicalization OK" << std::endl;
}

// Infinite bounds become exclusive; both-infinite allowed.
static void test_infinite_bounds() {
    std::string db = testDbPath("range_inf");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, r INT8RANGE)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"r","[10,)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "r") == "[10,)");
    assert(g_engine.insert(db, "t", {{"id","2"}, {"r","[,100]"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "r") == "(,101)");
    assert(g_engine.insert(db, "t", {{"id","3"}, {"r","(,)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 3"}, "r") == "(,)");

    cleanup(db);
    std::cout << "[RANGE] infinite bounds OK" << std::endl;
}

// Continuous ranges keep bound inclusivity; numrange / daterange.
static void test_num_date_range() {
    std::string db = testDbPath("range_nd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, n NUMRANGE, d DATERANGE)", s));

    // numrange keeps inclusivity, trims whitespace.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"n","[1.5, 3.5)"}, {"d","[2020-01-01,2020-12-31)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "n") == "[1.5,3.5)");
    assert(fetchOne(db, "t", {"=id 1"}, "d") == "[2020-01-01,2020-12-31)");
    // numrange single inclusive point kept.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"n","[2,2]"}, {"d","[2021-06-01,2021-06-02)"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "n") == "[2,2]");

    cleanup(db);
    std::cout << "[RANGE] numrange/daterange OK" << std::endl;
}

// Malformed literals and lower>upper are rejected.
static void test_range_invalid() {
    std::string db = testDbPath("range_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, r INT4RANGE, d DATERANGE)", s));

    // lower > upper.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"r","[10,1)"}}) == dbms::DBStatus::INVALID_VALUE);
    // missing bracket.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"r","1,10)"}}) == dbms::DBStatus::INVALID_VALUE);
    // missing comma.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"r","[110)"}}) == dbms::DBStatus::INVALID_VALUE);
    // non-integer bound for int range.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"r","[1,x)"}}) == dbms::DBStatus::INVALID_VALUE);
    // invalid date bound.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"d","[2020-13-01,2020-12-31)"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[RANGE] invalid rejected OK" << std::endl;
}

static void test_range_update() {
    std::string db = testDbPath("range_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, r INT4RANGE)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"r","[1,10)"}}) == dbms::DBStatus::OK);
    // Valid update canonicalizes.
    assert(g_engine.update(db, "t", {{"r","(3,7]"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "r") == "[4,8)");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "t", {{"r","[9,2)"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "r") == "[4,8)");

    cleanup(db);
    std::cout << "[RANGE] update enforce/canonicalize OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_int_range_canonical();
    test_infinite_bounds();
    test_num_date_range();
    test_range_invalid();
    test_range_update();
    std::cout << "[RANGE] all passed" << std::endl;
    return 0;
}
