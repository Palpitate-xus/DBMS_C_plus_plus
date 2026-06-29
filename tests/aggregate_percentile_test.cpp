// ============================================================================
// PERCENTILE_CONT / PERCENTILE_DISC aggregate test — Phase 4 Wave 4.20
// percentile_cont: continuous percentile with linear interpolation
// percentile_disc: discrete percentile returning an actual value from the set
// Syntax: aggregate("percentile_cont", "0.5,col") → median
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include <cassert>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) {
    if (fs::exists(db)) fs::remove_all(db);
    std::string wal = db + "_wal";
    if (fs::exists(wal)) fs::remove_all(wal);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::string aggResult(const std::string& db, const std::string& tbl,
                              const std::string& func, const std::string& arg) {
    std::vector<dbms::StorageEngine::AggItem> items = {{func, arg, {}}};
    auto res = g_engine.aggregate(db, tbl, {}, items);
    if (res.empty()) return "";
    std::string s = res[0];
    while (!s.empty() && s.back() == ' ') s.pop_back();
    return s;
}

// ----- Test 1: percentile_cont(0.5) = median of {10,20,30,40,50} = 30 -----
static void test_pct_cont_odd() {
    std::string db = "pct_cont_odd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, v INT)", s));
    for (int i = 1; i <= 5; i++)
        assert(g_engine.insert(db, "t", {{"id", std::to_string(i)}, {"v", std::to_string(i * 10)}}) == dbms::DBStatus::OK);

    // percentile_cont(0.5) with 5 values: idx = 0.5 * 4 = 2.0 → exact value at index 2 = 30
    assert(aggResult(db, "t", "percentile_cont", "0.5,v") == "30");
    cleanup(db);
    std::cout << "[PCTILE] cont(0.5) odd count → 30 OK" << std::endl;
}

// ----- Test 2: percentile_cont(0.5) = median of {10,20,30,40} = 25 (interpolated) -----
static void test_pct_cont_even() {
    std::string db = "pct_cont_even";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, v INT)", s));
    for (int i = 1; i <= 4; i++)
        assert(g_engine.insert(db, "t", {{"id", std::to_string(i)}, {"v", std::to_string(i * 10)}}) == dbms::DBStatus::OK);

    // percentile_cont(0.5) with 4 values: idx = 0.5 * 3 = 1.5 → interpolate(20,30) = 25
    assert(aggResult(db, "t", "percentile_cont", "0.5,v") == "25");
    cleanup(db);
    std::cout << "[PCTILE] cont(0.5) even count → 25 OK" << std::endl;
}

// ----- Test 3: percentile_disc(0.5) = discrete median of {10,20,30,40} = 20 -----
static void test_pct_disc_even() {
    std::string db = "pct_disc_even";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, v INT)", s));
    for (int i = 1; i <= 4; i++)
        assert(g_engine.insert(db, "t", {{"id", std::to_string(i)}, {"v", std::to_string(i * 10)}}) == dbms::DBStatus::OK);

    // percentile_disc(0.5) with 4 values: ceil(0.5*4)=2, idx=1 → 20
    assert(aggResult(db, "t", "percentile_disc", "0.5,v") == "20");
    cleanup(db);
    std::cout << "[PCTILE] disc(0.5) even count → 20 OK" << std::endl;
}

// ----- Test 4: percentile_cont(0.25) = Q1 of {10,20,30,40,50} -----
static void test_pct_cont_q1() {
    std::string db = "pct_cont_q1";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, v INT)", s));
    for (int i = 1; i <= 5; i++)
        assert(g_engine.insert(db, "t", {{"id", std::to_string(i)}, {"v", std::to_string(i * 10)}}) == dbms::DBStatus::OK);

    // percentile_cont(0.25) with 5 values: idx = 0.25 * 4 = 1.0 → exact value at index 1 = 20
    assert(aggResult(db, "t", "percentile_cont", "0.25,v") == "20");
    cleanup(db);
    std::cout << "[PCTILE] cont(0.25) Q1 → 20 OK" << std::endl;
}

// ----- Test 5: percentile_cont(1.0) = max -----
static void test_pct_cont_max() {
    std::string db = "pct_cont_max";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, v INT)", s));
    for (int i = 1; i <= 5; i++)
        assert(g_engine.insert(db, "t", {{"id", std::to_string(i)}, {"v", std::to_string(i * 10)}}) == dbms::DBStatus::OK);

    // percentile_cont(1.0) → idx = 1.0 * 4 = 4.0 → value at index 4 = 50
    assert(aggResult(db, "t", "percentile_cont", "1.0,v") == "50");
    cleanup(db);
    std::cout << "[PCTILE] cont(1.0) → max 50 OK" << std::endl;
}

// ----- Test 6: empty table → NULL -----
static void test_pct_empty() {
    std::string db = "pct_empty";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, v INT)", s));

    assert(aggResult(db, "t", "percentile_cont", "0.5,v") == "NULL");
    assert(aggResult(db, "t", "percentile_disc", "0.5,v") == "NULL");
    cleanup(db);
    std::cout << "[PCTILE] empty table → NULL OK" << std::endl;
}

// ----- Test 7: single row -----
static void test_pct_single() {
    std::string db = "pct_single";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, v INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"v", "42"}}) == dbms::DBStatus::OK);

    assert(aggResult(db, "t", "percentile_cont", "0.5,v") == "42");
    assert(aggResult(db, "t", "percentile_disc", "0.5,v") == "42");
    cleanup(db);
    std::cout << "[PCTILE] single row → 42 OK" << std::endl;
}

int main() {
    test_pct_cont_odd();
    test_pct_cont_even();
    test_pct_disc_even();
    test_pct_cont_q1();
    test_pct_cont_max();
    test_pct_empty();
    test_pct_single();
    std::cout << "=== All PERCENTILE tests PASSED ===" << std::endl;
    return 0;
}
