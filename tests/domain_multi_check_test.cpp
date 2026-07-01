// ============================================================================
// Domain multi-constraint test — Phase 4 Wave 4.17
// Verifies that domains support multiple CHECK constraints with named/unnamed
// constraint combinations.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>

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

// Domain with two CHECK constraints: value must be positive AND < 1000.
static void test_domain_multi_check() {
    std::string db = "dom_multi_chk";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE DOMAIN price_t AS INT "
        "CONSTRAINT positive CHECK (VALUE > 0) "
        "CHECK (VALUE < 1000)", s));

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, p price_t)", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"p", "50"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"p", "0"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"p", "-5"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "4"}, {"p", "1000"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "5"}, {"p", "999"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {}, {"id", "p"});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[DOMAIN_MULTI] two CHECK constraints OK" << std::endl;
}

// Domain with three CHECK constraints.
static void test_domain_three_checks() {
    std::string db = "dom_three_chk";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE DOMAIN rating AS INT "
        "CHECK (VALUE >= 1) CHECK (VALUE <= 5) "
        "CONSTRAINT odd_only CHECK (VALUE % 2 = 1)", s));

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, r rating)", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"r", "3"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"r", "2"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"r", "0"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "4"}, {"r", "6"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id", "5"}, {"r", "5"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {}, {"id", "r"});
    assert(rows.size() == 2);

    cleanup(db);
    std::cout << "[DOMAIN_MULTI] three CHECK constraints OK" << std::endl;
}

// Domain with single CHECK still works (regression).
static void test_domain_single_check() {
    std::string db = "dom_single_chk";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE DOMAIN pos_int AS INT CHECK (VALUE > 0)", s));

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, x pos_int)", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"x", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"x", "0"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[DOMAIN_MULTI] single CHECK regression OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_domain_multi_check();
    test_domain_three_checks();
    test_domain_single_check();
    std::cout << "[DOMAIN_MULTI] all passed" << std::endl;
    return 0;
}
