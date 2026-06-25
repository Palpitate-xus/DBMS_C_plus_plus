// ============================================================================
// inet / cidr type test — Phase 4 Wave 4.8b
// Strict IPv4/IPv6 address parsing (rejecting bad octets, bad groups and
// out-of-range prefixes) with IPv6 storage now functional, plus UPDATE support.
// Output is the engine's canonical decoded form (full IPv6 groups).
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

static void test_inet_ipv4() {
    std::string db = "inet_v4";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INET, c CIDR)", s));

    // Host address (no prefix) and network with prefix round-trip.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","192.168.1.1"}, {"c","10.0.0.0/8"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "192.168.1.1");
    assert(fetchOne(db, "t", {"=id 1"}, "c") == "10.0.0.0/8");

    // inet with explicit prefix keeps it.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","172.16.5.4/24"}, {"c","192.168.0.0/16"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "a") == "172.16.5.4/24");

    cleanup(db);
    std::cout << "[INET] IPv4 round-trip OK" << std::endl;
}

static void test_inet_ipv6() {
    std::string db = "inet_v6";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INET)", s));

    // IPv6 is now stored (previously silently dropped to family 0). Output is
    // the full uncompressed group form.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","::1"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "0000:0000:0000:0000:0000:0000:0000:0001");

    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","2001:db8::1"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "a") == "2001:0db8:0000:0000:0000:0000:0000:0001");

    cleanup(db);
    std::cout << "[INET] IPv6 storage OK" << std::endl;
}

static void test_inet_invalid() {
    std::string db = "inet_bad";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INET)", s));

    // Octet out of range.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","256.1.1.1"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too few octets.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"a","192.168.1"}}) == dbms::DBStatus::INVALID_VALUE);
    // Prefix out of range for IPv4.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"a","192.168.1.1/33"}}) == dbms::DBStatus::INVALID_VALUE);
    // Non-numeric prefix.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"a","192.168.1.1/x"}}) == dbms::DBStatus::INVALID_VALUE);
    // Pure garbage.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"a","not-an-ip"}}) == dbms::DBStatus::INVALID_VALUE);
    // IPv6 group too long / double "::".
    assert(g_engine.insert(db, "t", {{"id","6"}, {"a","12345::"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","7"}, {"a","2001::db8::1"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[INET] invalid rejected OK" << std::endl;
}

static void test_inet_update() {
    std::string db = "inet_upd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, a INET)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"a","192.168.1.1"}}) == dbms::DBStatus::OK);
    // Valid update (previously the int-parse catch-all rejected all inet UPDATEs).
    assert(g_engine.update(db, "t", {{"a","10.1.2.3/24"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "10.1.2.3/24");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "t", {{"a","300.0.0.1"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "a") == "10.1.2.3/24");

    cleanup(db);
    std::cout << "[INET] update enforce/reject OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_inet_ipv4();
    test_inet_ipv6();
    test_inet_invalid();
    test_inet_update();
    std::cout << "[INET] all passed" << std::endl;
    return 0;
}
