// ============================================================================
// bytea type test — Phase 4 Wave 4.4
// bytea (mapped to dataType "blob") accepts PostgreSQL hex (\xDEADBEEF) and
// escape (literal bytes, \\ and \ooo octal) input, canonicalizes to lowercase
// hex "\xhh..", and rejects malformed input. INSERT and UPDATE both enforce it.
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

static void test_bytea_hex() {
    std::string db = "bytea_hex";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, b BYTEA)", s));

    // Uppercase hex normalizes to lowercase canonical form.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"b","\\xDEADBEEF"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "b") == "\\xdeadbeef");
    // Already-canonical input is idempotent.
    assert(g_engine.insert(db, "t", {{"id","2"}, {"b","\\xdeadbeef"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "b") == "\\xdeadbeef");
    // Empty hex payload is a valid zero-length bytea.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"b","\\x"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 3"}, "b") == "\\x");

    // Odd number of hex digits rejected.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"b","\\xABC"}}) == dbms::DBStatus::INVALID_VALUE);
    // Non-hex digit rejected.
    assert(g_engine.insert(db, "t", {{"id","5"}, {"b","\\xZZ"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[BYTEA] hex input OK" << std::endl;
}

static void test_bytea_escape() {
    std::string db = "bytea_esc";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, b BYTEA)", s));

    // Plain printable text is escape format: literal bytes -> hex.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"b","abc"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "b") == "\\x616263");
    // Octal escape \047 == 0x27 == "'".
    assert(g_engine.insert(db, "t", {{"id","2"}, {"b","\\047"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 2"}, "b") == "\\x27");
    // Doubled backslash is a single 0x5c byte.
    assert(g_engine.insert(db, "t", {{"id","3"}, {"b","\\\\"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 3"}, "b") == "\\x5c");

    // Lone trailing backslash is an invalid escape.
    assert(g_engine.insert(db, "t", {{"id","4"}, {"b","ab\\"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[BYTEA] escape input OK" << std::endl;
}

static void test_bytea_update() {
    std::string db = "bytea_upd";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, b BYTEA)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"b","\\xdeadbeef"}}) == dbms::DBStatus::OK);
    // Valid update (uppercase) canonicalizes.
    assert(g_engine.update(db, "t", {{"b","\\xCAFE"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "b") == "\\xcafe");
    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "t", {{"b","\\xGG"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "b") == "\\xcafe");

    cleanup(db);
    std::cout << "[BYTEA] update normalize/reject OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_bytea_hex();
    test_bytea_escape();
    test_bytea_update();
    std::cout << "[BYTEA] all passed" << std::endl;
    return 0;
}
