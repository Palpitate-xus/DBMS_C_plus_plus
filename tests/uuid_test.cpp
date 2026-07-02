// ============================================================================
// UUID type test — Phase 4 Wave 4.11
// Stored as canonical 36-char text. Accepts hyphenated, unhyphenated, braced
// and mixed-case input; validates 32 hex digits; rejects malformed input;
// canonicalizes to lowercase 8-4-4-4-12 on INSERT and UPDATE.
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

static void test_uuid_input_formats() {
    std::string db = testDbPath("uuid_fmt");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE u (id INT PRIMARY KEY, g UUID)", s));

    const std::string canon = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    // Canonical, uppercase, unhyphenated, and braced forms all normalize.
    assert(g_engine.insert(db, "u", {{"id","1"}, {"g","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "u", {{"id","2"}, {"g","A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "u", {{"id","3"}, {"g","a0eebc999c0b4ef8bb6d6bb9bd380a11"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "u", {{"id","4"}, {"g","{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"}}) == dbms::DBStatus::OK);
    for (int id = 1; id <= 4; ++id)
        assert(fetchOne(db, "u", {"=id " + std::to_string(id)}, "g") == canon);

    cleanup(db);
    std::cout << "[UUID] input formats normalize OK" << std::endl;
}

static void test_uuid_invalid() {
    std::string db = testDbPath("uuid_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE u (id INT PRIMARY KEY, g UUID)", s));

    // Non-hex digit.
    assert(g_engine.insert(db, "u", {{"id","1"}, {"g","z0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too few hex digits.
    assert(g_engine.insert(db, "u", {{"id","2"}, {"g","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too many hex digits.
    assert(g_engine.insert(db, "u", {{"id","3"}, {"g","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1122"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "u", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[UUID] invalid input rejected OK" << std::endl;
}

static void test_uuid_update() {
    std::string db = testDbPath("uuid_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE u (id INT PRIMARY KEY, g UUID)", s));

    assert(g_engine.insert(db, "u", {{"id","1"}, {"g","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}}) == dbms::DBStatus::OK);

    // Valid update (uppercase) canonicalizes.
    assert(g_engine.update(db, "u", {{"g","FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "u", {"=id 1"}, "g") == "ffffffff-ffff-ffff-ffff-ffffffffffff");

    // Invalid update rejected, row unchanged.
    assert(g_engine.update(db, "u", {{"g","not-a-uuid"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "u", {"=id 1"}, "g") == "ffffffff-ffff-ffff-ffff-ffffffffffff");

    cleanup(db);
    std::cout << "[UUID] update normalize/reject OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_uuid_input_formats();
    test_uuid_invalid();
    test_uuid_update();
    std::cout << "[UUID] all passed" << std::endl;
    return 0;
}
