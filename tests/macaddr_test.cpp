// ============================================================================
// MAC address type test — Phase 4 Wave 4.8 (macaddr / macaddr8)
// Verifies binary fixed-width storage with normalization on INSERT/UPDATE,
// canonical lowercase colon output on read, and rejection of malformed input.
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

// Fetch the single value of column `col` for the (only) matching row.
static std::string fetchOne(const std::string& db, const std::string& tbl,
                            const std::vector<std::string>& conds,
                            const std::string& col) {
    auto rows = g_engine.query(db, tbl, conds, {col}, {});
    assert(rows.size() == 1);
    std::string r = rows[0];
    while (!r.empty() && (r.back() == ' ' || r.back() == '\n')) r.pop_back();
    return r;
}

// macaddr accepts colon / hyphen / dot / bare-hex forms; all canonicalize to
// lowercase colon-separated output.
static void test_macaddr_input_formats() {
    std::string db = testDbPath("macaddr_fmt");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, a macaddr)", s));

    // id 1..5 carry the same address in five accepted notations.
    assert(g_engine.insert(db, "m", {{"id", "1"}, {"a", "08:00:2b:01:02:03"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "m", {{"id", "2"}, {"a", "08-00-2b-01-02-03"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "m", {{"id", "3"}, {"a", "0800.2b01.0203"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "m", {{"id", "4"}, {"a", "08002b010203"}}) == dbms::DBStatus::OK);
    // Mixed case input normalizes to lowercase.
    assert(g_engine.insert(db, "m", {{"id", "5"}, {"a", "08:00:2B:01:02:03"}}) == dbms::DBStatus::OK);

    const std::string canon = "08:00:2b:01:02:03";
    for (int id = 1; id <= 5; ++id) {
        std::string got = fetchOne(db, "m", {"=id " + std::to_string(id)}, "a");
        assert(got == canon);
    }

    cleanup(db);
    std::cout << "[MACADDR] input formats normalize OK" << std::endl;
}

// Malformed input (bad hex digit, wrong length) is rejected at INSERT.
static void test_macaddr_invalid() {
    std::string db = testDbPath("macaddr_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, a macaddr)", s));

    // Non-hex digit.
    assert(g_engine.insert(db, "m", {{"id", "1"}, {"a", "zz:00:2b:01:02:03"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too few bytes (5).
    assert(g_engine.insert(db, "m", {{"id", "2"}, {"a", "08:00:2b:01:02"}}) == dbms::DBStatus::INVALID_VALUE);
    // Too many bytes (7).
    assert(g_engine.insert(db, "m", {{"id", "3"}, {"a", "08:00:2b:01:02:03:04"}}) == dbms::DBStatus::INVALID_VALUE);

    // After rejected inserts the table is empty.
    auto rows = g_engine.query(db, "m", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[MACADDR] invalid input rejected OK" << std::endl;
}

// macaddr8 stores 8 bytes and round-trips canonical form.
static void test_macaddr8() {
    std::string db = testDbPath("macaddr8_t");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, a macaddr8)", s));

    assert(g_engine.insert(db, "m", {{"id", "1"}, {"a", "08-00-2b-01-02-03-04-05"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "m", {"=id 1"}, "a") == "08:00:2b:01:02:03:04:05");

    // 6-byte address into macaddr8 column is wrong length → rejected.
    assert(g_engine.insert(db, "m", {{"id", "2"}, {"a", "08:00:2b:01:02:03"}}) == dbms::DBStatus::INVALID_VALUE);

    cleanup(db);
    std::cout << "[MACADDR] macaddr8 8-byte OK" << std::endl;
}

// UPDATE normalizes valid input and rejects malformed input.
static void test_macaddr_update() {
    std::string db = testDbPath("macaddr_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, a macaddr)", s));

    assert(g_engine.insert(db, "m", {{"id", "1"}, {"a", "08:00:2b:01:02:03"}}) == dbms::DBStatus::OK);

    // Valid update in hyphen form normalizes.
    assert(g_engine.update(db, "m", {{"a", "AA-BB-CC-DD-EE-FF"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "m", {"=id 1"}, "a") == "aa:bb:cc:dd:ee:ff");

    // Invalid update is rejected and leaves the row unchanged.
    assert(g_engine.update(db, "m", {{"a", "nothex"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "m", {"=id 1"}, "a") == "aa:bb:cc:dd:ee:ff");

    cleanup(db);
    std::cout << "[MACADDR] update normalize/reject OK" << std::endl;
}

// A table mixing a variable-length column with macaddr exercises the
// mixed (fixedData) buildRowBuffer encode path.
static void test_macaddr_with_varlen() {
    std::string db = testDbPath("macaddr_var");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE m (id INT PRIMARY KEY, name VARCHAR(20), a macaddr)", s));

    assert(g_engine.insert(db, "m", {{"id", "1"}, {"name", "host"}, {"a", "DE:AD:BE:EF:00:01"}}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "m", {"=id 1"}, "a") == "de:ad:be:ef:00:01");
    assert(fetchOne(db, "m", {"=id 1"}, "name") == "host");

    cleanup(db);
    std::cout << "[MACADDR] mixed varlen encode OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_macaddr_input_formats();
    test_macaddr_invalid();
    test_macaddr8();
    test_macaddr_update();
    test_macaddr_with_varlen();
    std::cout << "[MACADDR] all passed" << std::endl;
    return 0;
}
