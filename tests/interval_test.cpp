// ============================================================================
// interval type test — Phase 4 Wave 4.5 (interval input + canonicalization)
// interval columns parse the verbose form, HH:MM:SS time, Y-M shorthand, bare
// seconds and trailing "ago", then canonicalize to PostgreSQL "postgres" style
// ([N years] [N mons] [N days] [HH:MM:SS]). Malformed input is rejected.
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

static void check(const std::string& db, dbms::DdlExecutor&, Session&, int& id,
                  const std::string& input, const std::string& expected) {
    assert(g_engine.insert(db, "t", {{"id", std::to_string(id)}, {"v", input}}) == dbms::DBStatus::OK);
    std::string got = fetchOne(db, "t", {"=id " + std::to_string(id)}, "v");
    if (got != expected) {
        std::cerr << "interval '" << input << "' -> '" << got << "' expected '" << expected << "'\n";
        assert(false);
    }
    ++id;
}

static void test_interval_canonical() {
    std::string db = testDbPath("iv_ok");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, v INTERVAL)", s));

    int id = 1;
    check(db, ddl, s, id, "1 year", "1 year");
    check(db, ddl, s, id, "2 years", "2 years");
    check(db, ddl, s, id, "1 month", "1 mon");
    check(db, ddl, s, id, "14 months", "1 year 2 mons");
    check(db, ddl, s, id, "1 year 2 months 3 days", "1 year 2 mons 3 days");
    check(db, ddl, s, id, "3 days", "3 days");
    check(db, ddl, s, id, "1 day", "1 day");
    check(db, ddl, s, id, "90 minutes", "01:30:00");
    check(db, ddl, s, id, "04:05:06", "04:05:06");
    check(db, ddl, s, id, "25 hours", "25:00:00");
    check(db, ddl, s, id, "1 day 2 hours", "1 day 02:00:00");
    check(db, ddl, s, id, "1 year 2 mons 3 days 04:05:06", "1 year 2 mons 3 days 04:05:06");
    check(db, ddl, s, id, "2 weeks", "14 days");
    check(db, ddl, s, id, "1-2", "1 year 2 mons");        // SQL year-month shorthand
    check(db, ddl, s, id, "1.5 days", "1 day 12:00:00");  // fractional cascades
    check(db, ddl, s, id, "30", "00:00:30");              // bare number = seconds

    cleanup(db);
    std::cout << "[INTERVAL] canonicalization OK" << std::endl;
}

static void test_interval_invalid() {
    std::string db = testDbPath("iv_bad");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, v INTERVAL)", s));

    // Garbage / unknown unit / dangling unit.
    assert(g_engine.insert(db, "t", {{"id","1"}, {"v","hello"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","2"}, {"v","1 fortnight"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","3"}, {"v","year"}}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(db, "t", {{"id","4"}, {"v","1 year bogus"}}) == dbms::DBStatus::INVALID_VALUE);

    auto rows = g_engine.query(db, "t", {}, {"id"}, {});
    assert(rows.empty());

    cleanup(db);
    std::cout << "[INTERVAL] invalid rejected OK" << std::endl;
}

static void test_interval_update() {
    std::string db = testDbPath("iv_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, v INTERVAL)", s));

    assert(g_engine.insert(db, "t", {{"id","1"}, {"v","1 day"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "t", {{"v","2 hours 30 minutes"}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(fetchOne(db, "t", {"=id 1"}, "v") == "02:30:00");
    assert(g_engine.update(db, "t", {{"v","nonsense"}}, {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(fetchOne(db, "t", {"=id 1"}, "v") == "02:30:00");

    cleanup(db);
    std::cout << "[INTERVAL] update enforce/canonicalize OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_interval_canonical();
    test_interval_invalid();
    test_interval_update();
    std::cout << "[INTERVAL] all passed" << std::endl;
    return 0;
}
