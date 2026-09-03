// ============================================================================
// Date infinity test — Phase 4 Wave 4.5
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <vector>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_infinity_timestamp() {
    std::string db = testDbPath("date_inf");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP, tz TIMESTAMPTZ)",
        s));

    assert(g_engine.insert(db, "t",
                           {{"id", "1"},
                            {"ts", "2025-01-01 00:00:00"},
                            {"tz", "2025-01-01 00:00:00+00:00"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t",
                           {{"id", "2"},
                            {"ts", "infinity"},
                            {"tz", "infinity"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t",
                           {{"id", "3"},
                            {"ts", "-infinity"},
                            {"tz", "-infinity"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "4"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {}, {"id", "ts"});
    assert(rows.size() == 4);
    assert(g_engine.query(db, "t", {"=id 2"}, {"ts"}) ==
           std::vector<std::string>{"infinity "});
    assert(g_engine.query(db, "t", {"=id 3"}, {"ts"}) ==
           std::vector<std::string>{"-infinity "});
    assert(g_engine.query(db, "t", {"=id 4"}, {"ts"}) ==
           std::vector<std::string>{"NULL "});

    assert(g_engine.query(db, "t", {">ts 2025-01-01 00:00:00"}, {"id"}) ==
           std::vector<std::string>{"2 "});
    assert(g_engine.query(db, "t", {"<ts 2025-01-01 00:00:00"}, {"id"}) ==
           std::vector<std::string>{"3 "});
    assert(g_engine.query(db, "t", {"=ts infinity"}, {"id"}) ==
           std::vector<std::string>{"2 "});
    assert(g_engine.query(db, "t", {"=ts -infinity"}, {"id"}) ==
           std::vector<std::string>{"3 "});

    assert(g_engine.query(db, "t", {"=id 2"}, {"tz"}, {}, false, false,
                          false, 480) ==
           std::vector<std::string>{"infinity "});
    assert(g_engine.query(db, "t", {"=id 3"}, {"tz"}, {}, false, false,
                          false, -300) ==
           std::vector<std::string>{"-infinity "});

    assert(g_engine.update(db, "t", {{"ts", "-infinity"}}, {"=id 4"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.query(db, "t", {"=id 4"}, {"ts"}) ==
           std::vector<std::string>{"-infinity "});
    assert(g_engine.update(db, "t", {{"ts", "NULL"}}, {"=id 4"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.query(db, "t", {"=id 4"}, {"ts"}) ==
           std::vector<std::string>{"NULL "});

    cleanup(db);
    std::cout << "[DATE_INF] infinity timestamp OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_infinity_timestamp();
    std::cout << "[DATE_INF] all passed" << std::endl;
    return 0;
}
