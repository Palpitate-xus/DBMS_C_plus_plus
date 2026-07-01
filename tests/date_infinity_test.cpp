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

static void test_infinity_timestamp() {
    std::string db = "date_inf";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP)", s));

    assert(g_engine.insert(db, "t", {{"id", "1"}, {"ts", "2025-01-01 00:00:00"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"ts", "infinity"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "3"}, {"ts", "-infinity"}}) == dbms::DBStatus::OK);

    auto rows = g_engine.query(db, "t", {}, {"id", "ts"});
    assert(rows.size() == 3);

    cleanup(db);
    std::cout << "[DATE_INF] infinity timestamp OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_infinity_timestamp();
    std::cout << "[DATE_INF] all passed" << std::endl;
    return 0;
}
