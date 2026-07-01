// ============================================================================
// ALTER TABLE SET STATISTICS test — Phase 4 Wave 4.27
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

static void test_set_statistics_basic() {
    std::string db = "alter_stats";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))", s));

    // Execute ALTER TABLE via legacy path (main.cpp)
    // Use engine directly since we can't call main.cpp from test
    std::map<std::string, std::string> params;
    params["column_statistics:name"] = "500";
    g_engine.setStorageParams(db, "t", params);

    auto opts = g_engine.getStorageParams(db, "t");
    assert(opts["column_statistics:name"] == "500");

    cleanup(db);
    std::cout << "[ALTER_STATS] SET STATISTICS basic OK" << std::endl;
}

static void test_set_statistics_overwrite() {
    std::string db = "alter_stats2";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT, val INT)", s));

    std::map<std::string, std::string> params;
    params["column_statistics:val"] = "100";
    g_engine.setStorageParams(db, "t", params);

    auto opts = g_engine.getStorageParams(db, "t");
    assert(opts["column_statistics:val"] == "100");

    // Overwrite
    params["column_statistics:val"] = "200";
    g_engine.setStorageParams(db, "t", params);

    opts = g_engine.getStorageParams(db, "t");
    assert(opts["column_statistics:val"] == "200");

    cleanup(db);
    std::cout << "[ALTER_STATS] SET STATISTICS overwrite OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_set_statistics_basic();
    test_set_statistics_overwrite();
    std::cout << "[ALTER_STATS] all passed" << std::endl;
    return 0;
}
