// ============================================================================
// ALTER TABLE SET STATISTICS test — Phase 4 Wave 4.27
// Tests parser for ALTER TABLE ... ALTER COLUMN ... SET STATISTICS n
// and verifies option persistence through the engine's storage params.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

// Verify parser correctly parses SET STATISTICS.
static void test_statistics_parser() {
    dbms::SQLParser parser;

    auto r = parser.parse("ALTER TABLE t ALTER COLUMN name SET STATISTICS 500");
    assert(r.success);
    assert(r.stmt);
    auto* alter = dynamic_cast<dbms::AlterTableStmt*>(r.stmt.get());
    assert(alter);
    assert(alter->tableName == "t");
    assert(alter->subCommands.size() == 1);
    assert(alter->subCommands[0].action == dbms::AlterTableStmt::Action::SetStatistics);
    assert(alter->subCommands[0].name == "name");
    assert(alter->subCommands[0].statisticsTarget == 500);

    cleanupTestDb("parser_stats_tmp");
    std::cout << "[ALTER_STATS] parser OK" << std::endl;
}

// Verify statistics target persists through engine storage params.
static void test_statistics_persistence() {
    std::string db = testDbPath("stats_persist");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50), val INT)", s));

    // Simulate main.cpp SET STATISTICS path (each SET replaces storage params)
    {
        std::map<std::string, std::string> params;
        params["column_statistics:name"] = "500";
        g_engine.setStorageParams(db, "t", params);
    }

    {
        auto opts = g_engine.getStorageParams(db, "t");
        assert(opts["column_statistics:name"] == "500");
    }

    // Overwrite: replace the previous value
    {
        std::map<std::string, std::string> params;
        params["column_statistics:name"] = "200";
        g_engine.setStorageParams(db, "t", params);
    }

    {
        auto opts = g_engine.getStorageParams(db, "t");
        assert(opts["column_statistics:name"] == "200");
    }

    // Two columns together
    {
        std::map<std::string, std::string> params;
        params["column_statistics:name"] = "200";
        params["column_statistics:val"] = "1000";
        g_engine.setStorageParams(db, "t", params);
    }

    {
        auto opts = g_engine.getStorageParams(db, "t");
        assert(opts["column_statistics:name"] == "200");
        assert(opts["column_statistics:val"] == "1000");
    }

    cleanup(db);
    std::cout << "[ALTER_STATS] persistence OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_statistics_parser();
    test_statistics_persistence();
    std::cout << "[ALTER_STATS] all passed" << std::endl;
    return 0;
}
