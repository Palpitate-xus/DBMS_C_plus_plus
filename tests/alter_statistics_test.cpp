// ============================================================================
// ALTER TABLE SET STATISTICS test — Phase 4 Wave 4.27
// Tests parser for ALTER TABLE ... ALTER COLUMN ... SET STATISTICS n
// and verifies option persistence through the engine's storage params.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/CatalogService.h"
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

    auto reset = parser.parse(
        "ALTER TABLE t ALTER COLUMN name SET STATISTICS -1");
    assert(reset.success);
    alter = dynamic_cast<dbms::AlterTableStmt*>(reset.stmt.get());
    assert(alter && alter->subCommands[0].statisticsTarget == -1);
    assert(!parser.parse(
        "ALTER TABLE t ALTER COLUMN name SET STATISTICS -2").success);
    assert(!parser.parse(
        "ALTER TABLE t ALTER COLUMN name SET STATISTICS 10001").success);

    cleanupTestDb("parser_stats_tmp");
    std::cout << "[ALTER_STATS] parser OK" << std::endl;
}

// Verify the DDL path validates columns and keeps private options and
// pg_attribute in sync across catalog reloads.
static void test_statistics_persistence() {
    std::string db = testDbPath("stats_persist");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50), val INT)", s));

    dbms::CatalogManager& initial = g_engine.catalogService().get(db);
    const auto* relation = initial.resolveRelation("t", {"public"});
    assert(relation != nullptr);
    const dbms::Oid relationOid = relation->oid;

    assert(!ddl.executeSql(
        "ALTER TABLE t ALTER COLUMN name SET STATISTICS 500", s));
    auto opts = g_engine.getStorageParams(db, "t");
    assert(opts["column_statistics:name"] == "500");
    auto* attribute = initial.findAttribute(relationOid, "name");
    assert(attribute != nullptr && attribute->attstattarget == 500);

    assert(!ddl.executeSql(
        "ALTER TABLE t ALTER COLUMN val SET STATISTICS 1000", s));
    opts = g_engine.getStorageParams(db, "t");
    assert(opts["column_statistics:name"] == "500");
    assert(opts["column_statistics:val"] == "1000");
    attribute = initial.findAttribute(relationOid, "val");
    assert(attribute != nullptr && attribute->attstattarget == 1000);

    // -1 removes the private override and restores pg_attribute's default.
    assert(!ddl.executeSql(
        "ALTER TABLE t ALTER COLUMN name SET STATISTICS -1", s));
    opts = g_engine.getStorageParams(db, "t");
    assert(opts.count("column_statistics:name") == 0);
    assert(opts["column_statistics:val"] == "1000");
    attribute = initial.findAttribute(relationOid, "name");
    assert(attribute != nullptr && attribute->attstattarget == -1);

    // A misspelled column must fail without leaving an orphan option behind.
    assert(ddl.executeSql(
        "ALTER TABLE t ALTER COLUMN missing SET STATISTICS 42", s));
    opts = g_engine.getStorageParams(db, "t");
    assert(opts.count("column_statistics:missing") == 0);

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    attribute = reloaded.findAttribute(relationOid, "name");
    assert(attribute != nullptr && attribute->attstattarget == -1);
    attribute = reloaded.findAttribute(relationOid, "val");
    assert(attribute != nullptr && attribute->attstattarget == 1000);

    g_engine.catalogService().evict(db);
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
