#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

// Test ALTER TABLE ONLY parser support
static void test_only_parser() {
    dbms::SQLParser parser;
    auto r = parser.parse("ALTER TABLE ONLY t ADD COLUMN c INT");
    assert(r.success);
    auto* stmt = dynamic_cast<dbms::AlterTableStmt*>(r.stmt.get());
    assert(stmt);
    assert(stmt->only == true);
    assert(stmt->tableName == "t");
    std::cout << "[ALTER_ONLY] parser OK" << std::endl;
}

// Test ALTER TABLE SET TABLESPACE migrates relation files and survives restart.
static void test_set_tablespace() {
    std::string db = testDbPath("alter_ts");
    std::string location = db + "_location";
    cleanup(db);
    cleanup(location);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    assert(g_engine.createTablespace(db, "my_space", location) == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "42"}}) == dbms::DBStatus::OK);
    size_t rows = 0;
    g_engine.forEachRow(db, "t", [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);

    auto res = g_engine.alterTableTablespace(db, "t", "my_space");
    assert(res == dbms::DBStatus::OK);

    auto schema = g_engine.getTableSchema(db, "t");
    assert(schema.tablespace == "my_space");
    assert(fs::exists(fs::path(location) / db / "t.dt"));
    assert(!fs::exists(fs::path(db) / "t.dt"));

    // A fresh engine must read the migrated relation, not create an empty
    // default-path file after restart.
    dbms::StorageEngine restarted;
    rows = 0;
    restarted.forEachRow(db, "t", [&](uint32_t, uint16_t, const char* data, size_t len) {
        ++rows;
        assert(len > 0);
        (void)data;
    });
    assert(rows == 1);

    // Transaction snapshots must include external tablespace files. Move the
    // table during a transaction, abort, restore the snapshot, and verify the
    // original custom relation is still readable.
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.alterTableTablespace(db, "t", "pg_default") == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.restoreTransactionBackup(db));
    assert(g_engine.getTableSchema(db, "t").tablespace == "my_space");
    rows = 0;
    g_engine.forEachRow(db, "t", [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);

    // UNLOGGED relation files are intentionally absent from crash snapshots.
    assert(!ddl.executeSql(
        "CREATE TABLE u (id INT) TABLESPACE my_space", s));
    assert(g_engine.insert(db, "u", {{"id", "9"}}) == dbms::DBStatus::OK);
    assert(g_engine.alterTableSetLogged(db, "u", false) == dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.alterTableTablespace(db, "u", "pg_default") == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.restoreTransactionBackup(db));
    auto unloggedSchema = g_engine.getTableSchema(db, "u");
    assert(unloggedSchema.isUnlogged && unloggedSchema.tablespace == "my_space");
    rows = 0;
    g_engine.forEachRow(db, "u", [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 0);
    assert(g_engine.dropTable(db, "u") == dbms::DBStatus::OK);

    const std::string backup = db + "_backup";
    cleanup(backup);
    assert(restarted.physicalBackup(db, backup));
    dbms::StorageEngine restored;
    assert(restored.physicalRestore(db, backup));
    rows = 0;
    restored.forEachRow(db, "t", [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);
    assert(restored.dropTable(db, "t") == dbms::DBStatus::OK);
    assert(!fs::exists(fs::path(location) / db / "t.dt"));

    cleanup(db);
    cleanup(location);
    cleanup(backup);
    std::cout << "[ALTER_ONLY] SET TABLESPACE OK" << std::endl;
}

static void test_typed_security_partition_and_trigger_actions() {
    std::string db = testDbPath("alter_typed_actions");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    dbms::SQLParser parser;
    const auto executeAlter = [&](const std::string& sql) {
        bool handled = false;
        const bool error = dbms::tryDdlBridge(
            sql, dbms::SqlCommand::AlterTable, s, handled);
        assert(handled);
        return error;
    };

    auto rlsAst = parser.parse("ALTER TABLE secure ENABLE ROW LEVEL SECURITY");
    assert(rlsAst.success);
    auto* rls = dynamic_cast<dbms::AlterTableStmt*>(rlsAst.stmt.get());
    assert(rls && rls->subCommands.size() == 1);
    assert(rls->subCommands[0].action ==
           dbms::AlterTableStmt::Action::EnableRowLevelSecurity);

    assert(!ddl.executeSql("CREATE TABLE secure (id INT)", s));
    assert(!executeAlter("ALTER TABLE secure ENABLE ROW LEVEL SECURITY"));
    auto schema = g_engine.getTableSchema(db, "secure");
    assert(schema.rowLevelSecurity && !schema.forceRowLevelSecurity);
    assert(!executeAlter("ALTER TABLE secure FORCE ROW LEVEL SECURITY"));
    schema = g_engine.getTableSchema(db, "secure");
    assert(schema.rowLevelSecurity && schema.forceRowLevelSecurity);
    assert(!executeAlter("ALTER TABLE secure NO FORCE ROW LEVEL SECURITY"));
    schema = g_engine.getTableSchema(db, "secure");
    assert(schema.rowLevelSecurity && !schema.forceRowLevelSecurity);
    assert(!executeAlter("ALTER TABLE secure DISABLE ROW LEVEL SECURITY"));
    schema = g_engine.getTableSchema(db, "secure");
    assert(!schema.rowLevelSecurity && !schema.forceRowLevelSecurity);

    assert(!ddl.executeSql(
        "CREATE TABLE parent (id INT, yr INT) PARTITION BY RANGE (yr)", s));
    auto attachAst = parser.parse(
        "ALTER TABLE parent ATTACH PARTITION p2 FOR VALUES FROM (10) TO (20)");
    assert(attachAst.success);
    auto* attach = dynamic_cast<dbms::AlterTableStmt*>(attachAst.stmt.get());
    assert(attach && attach->subCommands.size() == 1);
    assert(attach->subCommands[0].action ==
           dbms::AlterTableStmt::Action::AttachPartition);
    assert(attach->subCommands[0].name == "p2");
    assert(attach->subCommands[0].partitionSpec.find("FOR") != std::string::npos);
    assert(attach->subCommands[0].partitionSpec.find("FROM") != std::string::npos);
    assert(attach->subCommands[0].partitionSpec.find("TO") != std::string::npos);
    assert(!executeAlter(
        "ALTER TABLE parent ATTACH PARTITION p2 FOR VALUES FROM (10) TO (20)"));
    schema = g_engine.getTableSchema(db, "parent");
    assert(std::any_of(schema.rangePartitions.begin(), schema.rangePartitions.end(),
                       [](const auto& p) { return p.first == "p2"; }));
    assert(!executeAlter("ALTER TABLE parent DETACH PARTITION p2"));
    schema = g_engine.getTableSchema(db, "parent");
    assert(std::none_of(schema.rangePartitions.begin(), schema.rangePartitions.end(),
                        [](const auto& p) { return p.first == "p2"; }));

    assert(!ddl.executeSql("CREATE TABLE trigger_target (id INT)", s));
    assert(g_engine.createTrigger(db, {
        "typed_trigger", "before", "insert", "trigger_target", "", "", true, true
    }) == dbms::DBStatus::OK);
    assert(!executeAlter("ALTER TABLE trigger_target DISABLE TRIGGER typed_trigger"));
    auto triggers = g_engine.getAllTriggers(db);
    assert(triggers.size() == 1 && !triggers[0].enabled);
    assert(!executeAlter("ALTER TABLE trigger_target ENABLE TRIGGER typed_trigger"));
    triggers = g_engine.getAllTriggers(db);
    assert(triggers.size() == 1 && triggers[0].enabled);

    cleanup(db);
    std::cout << "[ALTER_ONLY] typed security/partition/trigger actions OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_only_parser();
    test_set_tablespace();
    test_typed_security_partition_and_trigger_actions();
    std::cout << "[ALTER_ONLY] all passed" << std::endl;
    return 0;
}
