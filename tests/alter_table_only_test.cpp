#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

static std::string readFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
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

    // Keep the cached schema readable while replacing its path with a
    // same-timestamp directory. The final atomic publication must fail and
    // every relation fork already moved to the target tablespace must return
    // to pg_default.
    const fs::path schemaPath = fs::path(db) / "t.stc";
    const fs::path savedSchemaPath = fs::path(db) / "t.stc.saved";
    const std::string originalSchemaBytes = readFile(schemaPath);
    assert(g_engine.getTableSchema(db, "t").tablespace == "pg_default");
    const auto schemaTimestamp = fs::last_write_time(schemaPath);
    fs::rename(schemaPath, savedSchemaPath);
    assert(fs::create_directory(schemaPath));
    fs::last_write_time(schemaPath, schemaTimestamp);
    assert(g_engine.alterTableTablespace(db, "t", "my_space") ==
           dbms::DBStatus::IO_ERROR);
    assert(fs::exists(fs::path(db) / "t.dt"));
    assert(!fs::exists(fs::path(location) / db / "t.dt"));
    fs::remove(schemaPath);
    fs::rename(savedSchemaPath, schemaPath);
    assert(readFile(schemaPath) == originalSchemaBytes);
    dbms::StorageEngine failedMoveRestart;
    assert(failedMoveRestart.getTableSchema(db, "t").tablespace ==
           "pg_default");
    rows = 0;
    failedMoveRestart.forEachRow(
        db, "t", [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
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
    g_engine.preserveTransactionBackupOnRollback(true);
    assert(g_engine.alterTableTablespace(db, "t", "pg_default") == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.restoreTransactionBackup(db));
    assert(g_engine.getTableSchema(db, "t").tablespace == "my_space");
    rows = 0;
    g_engine.forEachRow(db, "t", [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);

    // SQL transaction rollback restores UNLOGGED rows. Crash recovery is a
    // separate path that resets those rows after an unclean shutdown.
    assert(!ddl.executeSql(
        "CREATE TABLE u (id INT) TABLESPACE my_space", s));
    assert(g_engine.insert(db, "u", {{"id", "9"}}) == dbms::DBStatus::OK);
    assert(g_engine.alterTableSetLogged(db, "u", false) == dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    g_engine.preserveTransactionBackupOnRollback(true);
    assert(g_engine.alterTableTablespace(db, "u", "pg_default") == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.restoreTransactionBackup(db));
    auto unloggedSchema = g_engine.getTableSchema(db, "u");
    assert(unloggedSchema.isUnlogged && unloggedSchema.tablespace == "my_space");
    rows = 0;
    g_engine.forEachRow(db, "u", [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);
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

static void test_rename_preserves_table_sidecars() {
    std::string db = testDbPath("alter_rename_sidecars");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "configured";
    table.append(dbms::makeIntColumn("id", false, 2));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.setStorageParams(
               db, "configured",
               {{"fillfactor", "73"}, {"column_statistics:id", "250"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "configured", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.truncateTable(db, "configured") ==
           dbms::DBStatus::OK);

    dbms::StorageEngine::RowPolicy policy;
    policy.name = "visible_rows";
    policy.cmd = "SELECT";
    policy.usingExpr = "id > 0";
    policy.roles = {"PUBLIC"};
    assert(g_engine.createPolicy(db, "configured", policy) ==
           dbms::DBStatus::OK);
    assert(g_engine.enableRowLevelSecurity(db, "configured") ==
           dbms::DBStatus::OK);
    g_engine.grant(
        db, "configured", "configured_reader",
        dbms::StorageEngine::TablePrivilege::Select, {}, true, "owner");
    g_engine.grant(
        db, "configured", "downstream_reader",
        dbms::StorageEngine::TablePrivilege::Select, {}, false,
        "configured_reader");
    const auto oldParams = fs::path(db) / "configured.params";
    const auto oldPolicies = fs::path(db) / "configured.rls";
    const auto oldTruncate = fs::path(db) / "configured.truncate_state";
    assert(fs::is_regular_file(oldParams));
    assert(fs::is_regular_file(oldPolicies));
    assert(fs::is_regular_file(oldTruncate));

    assert(g_engine.alterTableRenameTable(
               db, "configured", "renamed") == dbms::DBStatus::OK);
    assert(!fs::exists(oldParams));
    assert(!fs::exists(oldPolicies));
    assert(!fs::exists(oldTruncate));
    assert(fs::is_regular_file(fs::path(db) / "renamed.params"));
    assert(fs::is_regular_file(fs::path(db) / "renamed.rls"));
    assert(fs::is_regular_file(fs::path(db) / "renamed.truncate_state"));

    auto params = g_engine.getStorageParams(db, "renamed");
    assert(params["fillfactor"] == "73");
    assert(params["column_statistics:id"] == "250");
    auto policies = g_engine.getPolicies(db, "renamed");
    assert(policies.size() == 1);
    assert(policies.front().name == "visible_rows");
    assert(policies.front().usingExpr == "id > 0");
    assert(g_engine.getTableSchema(db, "renamed").rowLevelSecurity);
    assert(g_engine.hasPermission(
        db, "renamed", "configured_reader",
        dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasPermission(
        db, "renamed", "downstream_reader",
        dbms::StorageEngine::TablePrivilege::Select));
    assert(g_engine.hasGrantOption(
        db, "renamed", "configured_reader",
        dbms::StorageEngine::TablePrivilege::Select));
    assert(!g_engine.hasPermission(
        db, "configured", "configured_reader",
        dbms::StorageEngine::TablePrivilege::Select));
    // The downstream grant is still linked to its grantor under the new
    // object name, so REVOKE without CASCADE must be rejected.
    assert(!g_engine.revoke(
        db, "renamed", "configured_reader",
        dbms::StorageEngine::TablePrivilege::Select, {}, false, true));

    {
        dbms::StorageEngine restarted;
        params = restarted.getStorageParams(db, "renamed");
        assert(params["fillfactor"] == "73");
        policies = restarted.getPolicies(db, "renamed");
        assert(policies.size() == 1);
        assert(policies.front().name == "visible_rows");
        assert(restarted.getTableSchema(db, "renamed").rowLevelSecurity);
        assert(restarted.hasPermission(
            db, "renamed", "configured_reader",
            dbms::StorageEngine::TablePrivilege::Select));
    }

    cleanup(db);
    std::cout << "[ALTER_ONLY] RENAME preserves table sidecars OK" << std::endl;
}

static void test_add_column_preserves_modifiers() {
    const std::string db = testDbPath("alter_add_column_modifiers");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE modifier_target (id INT PRIMARY KEY)", s));
    assert(g_engine.insert(db, "modifier_target", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(!ddl.executeSql(
        "ALTER TABLE modifier_target ADD COLUMN score INT DEFAULT 5 "
        "NOT NULL CONSTRAINT score_positive CHECK (score > 0)", s));
    auto schema = g_engine.getTableSchema(db, "modifier_target");
    assert(schema.len == 2);
    assert(schema.cols[1].dataName == "score");
    assert(!schema.cols[1].isNull);
    assert(schema.cols[1].defaultValue == "5");
    assert(schema.cols[1].checkExpr == "score > 0");
    assert(schema.cols[1].checkConstraintName == "score_positive");
    assert(g_engine.insert(db, "modifier_target",
                           {{"id", "2"}, {"score", "0"}}) ==
           dbms::DBStatus::CHECK_VIOLATION);
    assert(g_engine.insert(db, "modifier_target", {{"id", "2"}}) ==
           dbms::DBStatus::OK);

    assert(!ddl.executeSql(
        "ALTER TABLE modifier_target ADD COLUMN doubled INT "
        "GENERATED ALWAYS AS (score * 2) VIRTUAL", s));
    schema = g_engine.getTableSchema(db, "modifier_target");
    assert(schema.len == 3);
    assert(schema.cols[2].generatedExpr == "score * 2");
    assert(schema.cols[2].generatedKind == 'v');

    cleanup(db);
    std::cout << "[ALTER_ONLY] ADD COLUMN modifiers preserved OK"
              << std::endl;
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
    bool missingPartitionRelation = false;
    try {
        executeAlter(
            "ALTER TABLE parent ATTACH PARTITION no_such_partition "
            "FOR VALUES FROM (10) TO (20)");
    } catch (const dbms::DbError& error) {
        missingPartitionRelation = error.sqlState() == "42P01";
    }
    assert(missingPartitionRelation);
    assert(g_engine.getTableSchema(db, "parent").rangePartitions.empty());
    assert(!ddl.executeSql("CREATE TABLE p2 (id INT, yr INT)", s));
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
    test_rename_preserves_table_sidecars();
    test_add_column_preserves_modifiers();
    test_typed_security_partition_and_trigger_actions();
    std::cout << "[ALTER_ONLY] all passed" << std::endl;
    return 0;
}
