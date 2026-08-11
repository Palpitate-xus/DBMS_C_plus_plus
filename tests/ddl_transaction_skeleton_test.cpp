// ============================================================================
// DDL Transaction Skeleton Test — Phase 4 Wave 0.4
// ============================================================================

#include "commands/DdlTransaction.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "storage/WAL.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <atomic>
#include <chrono>
#include <thread>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static void test_rollback_create() {
    std::string db = testDbPath("ddl_txn_t1");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlTransaction txn(s);
    assert(txn.begin());

    dbms::TableSchema tbl;
    tbl.tablename = "rollback_tbl";
    dbms::Column col = dbms::makeIntColumn("id", true, 2, true);
    tbl.append(col);
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);
    assert(g_engine.tableExists(db, "rollback_tbl"));

    txn.recordCreate(dbms::DdlObjectKind::Table, "rollback_tbl");
    txn.rollback();

    assert(!g_engine.tableExists(db, "rollback_tbl"));
    assert(!g_engine.inTransaction());

    cleanup(db);
    std::cout << "[DDL-TXN] rollback create OK" << std::endl;
}

static void test_commit_survives() {
    std::string db = testDbPath("ddl_txn_t2");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlTransaction txn(s);
    assert(txn.begin());

    dbms::TableSchema tbl;
    tbl.tablename = "commit_tbl";
    tbl.append(dbms::makeIntColumn("id", true, 2, true));
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Table, "commit_tbl");
    txn.commit();

    assert(g_engine.tableExists(db, "commit_tbl"));
    assert(!g_engine.inTransaction());

    cleanup(db);
    std::cout << "[DDL-TXN] commit survives OK" << std::endl;
}

static void test_commit_failure_is_propagated_and_restored() {
    std::string db = testDbPath("ddl_txn_t_commit_failure");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE deferred_tbl (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0) DEFERRABLE INITIALLY DEFERRED)",
        s));

    dbms::DdlTransaction txn(s);
    assert(txn.begin());
    txn.enableSnapshotRollback();
    txn.markSnapshotDirty();
    dbms::Column extra = dbms::makeIntColumn("extra", false, 0, false);
    assert(g_engine.alterTableAddColumn(db, "deferred_tbl", extra) == dbms::DBStatus::OK);
    assert(g_engine.getTableSchema(db, "deferred_tbl").len == 3);
    assert(g_engine.insert(db, "deferred_tbl", {{"id", "1"}, {"price", "0"},
                                                  {"extra", "7"}}) == dbms::DBStatus::OK);

    // The deferred CHECK aborts the engine transaction.  DdlTransaction must
    // return failure and restore the physical DDL snapshot instead of letting
    // the caller print a success message for a failed commit.
    assert(!txn.commit());
    assert(!txn.isActive());
    assert(!txn.isCommitted());
    assert(!g_engine.inTransaction());
    assert(g_engine.getTableSchema(db, "deferred_tbl").len == 2);
    assert(g_engine.query(db, "deferred_tbl", {"=id 1"}, {"price"}).empty());

    cleanup(db);
    std::cout << "[DDL-TXN] commit failure propagation and snapshot restore OK" << std::endl;
}

static void test_engine_transaction_backup_lifecycle() {
    std::string db = testDbPath("ddl_txn_t_backup_lifecycle");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema tbl;
    tbl.tablename = "lifecycle_tbl";
    tbl.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "lifecycle_tbl", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(!fs::exists(db + ".txn_backup"));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "lifecycle_tbl", {{"id", "2"}}) == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!fs::exists(db + ".txn_backup"));

    cleanup(db);
    std::cout << "[DDL-TXN] engine backup lifecycle OK" << std::endl;
}

static void test_begin_propagates_implicit_commit_failure() {
    std::string db = testDbPath("ddl_txn_t_begin_failure");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE deferred_begin (id INT PRIMARY KEY, price INT, "
        "CONSTRAINT chk_price CHECK (price > 0) DEFERRABLE INITIALLY DEFERRED)",
        s));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "deferred_begin", {{"id", "1"}, {"price", "0"}}) ==
           dbms::DBStatus::OK);
    // Starting another transaction must not swallow the failed implicit
    // commit or open a fresh transaction after the deferred CHECK aborts.
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::INVALID_VALUE);
    assert(!g_engine.inTransaction());
    assert(!fs::exists(db + ".txn_backup"));

    cleanup(db);
    std::cout << "[DDL-TXN] implicit begin failure propagation OK" << std::endl;
}

static void test_snapshot_ddl_serializes_database_backends() {
    std::string db = testDbPath("ddl_txn_t_snapshot_lock");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema tbl;
    tbl.tablename = "snapshot_lock_tbl";
    tbl.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, tbl) == dbms::DBStatus::OK);

    // A file snapshot is only safe while the database is quiescent. The
    // second backend must wait instead of committing data that a later
    // physical restore could erase.
    dbms::StorageEngine other;
    Session s;
    setupSession(s, db);
    dbms::DdlTransaction txn(s);
    txn.enableSnapshotRollback();
    assert(txn.begin());

    std::atomic<bool> returned{false};
    dbms::DBStatus otherStatus = dbms::DBStatus::INVALID_VALUE;
    std::thread worker([&] {
        otherStatus = other.beginTransaction(db);
        returned = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    assert(!returned.load());

    txn.rollback();
    worker.join();
    assert(returned.load());
    assert(otherStatus == dbms::DBStatus::OK);
    assert(other.rollbackTransaction() == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[DDL-TXN] snapshot database lock serialization OK" << std::endl;
}

static void test_unfinished_snapshot_recovers_on_restart() {
    const std::string db = testDbPath("ddl_txn_t_snapshot_recovery");
    const std::string backup = db + ".txn_backup.900001";
    cleanup(db);
    cleanup(backup);

    {
        dbms::StorageEngine source;
        assert(source.createDatabase(db, "utf8") == dbms::DBStatus::OK);
        dbms::TableSchema tbl;
        tbl.tablename = "recovery_tbl";
        tbl.append(dbms::makeIntColumn("id", false, 2, true));
        assert(source.createTable(db, tbl) == dbms::DBStatus::OK);
        assert(source.physicalBackup(db, backup));
        assert(source.alterTableAddColumn(
                   db, "recovery_tbl", dbms::makeIntColumn("after_crash", true, 0, false)) ==
               dbms::DBStatus::OK);
        assert(source.getTableSchema(db, "recovery_tbl").len == 2);
    }

    // No COMMIT record exists for the synthetic snapshot xid, so startup
    // recovery must restore the pre-DDL image and remove the snapshot.
    {
        dbms::StorageEngine restarted;
        assert(restarted.getTableSchema(db, "recovery_tbl").len == 1);
        assert(!std::filesystem::exists(backup));
    }

    cleanup(db);
    std::cout << "[DDL-TXN] unfinished snapshot restart recovery OK" << std::endl;
}

static void test_create_table_post_action_rollback() {
    std::string db = testDbPath("ddl_txn_t_post_action");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    // Force persistConstraintMetadata() to fail after the physical table has
    // already been created. DdlTransaction must remove the relation.
    fs::create_directory(db + "/constraint_fail.params");
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    bool err = ddl.executeSql(
        "CREATE TABLE constraint_fail (id INT, CONSTRAINT ck CHECK (id > 0))", s);
    assert(err);
    assert(!g_engine.tableExists(db, "constraint_fail"));
    assert(!fs::exists(db + "/constraint_fail.stc"));
    assert(!fs::exists(db + "/constraint_fail.dt"));
    for (const auto& name : g_engine.getTableNames(db)) assert(name != "constraint_fail");

    cleanup(db);
    std::cout << "[DDL-TXN] post-create failure rollback OK" << std::endl;
}

static void test_wal_catalog_record() {
    std::string db = testDbPath("ddl_txn_t3");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    bool err = ddl.executeSql("CREATE TABLE wal_tbl (id INT)", s);
    assert(!err);
    assert(g_engine.tableExists(db, "wal_tbl"));

    dbms::WALManager* wal = g_engine.getWAL(db);
    assert(wal != nullptr);

    bool found = false;
    dbms::Lsn lsn = 0;
    while (true) {
        auto rec = wal->ReadNextRecord(lsn);
        if (!rec) break;
        if (rec->rmid() == dbms::RM_CATALOG_ID) {
            found = true;
            break;
        }
        lsn += rec->header.xl_tot_len;
    }

    assert(found);

    cleanup(db);
    std::cout << "[DDL-TXN] WAL catalog record OK" << std::endl;
}

static void test_executor_uses_transaction() {
    std::string db = testDbPath("ddl_txn_t4");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!g_engine.inTransaction());
    bool err = ddl.executeSql("CREATE TABLE exec_tbl (id INT)", s);
    assert(!err);
    assert(g_engine.tableExists(db, "exec_tbl"));
    assert(!g_engine.inTransaction());

    cleanup(db);
    std::cout << "[DDL-TXN] executor uses transaction OK" << std::endl;
}

static void test_explicit_transaction_ddl_rollback_and_savepoint() {
    const std::string db = testDbPath("ddl_txn_t_outer_transaction");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE outer_first (id INT PRIMARY KEY)", s));
    assert(!ddl.executeSql("CREATE TABLE outer_second (id INT PRIMARY KEY)", s));
    assert(g_engine.tableExists(db, "outer_first"));
    assert(g_engine.tableExists(db, "outer_second"));
    assert(g_engine.prepareTransaction("ddl_prepare_rejected") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!g_engine.tableExists(db, "outer_first"));
    assert(!g_engine.tableExists(db, "outer_second"));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE savepoint_first (id INT PRIMARY KEY)", s));
    assert(g_engine.savepoint("after_first") == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE savepoint_second (id INT PRIMARY KEY)", s));
    assert(g_engine.rollbackToSavepoint("after_first") == dbms::DBStatus::OK);
    assert(g_engine.tableExists(db, "savepoint_first"));
    assert(!g_engine.tableExists(db, "savepoint_second"));
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(g_engine.tableExists(db, "savepoint_first"));

    cleanup(db);
    std::cout << "[DDL-TXN] explicit transaction DDL rollback and savepoint OK" << std::endl;
}

static void test_drop_and_replace_restore_before_outer_row_undo() {
    const std::string db = testDbPath("ddl_txn_t_drop_restore");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE drop_restore (id INT PRIMARY KEY)", s));
    assert(g_engine.insert(db, "drop_restore", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE VIEW replace_restore AS SELECT id FROM drop_restore", s));
    const std::string oldViewSql = g_engine.getViewSQL(db, "replace_restore");

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "drop_restore", {{"id", "2"}}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("DROP TABLE drop_restore", s));
    assert(!g_engine.tableExists(db, "drop_restore"));
    assert(g_engine.savepoint("after_drop") == dbms::DBStatus::INVALID_VALUE);
    // A second full-snapshot DDL statement is rejected rather than silently
    // reusing the first statement's image and breaking atomicity.
    assert(ddl.executeSql(
        "CREATE OR REPLACE VIEW replace_restore AS SELECT id FROM replace_restore", s));

    // The snapshot is restored first, then the row undo removes id=2 from
    // the restored table and the pre-transaction view definition survives.
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.tableExists(db, "drop_restore"));
    const auto restoredId2 = g_engine.query(db, "drop_restore", {"=id 2"}, {"id"});
    const auto restoredId1 = g_engine.query(db, "drop_restore", {"=id 1"}, {"id"});
    assert(restoredId2.empty());
    assert(restoredId1.size() == 1);
    assert(g_engine.getViewSQL(db, "replace_restore") == oldViewSql);

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "CREATE OR REPLACE VIEW replace_restore AS SELECT id FROM replace_restore", s));
    assert(g_engine.getViewSQL(db, "replace_restore") != oldViewSql);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.getViewSQL(db, "replace_restore") == oldViewSql);

    cleanup(db);
    std::cout << "[DDL-TXN] DROP/REPLACE restore before row undo OK" << std::endl;
}

static void test_alter_statement_rollback() {
    std::string db = testDbPath("ddl_txn_t_alter_atomic");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE alter_atomic (id INT)", s));

    // The first subcommand succeeds; the second one fails semantically.  A
    // PostgreSQL-style statement boundary must remove the first change too.
    bool err = ddl.executeSql(
        "ALTER TABLE alter_atomic ADD COLUMN first_value INT, DROP COLUMN missing_value", s);
    assert(err);
    const auto table = g_engine.getTableSchema(db, "alter_atomic");
    assert(table.len == 1);
    assert(table.cols[0].dataName == "id");
    assert(!fs::exists(db + ".txn_backup"));
    assert(!g_engine.inTransaction());

    cleanup(db);
    std::cout << "[DDL-TXN] ALTER statement rollback OK" << std::endl;
}

static void test_catalog_drop_plan_is_deferred() {
    std::string db = testDbPath("ddl_txn_t_drop_plan");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE drop_plan_tbl (id INT)", s));

    auto& cat = g_engine.catalogService().get(db);
    const auto* cls = cat.resolveRelation("drop_plan_tbl", {"public"});
    assert(cls != nullptr);
    const auto plan = cat.planDrop(dbms::PgClassOid_Class, cls->oid,
                                   dbms::CatalogManager::DropBehavior::Restrict);
    assert(plan.ok());
    assert(g_engine.tableExists(db, "drop_plan_tbl"));
    assert(cat.applyDropPlan(plan));
    assert(!cat.resolveRelation("drop_plan_tbl", {"public"}));
    // The plan is independent from physical storage; the executor applies it
    // only after StorageEngine::dropTable succeeds.
    assert(g_engine.tableExists(db, "drop_plan_tbl"));

    assert(g_engine.dropTable(db, "drop_plan_tbl") == dbms::DBStatus::OK);
    cleanup(db);
    std::cout << "[DDL-TXN] catalog drop plan ordering OK" << std::endl;
}

static void test_schema_drop_plan_is_deferred() {
    std::string db = testDbPath("ddl_txn_t_schema_drop_plan");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE SCHEMA app", s));

    auto& cat = g_engine.catalogService().get(db);
    const auto* ns = cat.findNamespaceByName("app");
    assert(ns != nullptr);
    const auto plan = cat.planDrop(dbms::PgClassOid_Namespace, ns->oid,
                                   dbms::CatalogManager::DropBehavior::Restrict);
    assert(plan.ok());
    assert(g_engine.schemaExists(db, "app"));
    assert(cat.applyDropPlan(plan));
    assert(!cat.findNamespaceByName("app"));
    // Catalog planning is independent from physical storage.  The executor
    // applies the plan only after StorageEngine::dropSchema succeeds.
    assert(g_engine.schemaExists(db, "app"));

    assert(g_engine.dropSchema(db, "app", false) == dbms::DBStatus::OK);
    cleanup(db);
    std::cout << "[DDL-TXN] schema catalog drop plan ordering OK" << std::endl;
}

static void test_auxiliary_object_rollback() {
    std::string db = testDbPath("ddl_txn_t_auxiliary_objects");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlTransaction txn(s);
    assert(txn.begin());

    assert(g_engine.createView(db, "rollback_view", "SELECT id FROM rollback_tbl") ==
           dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::View, "rollback_view");

    dbms::TableSchema relation;
    relation.tablename = "rollback_tbl";
    relation.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    relation.append(dbms::makeIntColumn("id", true, 2, true));
    assert(g_engine.createTable(db, relation) == dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Table, "rollback_tbl");

    dbms::StorageEngine::Trigger trigger;
    trigger.name = "rollback_trigger";
    trigger.timing = "before";
    trigger.event = "insert";
    trigger.tableName = "rollback_tbl";
    trigger.action = "SELECT 1";
    trigger.forEachRow = true;
    assert(g_engine.createTrigger(db, trigger) == dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Trigger, trigger.name, trigger.tableName);

    dbms::StorageEngine::RowPolicy policy;
    policy.name = "rollback_policy";
    policy.cmd = "ALL";
    policy.usingExpr = "true";
    policy.withCheckExpr = "true";
    assert(g_engine.createPolicy(db, "rollback_tbl", policy) == dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Policy, policy.name, "rollback_tbl");

    assert(g_engine.createUDF(db, "rollback_udf", "x", "x + 1") == dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Function, "rollback_udf");
    assert(g_engine.createTVF(db, "rollback_tvf", "x", "SELECT x") == dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Function, "rollback_tvf");

    assert(g_engine.createProcedure(db, "rollback_proc", {}, {"SELECT 1"}) ==
           dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Procedure, "rollback_proc");

    dbms::TableSchema materializedBacking;
    materializedBacking.tablename = dbms::StorageEngine::materializedViewPrefix("rollback_mv");
    materializedBacking.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    materializedBacking.append(dbms::makeIntColumn("id", true, 2, true));
    assert(g_engine.createTable(db, materializedBacking) == dbms::DBStatus::OK);
    std::filesystem::create_directories(g_engine.viewsDir(db));
    {
        std::ofstream mview(g_engine.viewsDir(db) / "rollback_mv.mview");
        mview << "SELECT 1";
    }
    txn.recordCreate(dbms::DdlObjectKind::MaterializedView, "rollback_mv");

    assert(g_engine.createCollation(db, "rollback_collation_a", "libc", "C") ==
           dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Collation, "rollback_collation_a");
    assert(g_engine.createCollation(db, "rollback_collation_b", "libc", "C") ==
           dbms::DBStatus::OK);
    txn.recordCreate(dbms::DdlObjectKind::Collation, "rollback_collation_b");

    txn.rollback();

    assert(!g_engine.viewExists(db, "rollback_view"));
    assert(!g_engine.tableExists(db, "rollback_tbl"));
    assert(!g_engine.udfExists(db, "rollback_udf"));
    assert(!g_engine.tvfExists(db, "rollback_tvf"));
    assert(!g_engine.procedureExists(db, "rollback_proc"));
    assert(!g_engine.isMaterializedView(db, "rollback_mv"));
    assert(!std::filesystem::exists(db + "/.collations"));
    assert(!g_engine.inTransaction());

    cleanup(db);
    std::cout << "[DDL-TXN] auxiliary object rollback OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_rollback_create();
    test_commit_survives();
    test_commit_failure_is_propagated_and_restored();
    test_engine_transaction_backup_lifecycle();
    test_begin_propagates_implicit_commit_failure();
    test_snapshot_ddl_serializes_database_backends();
    test_unfinished_snapshot_recovers_on_restart();
    test_create_table_post_action_rollback();
    test_wal_catalog_record();
    test_executor_uses_transaction();
    test_explicit_transaction_ddl_rollback_and_savepoint();
    test_drop_and_replace_restore_before_outer_row_undo();
    test_alter_statement_rollback();
    test_catalog_drop_plan_is_deferred();
    test_schema_drop_plan_is_deferred();
    test_auxiliary_object_rollback();
    std::cout << "[DDL-TXN] all passed" << std::endl;
    return 0;
}
