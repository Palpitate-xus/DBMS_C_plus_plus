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

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_rollback_create();
    test_commit_survives();
    test_create_table_post_action_rollback();
    test_wal_catalog_record();
    test_executor_uses_transaction();
    test_alter_statement_rollback();
    test_catalog_drop_plan_is_deferred();
    test_schema_drop_plan_is_deferred();
    std::cout << "[DDL-TXN] all passed" << std::endl;
    return 0;
}
