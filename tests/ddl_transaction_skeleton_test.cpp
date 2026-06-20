// ============================================================================
// DDL Transaction Skeleton Test — Phase 4 Wave 0.4
// ============================================================================

#include "commands/DdlTransaction.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "storage/WAL.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

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

static void test_rollback_create() {
    std::string db = "ddl_txn_t1";
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
    std::string db = "ddl_txn_t2";
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

static void test_wal_catalog_record() {
    std::string db = "ddl_txn_t3";
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
    std::string db = "ddl_txn_t4";
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

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_rollback_create();
    test_commit_survives();
    test_wal_catalog_record();
    test_executor_uses_transaction();
    std::cout << "[DDL-TXN] all passed" << std::endl;
    return 0;
}
