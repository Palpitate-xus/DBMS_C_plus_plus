#include "storage/CommitLog.h"
#include "TableManage.h"
#include "Config.h"
#include <iostream>
#include <filesystem>

dbms::Config g_config;

using namespace dbms;

int main() {
    std::string dbname = "clog_integration_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");

    StorageEngine engine;
    DBStatus r = engine.createDatabase(dbname);
    if (r != DBStatus::OK) {
        std::cerr << "createDatabase failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    // Create a simple table
    TableSchema tbl;
    tbl.tablename = "t";
    tbl.append(makeIntColumn("id", false, 0, true));
    tbl.append(makeVarCharColumn("name", false, 20, false));
    r = engine.createTable(dbname, tbl);
    if (r != DBStatus::OK) {
        std::cerr << "createTable failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    // Transaction 1: commit
    r = engine.beginTransaction(dbname);
    if (r != DBStatus::OK) {
        std::cerr << "beginTransaction failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    std::map<std::string, std::string> vals;
    vals["id"] = "1";
    vals["name"] = "alice";
    r = engine.insert(dbname, "t", vals);
    if (r != DBStatus::OK) {
        std::cerr << "insert failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    uint64_t txid1 = engine.currentTxnId();
    r = engine.commitTransaction();
    if (r != DBStatus::OK) {
        std::cerr << "commitTransaction failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    // Transaction 2: rollback
    r = engine.beginTransaction(dbname);
    if (r != DBStatus::OK) {
        std::cerr << "beginTransaction 2 failed: " << static_cast<int>(r) << "\n";
        return 1;
    }
    vals["id"] = "2";
    vals["name"] = "bob";
    r = engine.insert(dbname, "t", vals);
    if (r != DBStatus::OK) {
        std::cerr << "insert 2 failed: " << static_cast<int>(r) << "\n";
        return 1;
    }
    uint64_t txid2 = engine.currentTxnId();
    r = engine.rollbackTransaction();
    if (r != DBStatus::OK) {
        std::cerr << "rollbackTransaction failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    // Verify CLOG
    CommitLog* clog = engine.getCommitLog(dbname);
    if (!clog) {
        std::cerr << "CommitLog not found\n";
        return 1;
    }
    if (clog->getStatus(txid1) != CommitLog::Status::Committed) {
        std::cerr << "txid1 should be committed\n";
        return 1;
    }
    if (clog->getStatus(txid2) != CommitLog::Status::Aborted) {
        std::cerr << "txid2 should be aborted\n";
        return 1;
    }

    // A COMMIT WAL record must not be reported as a successful live commit
    // when CLOG cannot be durably published. The engine must undo the row,
    // append an ABORT record after the already-written COMMIT, and leave no
    // active transaction behind. Recovery uses the later ABORT to avoid
    // replaying the failed commit.
    const std::string failureDb = "clog_commit_failure_db";
    std::filesystem::remove_all(failureDb);
    if (engine.createDatabase(failureDb) != DBStatus::OK) return 1;
    TableSchema failureTable;
    failureTable.tablename = "t";
    failureTable.append(makeIntColumn("id", false, 0, true));
    if (engine.createTable(failureDb, failureTable) != DBStatus::OK) return 1;
    if (engine.beginTransaction(failureDb) != DBStatus::OK) return 1;
    if (engine.insert(failureDb, "t", {{"id", "1"}}) != DBStatus::OK) return 1;
    (void)engine.getCommitLog(failureDb); // materialize the in-memory CLOG
    std::filesystem::remove_all(std::filesystem::path(failureDb) / "pg_xact");
    if (engine.commitTransaction() != DBStatus::IO_ERROR) return 1;
    if (engine.inTransaction()) return 1;
    if (!engine.query(failureDb, "t", {"=id 1"}, {"id"}).empty()) return 1;
    {
        StorageEngine recovered;
        if (!recovered.query(failureDb, "t", {"=id 1"}, {"id"}).empty()) return 1;
    }
    std::cout << "[CLOG INTEGRATION TEST] commit persistence failure fails closed\n";

    std::cout << "[CLOG INTEGRATION TEST] passed\n";

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(failureDb);
    std::filesystem::remove_all(".txnid");
    return 0;
}
