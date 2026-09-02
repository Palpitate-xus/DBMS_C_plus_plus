#include "storage/CommitLog.h"
#include "storage/WAL.h"
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

    // Once COMMIT WAL is durable, a later CLOG publication failure cannot
    // reverse the decision.  The live backend keeps the committed status in
    // memory and startup reconstructs the missing CLOG from the sole terminal
    // WAL record; no contradictory ABORT may be appended.
    const std::string failureDb = "clog_commit_failure_db";
    std::filesystem::remove_all(failureDb);
    if (engine.createDatabase(failureDb) != DBStatus::OK) return 1;
    TableSchema failureTable;
    failureTable.tablename = "t";
    failureTable.append(makeIntColumn("id", false, 0, true));
    if (engine.createTable(failureDb, failureTable) != DBStatus::OK) return 1;
    if (engine.beginTransaction(failureDb) != DBStatus::OK) return 1;
    if (engine.insert(failureDb, "t", {{"id", "1"}}) != DBStatus::OK) return 1;
    const uint64_t failureXid = engine.currentTxnId();
    (void)engine.getCommitLog(failureDb); // materialize the in-memory CLOG
    std::filesystem::remove_all(std::filesystem::path(failureDb) / "pg_xact");
    if (engine.commitTransaction() != DBStatus::OK) return 1;
    if (engine.inTransaction()) return 1;
    if (engine.query(failureDb, "t", {"=id 1"}, {"id"}).size() != 1) return 1;

    size_t commitRecords = 0;
    size_t abortRecords = 0;
    WALManager* failureWal = engine.getWAL(failureDb);
    if (!failureWal) return 1;
    for (Lsn lsn = failureWal->earliestAvailableLsn();;) {
        const auto record = failureWal->ReadRecord(lsn);
        if (!record || record->header.xl_tot_len == 0) break;
        if (record->rmid() == RM_XACT_ID &&
            record->header.xl_xid == failureXid) {
            if (record->info() == XLOG_XACT_COMMIT) ++commitRecords;
            if (record->info() == XLOG_XACT_ABORT) ++abortRecords;
        }
        lsn += record->header.xl_tot_len;
    }
    if (commitRecords != 1 || abortRecords != 0) return 1;
    {
        StorageEngine recovered;
        if (recovered.query(failureDb, "t", {"=id 1"}, {"id"}).size() != 1) return 1;
        CommitLog* recoveredClog = recovered.getCommitLog(failureDb);
        if (!recoveredClog ||
            recoveredClog->getStatus(failureXid) !=
                CommitLog::Status::Committed) return 1;
    }
    std::cout << "[CLOG INTEGRATION TEST] durable COMMIT survives CLOG publication failure\n";

    std::cout << "[CLOG INTEGRATION TEST] passed\n";

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(failureDb);
    std::filesystem::remove_all(".txnid");
    return 0;
}
