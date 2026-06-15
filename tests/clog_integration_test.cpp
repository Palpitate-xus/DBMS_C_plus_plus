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

    std::cout << "[CLOG INTEGRATION TEST] passed\n";

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");
    return 0;
}
