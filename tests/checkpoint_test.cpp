// Checkpoint test: verify checkpoint record and persistent checkpoint file.

#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

int main() {
    std::string dbname = "checkpoint_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.formatVersion = 2;
        tbl.append(makeIntColumn("id", false, 0, true));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "1";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        engine.checkpoint(dbname);
    }

    // Verify checkpoint file exists with LSN.
    std::filesystem::path cpPath = std::filesystem::path(dbname) / "checkpoint";
    assert(std::filesystem::exists(cpPath));
    {
        std::ifstream cp(cpPath, std::ios::binary);
        uint64_t timestamp = 0, maxTxId = 0, ckptLsn = 0;
        cp.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
        cp.read(reinterpret_cast<char*>(&maxTxId), sizeof(maxTxId));
        cp.read(reinterpret_cast<char*>(&ckptLsn), sizeof(ckptLsn));
        assert(cp.gcount() == static_cast<std::streamsize>(sizeof(ckptLsn)));
        assert(ckptLsn > 0);
        std::cout << "[CHECKPOINT] checkpoint file contains LSN " << ckptLsn << "\n";
    }

    // Verify WAL checkpoint record.
    std::filesystem::path walDir = std::filesystem::path(dbname) / "pg_wal";
    WALManager wal(walDir);
    assert(wal.ensureOpen());
    auto ckptLsnOpt = wal.findLastCheckpointLsn();
    assert(ckptLsnOpt.has_value());
    auto recOpt = wal.ReadRecord(*ckptLsnOpt);
    assert(recOpt.has_value());
    assert(recOpt->rmid() == RM_CHECKPOINT_ID);
    assert(recOpt->info() == XLOG_CHECKPOINT_SHUTDOWN);
    assert(recOpt->data.size() >= sizeof(uint64_t));
    std::cout << "[CHECKPOINT] WAL checkpoint record found at LSN " << *ckptLsnOpt << "\n";

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[CHECKPOINT] all passed\n";
    return 0;
}
