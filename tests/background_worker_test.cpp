// Background worker (walwriter / bgwriter / checkpointer) test.

#include "TableManage.h"
#include "Config.h"
#include <filesystem>
#include <iostream>
#include <fstream>
#include <thread>
#include <chrono>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

static uint64_t readCheckpointLsn(const std::filesystem::path& cpPath) {
    if (!std::filesystem::exists(cpPath)) return 0;
    std::ifstream cp(cpPath, std::ios::binary);
    uint64_t timestamp = 0, maxTxId = 0, ckptLsn = 0;
    cp.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
    cp.read(reinterpret_cast<char*>(&maxTxId), sizeof(maxTxId));
    cp.read(reinterpret_cast<char*>(&ckptLsn), sizeof(ckptLsn));
    return ckptLsn;
}

int main() {
    std::string dbname = "background_worker_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::filesystem::path cpPath = std::filesystem::path(dbname) / "checkpoint";

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.formatVersion = 2;
        tbl.append(makeIntColumn("id", false, 0, true));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);

        // Use short intervals so the background checkpointer runs quickly.
        engine.setBackgroundIntervals(50, 200);

        // Initial checkpoint LSN should be zero before any checkpoint.
        assert(readCheckpointLsn(cpPath) == 0);

        // Insert a row and commit.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "1";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        // Wake the worker and wait long enough for the checkpointer to fire.
        engine.wakeBackgroundWorker();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        uint64_t lsn = readCheckpointLsn(cpPath);
        std::cout << "[BACKGROUND] checkpoint LSN after sleep=" << lsn << "\n";
        assert(lsn > 0);

        // Verify the row is still visible (recovery did not break data).
        auto rows = engine.query(dbname, "t", {}, {"id"});
        assert(rows.size() == 1);
    }

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[BACKGROUND] all passed\n";
    return 0;
}
