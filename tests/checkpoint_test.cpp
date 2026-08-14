// Checkpoint test: verify checkpoint record and persistent checkpoint file.

#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cassert>
#include <cstring>

dbms::Config g_config;

using namespace dbms;

int main() {
    // A pinned frame must not be force-evicted, and a dirty frame must survive
    // a normal clock-sweep eviction and reload from disk.
    {
        const std::filesystem::path poolPath = "checkpoint_buffer_pool.dat";
        std::filesystem::remove(poolPath);
        BufferPool pool(poolPath.string(), 1, 128);
        assert(pool.open());
        char* page = pool.fetchPage(0);
        assert(page != nullptr);
        std::memcpy(page, "durable-page", 12);
        pool.markDirty(0);
        pool.unpinPage(0);

        char* other = pool.fetchPage(1);
        assert(other != nullptr);
        pool.unpinPage(1);
        char* reloaded = pool.fetchPage(0);
        assert(reloaded != nullptr);
        assert(std::memcmp(reloaded, "durable-page", 12) == 0);

        // Keep page 0 pinned.  With a one-frame pool, fetching page 2 must
        // fail closed instead of evicting the live page.
        assert(pool.fetchPage(2) == nullptr);
        pool.unpinPage(0);
        assert(pool.fetchPage(2) != nullptr);
        pool.unpinPage(2);
        assert(pool.flush());
        pool.close();
        std::filesystem::remove(poolPath);
        std::cout << "[CHECKPOINT] BufferPool eviction/pin safety OK\n";
    }

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
        assert(!engine.checkpoint(dbname));
        assert(engine.rollbackTransaction() == DBStatus::OK);

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        vals["id"] = "1";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        assert(engine.checkpoint(dbname));
    }

    // Verify checkpoint file exists with LSN.
    std::filesystem::path cpPath = std::filesystem::path(dbname) / "checkpoint";
    assert(std::filesystem::exists(cpPath));
    assert(std::filesystem::file_size(cpPath) == 3 * sizeof(uint64_t));
    {
        std::ifstream cp(cpPath, std::ios::binary);
        uint64_t timestamp = 0, maxTxId = 0, ckptLsn = 0;
        cp.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
        cp.read(reinterpret_cast<char*>(&maxTxId), sizeof(maxTxId));
        cp.read(reinterpret_cast<char*>(&ckptLsn), sizeof(ckptLsn));
        assert(cp.gcount() == static_cast<std::streamsize>(sizeof(ckptLsn)));
        assert(cp.peek() == std::char_traits<char>::eof());
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
