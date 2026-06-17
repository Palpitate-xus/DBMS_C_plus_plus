// WAL full page write test: verify checkpoint and subsequent page images.

#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <filesystem>
#include <iostream>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

int main() {
    std::string dbname = "wal_fpw_db";
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
        tbl.append(makeVarCharColumn("name", false, 50, false));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);

        // Insert first row and commit.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "1";
        vals["name"] = "alice";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        // Checkpoint.
        engine.checkpoint(dbname);

        // Insert second row and commit.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        vals["id"] = "2";
        vals["name"] = "bob";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    std::filesystem::path walDir = std::filesystem::path(dbname) / "pg_wal";
    WALManager wal(walDir);
    assert(wal.ensureOpen());

    bool foundCheckpoint = false;
    bool foundPostCheckpointImage = false;
    bool postCheckpoint = false;
    Lsn checkpointLsn = 0;
    Lsn lsn = 0;
    while (true) {
        auto recOpt = wal.ReadRecord(lsn);
        if (!recOpt) break;
        const XLogRecord& rec = *recOpt;
        if (rec.rmid() == RM_CHECKPOINT_ID) {
            foundCheckpoint = true;
            checkpointLsn = lsn;
            postCheckpoint = true;
        }
        if (postCheckpoint && rec.rmid() == RM_HEAP_ID &&
            (rec.info() == XLOG_HEAP_PAGE_BEFORE || rec.info() == XLOG_HEAP_PAGE_AFTER)) {
            foundPostCheckpointImage = true;
            // Verify the page image length matches 8KB page size.
            const char* p = rec.data.data() + sizeof(uint32_t); // skip nameLen
            const char* end = rec.data.data() + rec.data.size();
            uint32_t nameLen = 0;
            std::memcpy(&nameLen, rec.data.data(), sizeof(nameLen));
            p += nameLen + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint16_t);
            assert(p + sizeof(uint32_t) <= end);
            uint32_t pageLen = 0;
            std::memcpy(&pageLen, p, sizeof(pageLen));
            assert(pageLen == 8192);
        }
        lsn += rec.header.xl_tot_len;
    }
    assert(foundCheckpoint);
    assert(foundPostCheckpointImage);
    assert(checkpointLsn > 0);
    std::cout << "[WAL FPW] checkpoint record and post-checkpoint page image found\n";

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[WAL FPW] all passed\n";
    return 0;
}
