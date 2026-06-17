// WAL basic test: verify segment file creation and record structure.

#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <filesystem>
#include <iostream>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

int main() {
    std::cerr << "[WAL BASIC] starting test\n";
    std::string dbname = "wal_basic_db";
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

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "1";
        vals["name"] = "alice";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    // Read and verify WAL records.
    std::filesystem::path walDir = std::filesystem::path(dbname) / "pg_wal";
    WALManager wal(walDir);
    assert(wal.ensureOpen());

    // Verify WAL directory and segment file exist.
    std::cerr << "[WAL BASIC] walDir exists=" << std::filesystem::exists(walDir)
              << " currentLsn=" << wal.currentWriteLsn() << "\n";
    assert(std::filesystem::exists(walDir));
    bool foundSegment = false;
    for (const auto& entry : std::filesystem::directory_iterator(walDir)) {
        std::string name = entry.path().filename().string();
        if (name.size() == 24) {
            foundSegment = true;
            break;
        }
    }
    assert(foundSegment);
    std::cout << "[WAL BASIC] WAL segment file exists\n";

    bool foundBefore = false;
    bool foundAfter = false;
    bool foundCommit = false;
    Lsn lsn = 0;
    int recCount = 0;
    while (true) {
        auto recOpt = wal.ReadRecord(lsn);
        if (!recOpt) break;
        const XLogRecord& rec = *recOpt;
        ++recCount;
        std::cerr << "[WAL BASIC] record rmid=" << (int)rec.rmid()
                  << " info=" << (int)rec.info()
                  << " xid=" << rec.header.xl_xid
                  << " len=" << rec.header.xl_tot_len << "\n";
        if (rec.rmid() == RM_HEAP_ID && rec.info() == XLOG_HEAP_PAGE_BEFORE) {
            foundBefore = true;
        } else if (rec.rmid() == RM_HEAP_ID && rec.info() == XLOG_HEAP_PAGE_AFTER) {
            foundAfter = true;
            // Verify payload contains table name and page image.
            assert(rec.data.size() > sizeof(uint32_t));
            uint32_t nameLen = 0;
            std::memcpy(&nameLen, rec.data.data(), sizeof(nameLen));
            assert(nameLen == 1); // "t"
            assert(rec.data[sizeof(uint32_t)] == 't');
        } else if (rec.rmid() == RM_XACT_ID && rec.info() == XLOG_XACT_COMMIT) {
            foundCommit = true;
        }
        lsn += rec.header.xl_tot_len;
    }
    std::cout << "[WAL BASIC] total records: " << recCount << "\n";
    assert(foundBefore);
    assert(foundAfter);
    assert(foundCommit);
    std::cout << "[WAL BASIC] HEAP_PAGE_BEFORE / AFTER / XACT_COMMIT records found\n";

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[WAL BASIC] all passed\n";
    return 0;
}
