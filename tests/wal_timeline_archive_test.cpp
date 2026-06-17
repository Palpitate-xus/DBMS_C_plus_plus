// WAL timeline and archive_status test.

#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <filesystem>
#include <iostream>
#include <fstream>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

int main() {
    std::string dbname = "wal_timeline_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    // Phase 1: default timeline 1.
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
    }

    std::filesystem::path walDir = std::filesystem::path(dbname) / "pg_wal";

    {
        WALManager wal(walDir);
        assert(wal.ensureOpen());
        assert(wal.timelineId() == 1);
        assert(std::filesystem::exists(walDir / "timeline"));
        assert(std::filesystem::exists(walDir / "archive_status"));

        // Reset WAL and switch to timeline 2 for a clean test of TLI filenames.
        wal.reset();
        assert(wal.setTimeline(2));
        assert(wal.timelineId() == 2);
    }

    // Phase 2: write on timeline 2.
    {
        StorageEngine engine;

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "2";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    // Verify segment uses timeline 2 and archive workflow.
    {
        WALManager wal(walDir);
        assert(wal.ensureOpen());
        assert(wal.timelineId() == 2);

        bool foundTli2 = false;
        for (const auto& entry : std::filesystem::directory_iterator(walDir)) {
            std::string name = entry.path().filename().string();
            if (name.size() == 24) {
                uint32_t tli = static_cast<uint32_t>(
                    std::stoull(name.substr(0, 8), nullptr, 16));
                if (tli == 2) foundTli2 = true;
            }
        }
        assert(foundTli2);

        assert(std::filesystem::exists(wal.segmentPath(0)));
        assert(!wal.isSegmentArchived(0));

        assert(wal.markSegmentReadyForArchive(0));
        std::filesystem::path archiveDir = std::filesystem::path(dbname) / "wal_archive";
        assert(wal.archivePendingSegments(archiveDir));

        assert(wal.isSegmentArchived(0));
        assert(std::filesystem::exists(archiveDir / wal.segmentPath(0).filename()));
        assert(wal.pendingArchiveSegments().empty());
    }

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[WAL TIMELINE ARCHIVE] all passed\n";
    return 0;
}
