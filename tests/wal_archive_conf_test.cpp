// WAL archiver configuration test: archive_command parsing (quoted values
// with embedded '='), disabled-by-default behaviour, and the built-in
// "dir:" destination resolution used by checkpoint + bgwriter archiving.

#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cassert>

// Use the weak g_config from tests/test_stubs.cpp instead of defining a
// strong duplicate: a strong definition here plus the weak stub both
// register the same static object's construction path under different
// Config layouts (the stub may be compiled against an older header in
// incremental runs), which produced a double-free in ~Config at exit.
extern dbms::Config g_config;

using namespace dbms;

static void writeConfig(const std::string& body) {
    std::ofstream ofs("dbms.conf", std::ios::trunc);
    ofs << body;
}

int main() {
    std::cerr << "[ARCHIVE CONF] starting test\n";

    // 1. Default: archiving disabled, empty destination.
    {
        Config cfg;
        assert(cfg.archiveCommand.empty());
    }

    // 2. Loader accepts a single-quoted value with spaces and '='.
    {
        writeConfig("archive_command='cp %p /archive/%f --opt=x'\n");
        Config cfg;
        assert(cfg.load("dbms.conf"));
        assert(cfg.archiveCommand == "cp %p /archive/%f --opt=x");
    }

    // 3. Loader accepts the built-in dir: form.
    {
        writeConfig("archive_command='dir:/tmp/archive_conf_test'\n");
        Config cfg;
        assert(cfg.load("dbms.conf"));
        assert(cfg.archiveCommand == "dir:/tmp/archive_conf_test");
    }

    // 4. Unquoted values with '=' remain rejected (legacy strictness).
    {
        writeConfig("checkpoint_interval=1\nbad_key=a=b\n");
        Config cfg;
        assert(!cfg.load("dbms.conf"));
    }

    // 5. Save emits a round-trippable quoted archive_command.
    {
        Config cfg;
        cfg.archiveCommand = "dir:/tmp/some archive";
        assert(cfg.save("dbms.conf"));
        Config reloaded;
        assert(reloaded.load("dbms.conf"));
        assert(reloaded.archiveCommand == "dir:/tmp/some archive");
    }

    // 6. End-to-end through the engine: a checkpoint with archive_command
    //    set archives .ready segments into the destination directory and
    //    flips the marker to .done.
    {
        const std::string dbname = "archive_conf_db";
        std::filesystem::remove_all(dbname);
        std::filesystem::remove_all(dbname + ".txn_backup");
        std::filesystem::remove_all(".txnid");
        const std::string archiveDir = "/tmp/archive_conf_test_dest";
        std::filesystem::remove_all(archiveDir);

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

        // Mark segment 0 ready by hand (a real fill requires crossing the
        // 16 MiB boundary; the marker->archive->done transition is what
        // this test exercises).  A standalone WALManager is used while no
        // StorageEngine is alive, so the two never contend on wal.lock.
        {
            WALManager wal(std::filesystem::path(dbname) / "pg_wal");
            assert(wal.ensureOpen());
            assert(wal.markSegmentReadyForArchive(0));
        }
        const auto statusDir =
            std::filesystem::path(dbname) / "pg_wal" / "archive_status";
        bool sawReady = false;
        for (const auto& e : std::filesystem::directory_iterator(statusDir)) {
            if (e.path().filename().string().size() == 30 &&
                e.path().filename().string().substr(24) == ".ready") sawReady = true;
        }
        assert(sawReady);

        // A fresh engine checkpoint with archiving configured must archive
        // the ready segment and flip the marker.
        {
            StorageEngine engine;
            g_config.archiveCommand = "dir:" + archiveDir;
            assert(engine.checkpoint(dbname));

            assert(std::filesystem::exists(
                std::filesystem::path(archiveDir) /
                "000000010000000000000000"));
            WALManager wal(std::filesystem::path(dbname) / "pg_wal");
            assert(wal.ensureOpen());
            assert(wal.isSegmentArchived(0));
            assert(wal.pendingArchiveSegments().empty());
        }
        std::filesystem::remove_all(dbname);
        std::filesystem::remove_all(dbname + ".archive");
        std::filesystem::remove_all(archiveDir);
    }

    std::filesystem::remove("dbms.conf");
    std::cerr << "[ARCHIVE CONF] all passed\n";
    return 0;
}
