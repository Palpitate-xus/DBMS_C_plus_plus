#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "storage/CommitLog.h"
#include "storage/WAL.h"

#include <cassert>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

dbms::Config g_config;

namespace {

using namespace dbms;

void waitAfter(uint64_t epoch) {
    while (static_cast<uint64_t>(std::time(nullptr)) <= epoch)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
}

uint64_t insert(StorageEngine& engine, const std::string& database,
                int id, const std::string& value) {
    assert(engine.beginTransaction(database) == DBStatus::OK);
    assert(engine.insert(database, "items",
                         {{"id", std::to_string(id)}, {"value", value}}) ==
           DBStatus::OK);
    const uint64_t xid = engine.currentTxnId();
    assert(engine.commitTransaction() == DBStatus::OK);
    return xid;
}

void expectRow(StorageEngine& engine, const std::string& database,
               int id, const std::string& value) {
    const auto rows = engine.query(database, "items",
                                   {"=id " + std::to_string(id)},
                                   {"id", "value"});
    if (rows.size() != 1) {
        std::cerr << "PITR forked archive row mismatch: id=" << id
                  << " count=" << rows.size() << '\n';
        for (const auto& row : engine.query(database, "items", {},
                                            {"id", "value"}))
            std::cerr << "  actual: " << row << '\n';
    }
    assert(rows.size() == 1);
    assert(rows.front().find(value) != std::string::npos);
    BPTree* primary = engine.getPKIndex(database, "items");
    BPTree* secondary = engine.getSecondaryIndex(database, "items", "value");
    int64_t rid = -1;
    assert(primary && primary->search(std::to_string(id), rid));
    assert(secondary);
    const auto secondaryRids = secondary->searchMulti(value);
    assert(secondaryRids.size() == 1 && secondaryRids.front() == rid);
}

} // namespace

void testForkedBackup(bool forkBase, bool initialFork = true) {
    using namespace dbms;
    const std::string database = "pitr_forked_archive_db";
    const std::string backup = "pitr_forked_archive_image";
    const std::string archive = "pitr_forked_archive_material";
    std::filesystem::remove_all(database);
    std::filesystem::remove_all(backup);
    std::filesystem::remove_all(archive);
    std::filesystem::remove_all(database + ".txn_backup");

    uint64_t firstDiscardXid = 0;
    uint64_t firstTarget = 0;
    {
        StorageEngine engine;
        assert(engine.createDatabase(database) == DBStatus::OK);
        TableSchema table;
        table.tablename = "items";
        table.formatVersion = 2;
        table.append(makeIntColumn("id", false, 4, true));
        table.append(makeVarCharColumn("value", false, 128, false));
        assert(engine.createTable(database, table) == DBStatus::OK);
        assert(engine.createIndex(database, "items", "value") == DBStatus::OK);
        insert(engine, database, 1, "before_fork");
        if (initialFork) {
            firstTarget = static_cast<uint64_t>(std::time(nullptr));
            waitAfter(firstTarget);
            firstDiscardXid = insert(engine, database, 2, "first_discard");
            std::ofstream target(std::filesystem::path(database) / "pg_wal" /
                                     "recovery_target");
            target << firstTarget << '\n';
            assert(target.good());
        }
    }

    uint64_t keepXid = 0;
    uint64_t discardXid = 0;
    uint64_t secondTarget = 0;
    {
        StorageEngine forked;
        WALManager* wal = forked.getWAL(database);
        assert(wal && wal->timelineId() == (initialFork ? 2u : 1u));
        expectRow(forked, database, 1, "before_fork");
        assert(forked.query(database, "items", {"=id 2"}, {"id"}).empty());
        if (initialFork)
            assert(forked.getCommitLog(database)->getStatus(firstDiscardXid) ==
                   CommitLog::Status::Aborted);
        if (forkBase) insert(forked, database, 6, "fork_base_image");
        if (initialFork) {
            // Reserve a numerically newer segment name on the abandoned
            // stream, as the original PITR fixture reserves an unused TLI.
            // This is explicitly not a valid WAL history record. Only the
            // actual selected timeline may determine the backup overlap.
            std::ofstream reserved(std::filesystem::path(database) / "pg_wal" /
                                       "000000010000000000000100");
            reserved << "reserved abandoned-stream segment name\n";
            assert(reserved.good());
        }
        // Fork cases use a real durable PITR transition, not a selector edit
        // or synthetic heap copy. The ordinary case retains timeline 1.
        assert(forked.physicalBackup(database, backup));
        keepXid = insert(forked, database, 3, "fork_archive_keep");
        secondTarget = static_cast<uint64_t>(std::time(nullptr));
        waitAfter(secondTarget);
        discardXid = insert(forked, database, 4, "fork_archive_discard");
        assert(wal->XLogFlush(wal->currentWriteLsn()));
        const Lsn sealed = wal->switchWal();
        assert(sealed != INVALID_LSN);
        assert(wal->markSegmentsReadyBefore(sealed));
        assert(wal->archivePendingSegments(archive));
        assert(std::filesystem::is_regular_file(
            std::filesystem::path(archive) / wal->segmentPath(0).filename()));

        // Valid-looking foreign timelines and malformed segment names must
        // not become part of this backup's selected stream.
        if (initialFork) {
            std::filesystem::copy_file(
                std::filesystem::path(database) / "pg_wal" /
                    "000000010000000000000000",
                std::filesystem::path(archive) / "000000010000000000000000");
        } else {
            std::ofstream unrelated(std::filesystem::path(archive) /
                                        "000000020000000000000000");
            unrelated << "not the timeline 1 stream\n";
            assert(unrelated.good());
        }
        for (const auto& filename : {
                 "000000070000000000000100",
                 "00000002000000000000000G",
                 "000000020000000100000000"}) {
            std::ofstream unrelated(std::filesystem::path(archive) / filename,
                                    std::ios::binary);
            unrelated << "not a record in the selected stream\n";
            assert(unrelated.good());
        }
        assert(forked.pitrRestore(database, backup, archive, secondTarget));
        assert(std::filesystem::is_regular_file(
            std::filesystem::path(database) / "pg_wal" / "recovery_target"));
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "pg_wal" /
            "000000070000000000000100"));
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "pg_wal" /
            "00000002000000000000000G"));
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "pg_wal" /
            "000000020000000100000000"));
        if (!initialFork)
            assert(!std::filesystem::exists(
                std::filesystem::path(database) / "pg_wal" /
                "000000020000000000000000"));
    }
    {
        StorageEngine restored;
        expectRow(restored, database, 1, "before_fork");
        if (forkBase) expectRow(restored, database, 6, "fork_base_image");
        // This row exists only in the selected timeline's post-backup WAL.
        expectRow(restored, database, 3, "fork_archive_keep");
        assert(restored.query(database, "items", {"=id 2"}, {"id"}).empty());
        assert(restored.query(database, "items", {"=id 4"}, {"id"}).empty());
        CommitLog* clog = restored.getCommitLog(database);
        assert(clog && clog->getStatus(keepXid) == CommitLog::Status::Committed);
        assert(clog->getStatus(discardXid) == CommitLog::Status::Aborted);
        WALManager* wal = restored.getWAL(database);
        assert(wal && wal->timelineId() == (initialFork ? 3u : 2u));
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "pg_wal" / "recovery_target"));
        insert(restored, database, 5, "after_second_restore");
    }
    {
        StorageEngine restarted;
        expectRow(restarted, database, 1, "before_fork");
        if (forkBase) expectRow(restarted, database, 6, "fork_base_image");
        expectRow(restarted, database, 3, "fork_archive_keep");
        expectRow(restarted, database, 5, "after_second_restore");
        assert(restarted.query(database, "items", {"=id 4"}, {"id"}).empty());
        assert(restarted.getWAL(database)->timelineId() ==
               (initialFork ? 3u : 2u));
    }

    std::filesystem::remove_all(database);
    std::filesystem::remove_all(backup);
    std::filesystem::remove_all(archive);
    std::filesystem::remove_all(database + ".txn_backup");
    std::cout << "[PITR FORKED BACKUP] "
              << (initialFork ? (forkBase ? "populated" : "empty")
                              : "ordinary timeline 1")
              << " fork image, selected timeline archive, target, indexes, "
                 "CLOG and cold restart OK\n";
}

int main() {
    // Both histories run in one process. Keep its durable cluster-wide XID
    // allocator alive between cases rather than unlinking a live counter.
    std::filesystem::remove_all(".txnid");
    testForkedBackup(true);
    testForkedBackup(false);
    testForkedBackup(true, false);
    std::filesystem::remove_all(".txnid");
}
