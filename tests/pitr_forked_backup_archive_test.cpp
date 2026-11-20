#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "storage/CommitLog.h"
#include "storage/WAL.h"

#include <array>
#include <cassert>
#include <chrono>
#include <cstring>
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

std::string readFile(const std::filesystem::path& path) {
    std::ifstream file(path, std::ios::binary);
    assert(file);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

void flipByte(const std::filesystem::path& path, std::streamoff offset) {
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    file.seekg(offset);
    char byte = '\0';
    assert(file.get(byte));
    file.seekp(offset);
    assert(file.put(byte ^ 1));
}

} // namespace

void testForkedBackup(bool forkBase, bool initialFork = true) {
    using namespace dbms;
    const std::string database = "pitr_forked_archive_db";
    const std::string backup = "pitr_forked_archive_image";
    const std::string archive = initialFork && !forkBase
        ? database + ".archive" : "pitr_forked_archive_material";
    const std::string allocatorDb = "pitr_allocator_control";
    const bool verifyFailedRestore = forkBase && initialFork;
    std::filesystem::remove_all(database);
    std::filesystem::remove_all(backup);
    std::filesystem::remove_all(archive);
    std::filesystem::remove_all(database + ".txn_backup");
    std::filesystem::remove_all(allocatorDb);

    uint64_t firstDiscardXid = 0;
    uint64_t firstTarget = 0;
    {
        StorageEngine engine;
        assert(engine.createDatabase(database) == DBStatus::OK);
        if (verifyFailedRestore)
            assert(engine.createDatabase(allocatorDb) == DBStatus::OK);
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
            auto* wal = engine.getWAL(database);
            assert(wal && wal->switchWal() == WALManager::kSegmentSize);
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
            // This genuine abandoned segment has a higher segment number
            // than the backup's selected stream. Only the selected timeline
            // may determine where to merge the external archive.
            assert(std::filesystem::is_regular_file(
                std::filesystem::path(database) / "pg_wal" / "000000010000000000000001"));
        }
        // Fork cases use a real durable PITR transition, not a selector edit
        // or synthetic heap copy. The ordinary case retains timeline 1.
        assert(forked.physicalBackup(database, backup));
        keepXid = insert(forked, database, 3, "fork_archive_keep");
        secondTarget = static_cast<uint64_t>(std::time(nullptr));
        waitAfter(secondTarget);
        discardXid = insert(forked, database, 4, "fork_archive_discard");
        const uint64_t archiveOnlyXid = discardXid + 65536;
        if (verifyFailedRestore) {
            // An archive sourced from a later allocator can have XIDs
            // beyond anything in the base image or this process's state.
            const std::array<uint64_t, 2> commit{archiveOnlyXid, secondTarget + 1};
            const auto* payload = reinterpret_cast<const char*>(commit.data());
            assert(wal->XLogInsert(RM_XACT_ID, XLOG_XACT_COMMIT, archiveOnlyXid,
                     std::vector<char>(payload, payload + sizeof(commit))) != INVALID_LSN);
        }
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
        const auto selectedName = wal->segmentPath(0).filename();
        const auto archiveBytes = readFile(std::filesystem::path(archive) / selectedName);
        if (verifyFailedRestore) {
            const auto live = std::filesystem::path(database);
            const auto invalidSegment = live / "pg_wal" / "000000010000000000000100";
            const auto invalidBackup = backup + "_invalid_wal";
            {
                std::ofstream file(invalidSegment);
                file << "reserved abandoned-stream segment name\n";
                assert(file.good());
            }
            assert(forked.physicalBackup(database, invalidBackup));
            assert(std::filesystem::remove(invalidSegment));
            const auto heap = readFile(live / "items.dt");
            const auto baseManifest = readFile(std::filesystem::path(backup) / ".dbms_backup_manifest");
            const auto unchanged = [&] {
                assert(readFile(live / "items.dt") == heap);
                assert(readFile(std::filesystem::path(backup) / ".dbms_backup_manifest") == baseManifest);
                assert(!std::filesystem::exists(live / "pg_wal" / "recovery_target"));
                expectRow(forked, database, 4, "fork_archive_discard");
            };
            // The old fixture's fabricated in-backup segment still fails
            // validation. Real abandoned segments remain in the positive case.
            assert(!forked.pitrRestore(database, invalidBackup, archive, secondTarget));
            unchanged();
            std::filesystem::remove_all(invalidBackup);

            const auto damaged = std::filesystem::path(archive + "_damaged");
            assert(!std::filesystem::exists(damaged));
            assert(!forked.pitrRestore(database, backup, damaged.string(), secondTarget));
            unchanged();

            std::filesystem::create_directory(damaged);
            const auto segment = damaged / selectedName;
            for (const auto offset : {std::streamoff(sizeof(XLogRecHeader)),
                                      std::streamoff(WALManager::kSegmentSize - 1)}) {
                std::filesystem::copy_file(std::filesystem::path(archive) / selectedName,
                                           segment, std::filesystem::copy_options::overwrite_existing);
                flipByte(segment, offset);
                assert(!forked.pitrRestore(database, backup, damaged.string(), secondTarget));
                unchanged();
            }
            std::filesystem::remove_all(damaged);
        }
        assert(forked.pitrRestore(database, backup, archive, secondTarget));
        assert(readFile(std::filesystem::path(archive) / selectedName) == archiveBytes);
        if (verifyFailedRestore) {
            assert(forked.beginTransaction(allocatorDb) == DBStatus::OK);
            assert(forked.currentTxnId() > archiveOnlyXid);
            assert(forked.rollbackTransaction() == DBStatus::OK);
        }
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
    std::filesystem::remove_all(allocatorDb);
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
