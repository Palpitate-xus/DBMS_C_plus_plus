#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "storage/CommitLog.h"
#include "storage/WAL.h"

#include <cassert>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

dbms::Config g_config;

using namespace dbms;

namespace {

void assertRecoveredState(StorageEngine& engine,
                          const std::string& database) {
    const auto kept = engine.query(
        database, "items", {"=id 1"}, {"id", "name"});
    if (kept.size() != 1) {
        const auto allRows =
            engine.query(database, "items", {}, {"id", "name"});
        std::cerr << "PITR state mismatch: kept=" << kept.size()
                  << ", all=" << allRows.size() << '\n';
        for (const auto& row : allRows) std::cerr << "  " << row << '\n';
    }
    assert(kept.size() == 1);
    assert(kept.front().find("keep") != std::string::npos);
    assert(engine.query(database, "items", {"=id 2"}, {"id"}).empty());
    assert(engine.query(database, "items", {"=name keep"}, {"id"}).size() == 1);
    assert(engine.query(database, "items", {"=name replace"}, {"id"}).empty());
    assert(engine.query(database, "items", {"=name discard"}, {"id"}).empty());

    int64_t primaryRid = -1;
    BPTree* primary = engine.getPKIndex(database, "items");
    assert(primary && primary->search("1", primaryRid));
    int64_t discardedRid = -1;
    assert(!primary->search("2", discardedRid));

    BPTree* secondary = engine.getSecondaryIndex(database, "items", "name");
    assert(secondary);
    const auto secondaryRids = secondary->searchMulti("keep");
    assert(secondaryRids.size() == 1 && secondaryRids.front() == primaryRid);
    assert(secondary->searchMulti("replace").empty());
    assert(secondary->searchMulti("discard").empty());

    HashIndex* hash = engine.getHashIndex(database, "items", "name");
    assert(hash);
    const auto hashRids = hash->search("keep");
    assert(hashRids.size() == 1 && hashRids.front() == primaryRid);
    assert(hash->search("replace").empty());
    assert(hash->search("discard").empty());

    BloomIndex* bloom = engine.getBloomIndex(database, "items", "name");
    assert(bloom);
    const auto bloomRids = bloom->search("keep");
    assert(bloomRids.size() == 1 && bloomRids.front() == primaryRid);
    assert(bloom->search("replace").empty());
    assert(bloom->search("discard").empty());
}

} // namespace

int main() {
    const std::string database = "pitr_recovery_db";
    const std::string invalidDatabase = "pitr_invalid_target_db";
    std::filesystem::remove_all(database);
    std::filesystem::remove_all(invalidDatabase);
    std::filesystem::remove_all(".txnid");

    uint64_t keepXid = 0;
    uint64_t insertAfterTargetXid = 0;
    uint64_t updateAfterTargetXid = 0;
    uint64_t postRestoreXid = 0;
    uint64_t targetEpoch = 0;
    {
        StorageEngine engine;
        assert(engine.createDatabase(database) == DBStatus::OK);
        TableSchema table;
        table.tablename = "items";
        table.formatVersion = 2;
        table.append(makeIntColumn("id", false, 4, true));
        table.append(makeVarCharColumn("name", false, 64, false));
        assert(engine.createTable(database, table) == DBStatus::OK);
        assert(engine.createIndex(database, "items", "name") == DBStatus::OK);
        assert(engine.createHashIndex(database, "items", "name") == DBStatus::OK);
        assert(engine.createBloomIndex(database, "items", "name") == DBStatus::OK);

        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.insert(database, "items",
                             {{"id", "1"}, {"name", "keep"}}) ==
               DBStatus::OK);
        keepXid = engine.currentTxnId();
        assert(engine.commitTransaction() == DBStatus::OK);
        targetEpoch = static_cast<uint64_t>(std::time(nullptr));

        // Commit timestamps have one-second resolution. Move strictly past
        // the inclusive target before creating transactions to be undone.
        while (static_cast<uint64_t>(std::time(nullptr)) <= targetEpoch) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.insert(database, "items",
                             {{"id", "2"}, {"name", "discard"}}) ==
               DBStatus::OK);
        insertAfterTargetXid = engine.currentTxnId();
        assert(engine.commitTransaction() == DBStatus::OK);

        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.update(database, "items", {{"name", "replace"}},
                             {"=id 1"}) == DBStatus::OK);
        updateAfterTargetXid = engine.currentTxnId();
        assert(engine.commitTransaction() == DBStatus::OK);

        const auto targetPath =
            std::filesystem::path(database) / "pg_wal" / "recovery_target";
        std::ofstream target(targetPath, std::ios::trunc);
        target << targetEpoch << '\n';
        assert(target.good());
    }

    // Reserve timeline 2 to verify that PITR never reuses an existing
    // timeline's segment namespace.
    {
        std::ofstream reserved(
            std::filesystem::path(database) / "pg_wal" /
                "000000020000000000000000",
            std::ios::binary | std::ios::trunc);
        assert(reserved.good());
    }

    {
        StorageEngine recovered;
        CommitLog* clog = recovered.getCommitLog(database);
        assert(clog);
        assert(clog->getStatus(keepXid) == CommitLog::Status::Committed);
        assert(clog->getStatus(insertAfterTargetXid) ==
               CommitLog::Status::Aborted);
        assert(clog->getStatus(updateAfterTargetXid) ==
               CommitLog::Status::Aborted);
        assertRecoveredState(recovered, database);
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "pg_wal" /
            "recovery_target"));
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "pg_wal" /
            "recovery_fork"));
        WALManager* wal = recovered.getWAL(database);
        assert(wal && wal->timelineId() == 3);
    }
    // Simulate a crash after the durable timeline switch but before the two
    // recovery markers were consumed. Startup must finish that transition
    // without ever scanning the abandoned source timeline again.
    {
        const auto walDir =
            std::filesystem::path(database) / "pg_wal";
        std::ofstream target(walDir / "recovery_target",
                             std::ios::trunc);
        target << targetEpoch << '\n';
        assert(target.good());
        std::ofstream fork(walDir / "recovery_fork", std::ios::trunc);
        fork << "DBMS_PITR_FORK_V1\n" << targetEpoch << " 1 3\n";
        assert(fork.good());
    }
    // The consumed target must not allow a later ordinary restart to replay
    // the post-target commits again. New work belongs to the forked timeline
    // and must remain recoverable independently of the abandoned history.
    {
        StorageEngine restarted;
        assertRecoveredState(restarted, database);
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "pg_wal" /
            "recovery_target"));
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "pg_wal" /
            "recovery_fork"));
        assert(restarted.beginTransaction(database) == DBStatus::OK);
        assert(restarted.insert(database, "items",
                                {{"id", "3"},
                                 {"name", "post_restore"}}) ==
               DBStatus::OK);
        postRestoreXid = restarted.currentTxnId();
        assert(restarted.commitTransaction() == DBStatus::OK);
    }
    {
        StorageEngine restartedAgain;
        assertRecoveredState(restartedAgain, database);
        assert(restartedAgain.query(database, "items", {"=id 3"},
                                    {"id", "name"}).size() == 1);
        CommitLog* clog = restartedAgain.getCommitLog(database);
        assert(clog);
        assert(clog->getStatus(insertAfterTargetXid) ==
               CommitLog::Status::Aborted);
        assert(clog->getStatus(updateAfterTargetXid) ==
               CommitLog::Status::Aborted);
        assert(clog->getStatus(postRestoreXid) ==
               CommitLog::Status::Committed);
    }
    std::cout << "[PITR] inclusive target and durable timeline fork OK\n";

    std::filesystem::remove_all(database);
    {
        StorageEngine setup;
        assert(setup.createDatabase(invalidDatabase) == DBStatus::OK);
        TableSchema table;
        table.tablename = "t";
        table.formatVersion = 2;
        table.append(makeIntColumn("id", false, 4, true));
        assert(setup.createTable(invalidDatabase, table) == DBStatus::OK);
        assert(setup.beginTransaction(invalidDatabase) == DBStatus::OK);
        assert(setup.insert(invalidDatabase, "t", {{"id", "1"}}) ==
               DBStatus::OK);
        assert(setup.commitTransaction() == DBStatus::OK);
        const auto targetPath = std::filesystem::path(invalidDatabase) /
                                "pg_wal" / "recovery_target";
        std::ofstream target(targetPath, std::ios::trunc);
        target << "not-an-epoch\n";
        assert(target.good());
    }
    bool failedClosed = false;
    try {
        StorageEngine invalidTarget;
    } catch (const std::runtime_error&) {
        failedClosed = true;
    }
    assert(failedClosed);
    assert(std::filesystem::exists(
        std::filesystem::path(invalidDatabase) / "pg_wal" /
        "recovery_target"));
    std::cout << "[PITR] malformed target fails closed without consumption OK\n";

    std::filesystem::remove_all(invalidDatabase);
    std::filesystem::remove_all(".txnid");
    std::cout << "[PITR] all passed\n";
    return 0;
}
