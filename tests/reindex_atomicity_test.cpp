#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "storage/PageCrypto.h"
#include "test_utils.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

struct TestRow {
    std::string id;
    std::string tag;
    std::string part;
};

void createIndexedTable(const std::string& database,
                        const std::vector<TestRow>& rows) {
    using dbms::DBStatus;

    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "t";
    table.formatVersion = 2;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeVarCharColumn("tag", false, 40));
    table.append(dbms::makeVarCharColumn("part", false, 40));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    assert(g_engine.createIndex(database, "t", "tag") == DBStatus::OK);
    assert(g_engine.createCompositeIndex(
               database, "t", {"tag", "part"}, "tag_part_idx") ==
           DBStatus::OK);
    for (const auto& row : rows) {
        assert(g_engine.insert(
                   database, "t",
                   {{"id", row.id}, {"tag", row.tag}, {"part", row.part}}) ==
               DBStatus::OK);
    }
}

void assertIndexed(const std::string& database,
                   const std::vector<TestRow>& rows) {
    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    dbms::BPTree* secondary =
        g_engine.getSecondaryIndex(database, "t", "tag");
    dbms::BPTree* composite =
        g_engine.getCompositeIndexTree(database, "t", "tag_part_idx");
    assert(primary && secondary && composite);

    for (const auto& row : rows) {
        int64_t rid = -1;
        assert(primary->search(row.id, rid));
        const auto secondaryRids = secondary->searchMulti(row.tag);
        const auto compositeRids = composite->searchMulti(
            row.tag + std::string(1, '\x01') + row.part);
        assert(secondaryRids.size() == 1 && secondaryRids.front() == rid);
        assert(compositeRids.size() == 1 && compositeRids.front() == rid);
    }
}

void assertNotIndexed(const std::string& database, const TestRow& row) {
    dbms::BPTree* primary = g_engine.getPKIndex(database, "t");
    dbms::BPTree* secondary =
        g_engine.getSecondaryIndex(database, "t", "tag");
    dbms::BPTree* composite =
        g_engine.getCompositeIndexTree(database, "t", "tag_part_idx");
    assert(primary && secondary && composite);

    int64_t rid = -1;
    assert(!primary->search(row.id, rid));
    assert(secondary->searchMulti(row.tag).empty());
    assert(composite->searchMulti(
               row.tag + std::string(1, '\x01') + row.part).empty());
}

void assertNoReindexArtifacts(const std::string& database) {
    for (const auto& entry : std::filesystem::directory_iterator(database)) {
        const std::string filename = entry.path().filename().string();
        assert(filename.find(".reindex.tmp.") == std::string::npos);
        assert(filename.find(".reindex.old.") == std::string::npos);
        assert(filename.find(".reindex_swap") == std::string::npos);
    }
}

void test_failed_build_preserves_live_indexes() {
    using dbms::DBStatus;

    const std::string testName = "reindex_failure_atomicity";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    const std::vector<TestRow> rows = {
        {"1", "alpha", "one"}, {"2", "bravo", "two"}};
    createIndexedTable(database, rows);
    assertIndexed(database, rows);

    // Ensure the process-lock directory and this table's lock file already
    // exist before making the relation directory read-only.
    auto& locks = g_engine.getLockManager();
    locks.setResourceNamespace(database);
    assert(locks.lockMetadata("t"));
    locks.unlock("t");

    std::error_code error;
    const auto originalPermissions =
        std::filesystem::status(database, error).permissions();
    assert(!error);
    constexpr auto writePermissions =
        std::filesystem::perms::owner_write |
        std::filesystem::perms::group_write |
        std::filesystem::perms::others_write;
    std::filesystem::permissions(
        database, writePermissions, std::filesystem::perm_options::remove,
        error);
    assert(!error);

    const DBStatus status = g_engine.reindex(database, "t");

    // Restore access before asserting so a deliberately failing result never
    // prevents test cleanup or the successful retry below.
    error.clear();
    std::filesystem::permissions(
        database, originalPermissions,
        std::filesystem::perm_options::replace, error);
    assert(!error);

    assert(status == DBStatus::IO_ERROR);
    assertIndexed(database, rows);
    assertNoReindexArtifacts(database);

    assert(g_engine.reindex(database, "t") == DBStatus::OK);
    assertIndexed(database, rows);
    assertNoReindexArtifacts(database);

    // REINDEX must include the current transaction's eagerly indexed INSERT.
    // ROLLBACK then removes it from the replacement just as it would from the
    // original tree.
    const TestRow rolledBack = {"3", "charlie", "three"};
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.insert(
               database, "t",
               {{"id", rolledBack.id},
                {"tag", rolledBack.tag},
                {"part", rolledBack.part}}) == DBStatus::OK);
    assert(g_engine.reindex(database, "t") == DBStatus::OK);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assertIndexed(database, rows);
    assertNotIndexed(database, rolledBack);

    // Internal DDL transactions already own the exclusive database snapshot
    // lock and remain able to invoke REINDEX during table rewrites.
    assert(g_engine.beginTransaction(database, true) == DBStatus::OK);
    assert(g_engine.reindex(database, "t") == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assertIndexed(database, rows);

    cleanupTestDb(testName);
}

void test_missing_live_index_generation_is_rebuilt() {
    using dbms::DBStatus;

    const std::string testName = "reindex_missing_generation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    dbms::PageCrypto::disable();
    createIndexedTable(database, {});
    assert(g_engine.checkpoint(database));

    // ALTER-style heap rewrites can leave an empty table with no B-tree main
    // files while stale sidecars from the previous generation still exist.
    // REINDEX must publish complete replacement pairs instead of rejecting
    // that recoverable state.
    const std::vector<std::filesystem::path> indexPaths = {
        std::filesystem::path(database) / "t.idx",
        std::filesystem::path(database) / "t_tag.idx",
        std::filesystem::path(database) / "t.idx_tag_part_idx"};
    for (const auto& path : indexPaths) {
        assert(std::filesystem::remove(path));
        assert(std::filesystem::is_regular_file(path.string() + ".tde"));
    }

    assert(g_engine.reindex(database, "t") == DBStatus::OK);
    for (const auto& path : indexPaths) {
        assert(std::filesystem::is_regular_file(path));
        assert(std::filesystem::is_regular_file(path.string() + ".tde"));
    }
    const std::vector<TestRow> rows = {{"1", "alpha", "one"}};
    assert(g_engine.insert(
               database, "t",
               {{"id", "1"}, {"tag", "alpha"}, {"part", "one"}}) ==
           DBStatus::OK);
    assertIndexed(database, rows);
    assertNoReindexArtifacts(database);
    cleanupTestDb(testName);
}

void test_peer_cache_reopens_replaced_generation() {
    using dbms::DBStatus;

    const std::string testName = "reindex_peer_cache";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    const TestRow first = {"1", "alpha", "one"};
    const TestRow second = {"2", "bravo", "two"};
    createIndexedTable(database, {first});
    assert(g_engine.checkpoint(database));

    {
        dbms::StorageEngine peer;
        dbms::BPTree* peerPrimary = peer.getPKIndex(database, "t");
        dbms::BPTree* peerSecondary =
            peer.getSecondaryIndex(database, "t", "tag");
        dbms::BPTree* peerComposite =
            peer.getCompositeIndexTree(database, "t", "tag_part_idx");
        assert(peerPrimary && peerSecondary && peerComposite);
        int64_t firstRid = -1;
        assert(peerPrimary->search(first.id, firstRid));
        assert(peerSecondary->searchMulti(first.tag).size() == 1);
        assert(peerComposite->searchMulti(
                   first.tag + std::string(1, '\x01') + first.part).size() ==
               1);

        // Warm the peer's old node caches, then add data and replace all
        // three index files through another engine instance.
        assert(g_engine.insert(
                   database, "t",
                   {{"id", second.id},
                    {"tag", second.tag},
                    {"part", second.part}}) == DBStatus::OK);
        assert(g_engine.reindex(database, "t") == DBStatus::OK);

        peerPrimary = peer.getPKIndex(database, "t");
        peerSecondary = peer.getSecondaryIndex(database, "t", "tag");
        peerComposite =
            peer.getCompositeIndexTree(database, "t", "tag_part_idx");
        assert(peerPrimary && peerSecondary && peerComposite);
        int64_t secondRid = -1;
        assert(peerPrimary->search(second.id, secondRid));
        const auto secondaryRids = peerSecondary->searchMulti(second.tag);
        const auto compositeRids = peerComposite->searchMulti(
            second.tag + std::string(1, '\x01') + second.part);
        assert(secondaryRids.size() == 1 &&
               secondaryRids.front() == secondRid);
        assert(compositeRids.size() == 1 &&
               compositeRids.front() == secondRid);
    }

    cleanupTestDb(testName);
}

void test_reindex_preserves_inflight_transaction_state() {
    using dbms::DBStatus;
    using namespace std::chrono_literals;

    const std::string testName = "reindex_inflight_transaction";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    std::vector<TestRow> rows = {{"1", "alpha", "one"}};
    createIndexedTable(database, rows);

    std::promise<void> insertedPromise;
    std::future<void> inserted = insertedPromise.get_future();
    std::promise<void> commitPromise;
    std::shared_future<void> mayCommit = commitPromise.get_future().share();
    std::thread writer([&] {
        assert(g_engine.beginTransaction(database) == DBStatus::OK);
        assert(g_engine.insert(
                   database, "t",
                   {{"id", "2"}, {"tag", "bravo"}, {"part", "two"}}) ==
               DBStatus::OK);
        insertedPromise.set_value();
        mayCommit.wait();
        assert(g_engine.commitTransaction() == DBStatus::OK);
    });
    inserted.wait();

    std::promise<void> reindexStartedPromise;
    std::future<void> reindexStarted = reindexStartedPromise.get_future();
    auto reindexResult = std::async(std::launch::async, [&] {
        reindexStartedPromise.set_value();
        return g_engine.reindex(database, "t");
    });
    reindexStarted.wait();
    const auto reindexState = reindexResult.wait_for(5s);
    commitPromise.set_value();
    writer.join();
    assert(reindexState == std::future_status::ready);
    assert(reindexResult.get() == DBStatus::OK);

    rows.push_back({"2", "bravo", "two"});
    assertIndexed(database, rows);

    // Active DELETE has already removed its keys even though its old heap
    // version remains snapshot-visible. The maintenance view must exclude it
    // so a later COMMIT cannot leave stale entries behind.
    std::promise<void> deletedPromise;
    std::future<void> deleted = deletedPromise.get_future();
    std::promise<void> deleteCommitPromise;
    std::shared_future<void> mayCommitDelete =
        deleteCommitPromise.get_future().share();
    std::thread deleter([&] {
        assert(g_engine.beginTransaction(database) == DBStatus::OK);
        assert(g_engine.remove(database, "t", {"=id 2"}) == DBStatus::OK);
        deletedPromise.set_value();
        mayCommitDelete.wait();
        assert(g_engine.commitTransaction() == DBStatus::OK);
    });
    deleted.wait();

    auto deleteReindexResult = std::async(std::launch::async, [&] {
        return g_engine.reindex(database, "t");
    });
    const auto deleteReindexState = deleteReindexResult.wait_for(5s);
    deleteCommitPromise.set_value();
    deleter.join();
    assert(deleteReindexState == std::future_status::ready);
    assert(deleteReindexResult.get() == DBStatus::OK);

    assertIndexed(database, {rows.front()});
    assertNotIndexed(database, rows.back());
    cleanupTestDb(testName);
}

void test_tde_sidecars_follow_rebuilt_indexes() {
    using dbms::DBStatus;
    using dbms::PageCrypto;

    const std::string testName = "reindex_tde_pair";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    PageCrypto::disable();
    assert(PageCrypto::enable(std::string(64, 'a')));

    const std::vector<TestRow> rows = {
        {"1", "alpha", "one"},
        {"2", "bravo", "two"},
        {"3", "charlie", "three"}};
    createIndexedTable(database, rows);
    assert(g_engine.checkpoint(database));

    assert(g_engine.reindex(database, "t") == DBStatus::OK);
    // REINDEX evicts all three live cache entries before swapping the files,
    // so these probes necessarily reopen the published encrypted generations.
    assertIndexed(database, rows);
    assertNoReindexArtifacts(database);

    const std::vector<std::filesystem::path> sidecars = {
        std::filesystem::path(database) / "t.idx.tde",
        std::filesystem::path(database) / "t_tag.idx.tde",
        std::filesystem::path(database) / "t.idx_tag_part_idx.tde"};
    for (const auto& sidecar : sidecars) {
        assert(std::filesystem::is_regular_file(sidecar));
        assert(std::filesystem::file_size(sidecar) >
               PageCrypto::kRecordSize);
    }

    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    PageCrypto::disable();
    cleanupTestDb(testName);
}

void test_interrupted_tde_swap_recovers_on_startup() {
    using dbms::DBStatus;
    using dbms::PageCrypto;

    const std::string testName = "reindex_tde_recovery";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    PageCrypto::disable();
    assert(PageCrypto::enable(std::string(64, 'b')));
    const std::vector<TestRow> rows = {
        {"1", "alpha", "one"}, {"2", "bravo", "two"}};

    const auto target =
        std::filesystem::absolute(
            std::filesystem::path(database) / "t.idx").lexically_normal();
    const auto targetSidecar =
        std::filesystem::path(target.string() + ".tde");
    const auto temporary =
        std::filesystem::path(target.string() + ".reindex.tmp.999.1");
    const auto temporarySidecar =
        std::filesystem::path(temporary.string() + ".tde");
    const auto backup =
        std::filesystem::path(target.string() + ".reindex.old.999.1");
    const auto backupSidecar =
        std::filesystem::path(backup.string() + ".tde");

    {
        dbms::StorageEngine source;
        assert(source.createDatabase(database, "utf8") == DBStatus::OK);
        dbms::TableSchema table;
        table.tablename = "t";
        table.formatVersion = 2;
        table.append(dbms::makeIntColumn("id", false, 4, true));
        table.append(dbms::makeVarCharColumn("tag", false, 40));
        table.append(dbms::makeVarCharColumn("part", false, 40));
        assert(source.createTable(database, table) == DBStatus::OK);
        for (const auto& row : rows) {
            assert(source.insert(
                       database, "t",
                       {{"id", row.id},
                        {"tag", row.tag},
                        {"part", row.part}}) == DBStatus::OK);
        }
        assert(source.checkpoint(database));

        // Preserve generation A, let REINDEX create generation B, then model
        // a crash after B's main file was renamed but before its sidecar.
        assert(std::filesystem::copy_file(target, backup));
        assert(std::filesystem::copy_file(targetSidecar, backupSidecar));
        assert(source.reindex(database, "t") == DBStatus::OK);
        assert(std::filesystem::copy_file(target, temporary));
        assert(std::filesystem::copy_file(targetSidecar, temporarySidecar));
        assert(std::filesystem::copy_file(
            backup, target, std::filesystem::copy_options::overwrite_existing));
        assert(std::filesystem::copy_file(
            backupSidecar, targetSidecar,
            std::filesystem::copy_options::overwrite_existing));
        assert(std::filesystem::copy_file(
            temporary, target,
            std::filesystem::copy_options::overwrite_existing));
        assert(std::filesystem::remove(temporary));

        const auto marker =
            std::filesystem::path(database) / ".t.reindex_swap";
        std::ofstream output(marker);
        output << "DBMS_REINDEX_SWAP_V1\n1\n"
               << std::quoted(target.string()) << ' '
               << std::quoted(temporary.string()) << ' '
               << std::quoted(backup.string()) << '\n';
        output.close();
        assert(output);
    }

    {
        dbms::StorageEngine recovered;
        dbms::BPTree* primary = recovered.getPKIndex(database, "t");
        assert(primary);
        for (const auto& row : rows) {
            int64_t rid = -1;
            assert(primary->search(row.id, rid));
        }
        assertNoReindexArtifacts(database);
        assert(recovered.dropDatabase(database) == DBStatus::OK);
    }

    PageCrypto::disable();
    cleanupTestDb(testName);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_failed_build_preserves_live_indexes();
    test_missing_live_index_generation_is_rebuilt();
    test_peer_cache_reopens_replaced_generation();
    test_reindex_preserves_inflight_transaction_state();
    test_tde_sidecars_follow_rebuilt_indexes();
    test_interrupted_tde_swap_recovers_on_startup();
    finalCleanupTestData();
    std::cout << "[REINDEX ATOMICITY] replacement and snapshot safety OK"
              << std::endl;
    return 0;
}
