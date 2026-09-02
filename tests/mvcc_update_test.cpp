#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"

#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

dbms::Config g_config;

using namespace dbms;

namespace {

std::string visibleValue(StorageEngine& engine, const std::string& database,
                         const TableSchema& table) {
    std::string value;
    size_t count = 0;
    assert(engine.forEachRow(
        database, table.tablename,
        [&](uint32_t, uint16_t, const char* data, size_t length) {
            ++count;
            const std::string row(data, length);
            assert(engine.extractColumnValue(row, table, 0, database, true) ==
                   "1");
            value = engine.extractColumnValue(
                row, table, 2, database, true);
        }));
    assert(count == 1);
    return value;
}

bool queryContains(StorageEngine& engine, const std::string& database,
                   const std::vector<std::string>& conditions,
                   const std::string& expected) {
    const auto rows = engine.query(
        database, "accounts", conditions, {"id", "tag", "value"});
    return rows.size() == 1 && rows.front().find(expected) != std::string::npos;
}

int64_t assertCurrentState(StorageEngine& engine,
                           const std::string& database,
                           const TableSchema& table,
                           const std::string& tag,
                           const std::string& value) {
    const std::string observedValue = visibleValue(engine, database, table);
    if (observedValue != value) {
        std::cerr << "state mismatch: expected " << value
                  << ", observed " << observedValue << '\n';
    }
    assert(observedValue == value);
    assert(queryContains(engine, database, {"=id 1"}, value));
    assert(queryContains(engine, database, {"=tag " + tag}, value));
    assert(queryContains(engine, database, {"=value " + value}, value));

    int64_t primaryRid = -1;
    BPTree* primary = engine.getPKIndex(database, "accounts");
    assert(primary && primary->search("1", primaryRid));

    BPTree* secondary =
        engine.getSecondaryIndex(database, "accounts", "tag");
    assert(secondary);
    const auto secondaryRids = secondary->searchMulti(tag);
    assert(secondaryRids.size() == 1 && secondaryRids.front() == primaryRid);

    HashIndex* hash = engine.getHashIndex(database, "accounts", "value");
    assert(hash);
    const auto hashRids = hash->search(value);
    assert(hashRids.size() == 1 && hashRids.front() == primaryRid);

    BloomIndex* bloom =
        engine.getBloomIndex(database, "accounts", "value");
    assert(bloom);
    const auto bloomRids = bloom->search(value);
    assert(bloomRids.size() == 1 && bloomRids.front() == primaryRid);

    BPTree* composite =
        engine.getCompositeIndexTree(database, "accounts", "tag_value_idx");
    assert(composite);
    const auto compositeRids = composite->allValues();
    assert(compositeRids.size() == 1 &&
           compositeRids.front() == primaryRid);
    return primaryRid;
}

} // namespace

int main() {
    const std::string database = "mvcc_update_db";
    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        assert(engine.createDatabase(database) == DBStatus::OK);
        TableSchema table;
        table.tablename = "accounts";
        table.formatVersion = 2;
        table.append(makeIntColumn("id", false, 4, true));
        table.append(makeVarCharColumn("tag", false, 64, false));
        table.append(makeVarCharColumn("value", false, 64, false));
        assert(engine.createTable(database, table) == DBStatus::OK);
        assert(engine.createIndex(database, "accounts", "tag") ==
               DBStatus::OK);
        assert(engine.createHashIndex(database, "accounts", "value") ==
               DBStatus::OK);
        assert(engine.createBloomIndex(database, "accounts", "value") ==
               DBStatus::OK);
        assert(engine.createCompositeIndex(
                   database, "accounts", {"tag", "value"},
                   "tag_value_idx") == DBStatus::OK);
        assert(engine.insert(database, "accounts",
                             {{"id", "1"}, {"tag", "old-tag"},
                              {"value", "old-value"}}) == DBStatus::OK);

        std::mutex mutex;
        std::condition_variable condition;
        int phase = 0;
        std::thread reader([&] {
            engine.setIsolationLevel(IsolationLevel::REPEATABLE_READ);
            assert(engine.beginTransaction(database) == DBStatus::OK);
            {
                std::lock_guard<std::mutex> lock(mutex);
                phase = 1;
            }
            condition.notify_all();

            {
                std::unique_lock<std::mutex> lock(mutex);
                condition.wait(lock, [&] { return phase >= 2; });
            }
            // While the writer is in progress, the old physical version must
            // remain visible rather than disappearing from the snapshot.
            assert(visibleValue(engine, database, table) == "old-value");
            {
                std::lock_guard<std::mutex> lock(mutex);
                phase = 3;
            }
            condition.notify_all();

            {
                std::unique_lock<std::mutex> lock(mutex);
                condition.wait(lock, [&] { return phase >= 4; });
            }
            // The same snapshot keeps the old version after COMMIT, including
            // predicates that would normally use the PK or secondary index.
            assert(visibleValue(engine, database, table) == "old-value");
            assert(queryContains(engine, database, {"=id 1"}, "old-value"));
            assert(queryContains(engine, database, {"=tag old-tag"},
                                 "old-value"));
            assert(queryContains(engine, database, {"=value old-value"},
                                 "old-value"));
            assert(engine.query(database, "accounts", {"=tag new-tag"},
                                {"value"}).empty());
            assert(engine.commitTransaction() == DBStatus::OK);
        });

        {
            std::unique_lock<std::mutex> lock(mutex);
            condition.wait(lock, [&] { return phase >= 1; });
        }
        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.update(database, "accounts",
                             {{"tag", "new-tag"},
                              {"value", "new-value"}},
                             {"=id 1"}) == DBStatus::OK);
        assert(visibleValue(engine, database, table) == "new-value");
        {
            std::lock_guard<std::mutex> lock(mutex);
            phase = 2;
        }
        condition.notify_all();
        {
            std::unique_lock<std::mutex> lock(mutex);
            condition.wait(lock, [&] { return phase >= 3; });
        }
        assert(engine.commitTransaction() == DBStatus::OK);
        {
            std::lock_guard<std::mutex> lock(mutex);
            phase = 4;
        }
        condition.notify_all();
        reader.join();

        const int64_t committedRid = assertCurrentState(
            engine, database, table, "new-tag", "new-value");

        // A full rollback must remove NEW, reactivate OLD at its original
        // RID, and reverse every index family.
        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.update(database, "accounts",
                             {{"tag", "rollback-tag"},
                              {"value", "rollback-value"}},
                             {"=id 1"}) == DBStatus::OK);
        const int64_t rollbackRid = assertCurrentState(
            engine, database, table, "rollback-tag", "rollback-value");
        assert(rollbackRid != committedRid);
        assert(engine.rollbackTransaction() == DBStatus::OK);
        assert(assertCurrentState(
                   engine, database, table, "new-tag", "new-value") ==
               committedRid);
        assert(engine.query(database, "accounts", {"=tag rollback-tag"},
                            {"value"}).empty());

        // Rolling back only the second update keeps the first NEW version
        // (whose xmin belongs to this transaction) and lets it commit.
        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.update(database, "accounts",
                             {{"tag", "stage-one-tag"},
                              {"value", "stage-one-value"}},
                             {"=id 1"}) == DBStatus::OK);
        const int64_t stageOneRid = assertCurrentState(
            engine, database, table, "stage-one-tag", "stage-one-value");
        assert(engine.savepoint("after_stage_one") == DBStatus::OK);
        assert(engine.update(database, "accounts",
                             {{"tag", "stage-two-tag"},
                              {"value", "stage-two-value"}},
                             {"=id 1"}) == DBStatus::OK);
        const int64_t stageTwoRid = assertCurrentState(
            engine, database, table, "stage-two-tag", "stage-two-value");
        assert(stageTwoRid != stageOneRid);
        assert(engine.rollbackToSavepoint("after_stage_one") ==
               DBStatus::OK);
        assert(assertCurrentState(
                   engine, database, table,
                   "stage-one-tag", "stage-one-value") == stageOneRid);
        assert(engine.commitTransaction() == DBStatus::OK);
        assert(assertCurrentState(
                   engine, database, table,
                   "stage-one-tag", "stage-one-value") == stageOneRid);

        // PREPARE detaches the backend but must leave its xid in-progress.
        // Autocommit readers continue to see OLD until explicit completion.
        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.update(database, "accounts",
                             {{"tag", "prepared-rollback-tag"},
                              {"value", "prepared-rollback-value"}},
                             {"=id 1"}) == DBStatus::OK);
        assert(engine.prepareTransaction("mvcc_update_rollback") ==
               DBStatus::OK);
        assert(!engine.inTransaction());
        assert(visibleValue(engine, database, table) == "stage-one-value");
        assert(queryContains(engine, database, {"=tag stage-one-tag"},
                             "stage-one-value"));
        assert(engine.rollbackPrepared("mvcc_update_rollback") ==
               DBStatus::OK);
        assert(assertCurrentState(
                   engine, database, table,
                   "stage-one-tag", "stage-one-value") == stageOneRid);

        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.update(database, "accounts",
                             {{"tag", "prepared-commit-tag"},
                              {"value", "prepared-commit-value"}},
                             {"=id 1"}) == DBStatus::OK);
        assert(engine.prepareTransaction("mvcc_update_commit") ==
               DBStatus::OK);
        assert(visibleValue(engine, database, table) == "stage-one-value");
        assert(engine.commitPrepared("mvcc_update_commit") == DBStatus::OK);
        assertCurrentState(engine, database, table,
                           "prepared-commit-tag", "prepared-commit-value");
        std::cout << "[MVCC UPDATE] concurrent snapshots preserve row versions OK\n";
    }

    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");
    std::filesystem::remove_all(".txnid");
    std::cout << "[MVCC UPDATE] all passed\n";
    return 0;
}
