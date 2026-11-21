#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "access/IndexFileUtil.h"
#include "catalog/type_registry.h"
#include "storage/HeapWalIdentity.h"
#include "storage/WAL.h"
#include "transaction/TxnIdGenerator.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

dbms::Config g_config;

using namespace dbms;
namespace fs = std::filesystem;

namespace {

constexpr const char* kCompletedDatabase = "truncate_recovery_db";
constexpr const char* kPendingDatabase = "truncate_pending_db";
constexpr const char* kCorruptDatabase = "truncate_corrupt_state_db";
constexpr const char* kNameOnlyDatabase = "truncate_name_only_state_db";

std::string makePayload(size_t size) {
    static constexpr char alphabet[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string value(size, '\0');
    uint32_t state = 0x8badf00du;
    for (char& ch : value) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        ch = alphabet[state % (sizeof(alphabet) - 1)];
    }
    return value;
}

void createTable(StorageEngine& engine, const std::string& database) {
    assert(engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("payload", false, 12000, false));
    table.append(makeVarCharColumn("tag", false, 64, false));
    assert(engine.createTable(database, table) == DBStatus::OK);
    assert(engine.createIndex(database, "items", "tag") == DBStatus::OK);
    assert(engine.createHashIndex(database, "items", "tag") ==
           DBStatus::OK);
    assert(engine.createBloomIndex(database, "items", "tag") ==
           DBStatus::OK);
}

size_t rowCount(StorageEngine& engine, const std::string& database) {
    size_t count = 0;
    assert(engine.forEachRow(
        database, "items",
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
    return count;
}

void assertFreshState(StorageEngine& engine, const std::string& payload,
                      const std::string& database = kCompletedDatabase) {
    assert(rowCount(engine, database) == 1);
    const auto fresh = engine.query(
        database, "items", {"=id 999"}, {"payload", "tag"});
    assert(fresh.size() == 1);
    assert(fresh.front().find(payload) != std::string::npos);
    assert(fresh.front().find("fresh") != std::string::npos);
    assert(engine.query(
               database, "items", {"=id 1"}, {"id"})
               .empty());

    int64_t rid = -1;
    BPTree* primary = engine.getPKIndex(database, "items");
    BPTree* secondary =
        engine.getSecondaryIndex(database, "items", "tag");
    HashIndex* hash =
        engine.getHashIndex(database, "items", "tag");
    BloomIndex* bloom =
        engine.getBloomIndex(database, "items", "tag");
    assert(primary && secondary && hash && bloom);
    assert(primary->search("999", rid));
    assert(secondary->searchMulti("fresh") == std::vector<int64_t>{rid});
    assert(secondary->searchMulti("old").empty());
    assert(hash->search("fresh") == std::vector<int64_t>{rid});
    assert(hash->search("old").empty());
    assert(bloom->search("fresh") == std::vector<int64_t>{rid});
    assert(bloom->search("old").empty());
}

std::vector<char> truncatePayload(const std::string& tableName,
                                  uint64_t physicalRelationId) {
    const uint32_t length = static_cast<uint32_t>(tableName.size());
    std::vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<const char*>(&length),
                   reinterpret_cast<const char*>(&length) + sizeof(length));
    payload.insert(payload.end(), tableName.begin(), tableName.end());
    // Match walSmgrTruncate: a current schema requires its actual generation.
    heap_wal_identity::appendImage(payload, physicalRelationId);
    return payload;
}

void assertTruncateRecord(WALManager& wal, Lsn lsn,
                          uint64_t physicalRelationId, uint64_t xid = 0) {
    const auto record = wal.ReadRecord(lsn);
    assert(record && record->rmid() == RM_SMGR_ID);
    assert(record->info() == XLOG_SMGR_TRUNCATE);
    assert(record->header.xl_xid == xid);
    std::string name;
    std::optional<uint64_t> identity;
    assert(heap_wal_identity::nameIdentity(record->data, name, identity));
    assert(name == "items");
    if (physicalRelationId == 0) assert(!identity);
    else assert(identity && *identity == physicalRelationId);
}

enum class Decision { Pending, Abort, Commit };

Lsn appendTransactionalTruncate(WALManager& wal, uint64_t physicalRelationId,
                               Decision decision, uint64_t commitEpoch = 0) {
    const uint64_t xid = TxnIdGenerator::instance().nextTxId();
    assert(xid != 0);
    const Lsn truncateLsn = wal.XLogInsert(
        RM_SMGR_ID, XLOG_SMGR_TRUNCATE, xid,
        truncatePayload("items", physicalRelationId));
    assert(truncateLsn != INVALID_LSN);
    Lsn flushLsn = truncateLsn;
    const bool commit = decision == Decision::Commit;
    const uint8_t info = commit ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT;
    if (decision != Decision::Pending) {
        const uint64_t words[] = {xid, commitEpoch};
        const auto* first = reinterpret_cast<const char*>(words);
        const auto* last = first + (commit ? sizeof(words) : sizeof(xid));
        flushLsn = wal.XLogInsert(RM_XACT_ID, info, xid, std::vector<char>(first, last));
        assert(flushLsn != INVALID_LSN);
    }
    assert(wal.XLogFlush(flushLsn));
    if (decision != Decision::Pending) {
        const auto record = wal.ReadRecord(flushLsn);
        assert(record && record->rmid() == RM_XACT_ID &&
               record->info() == info && record->header.xl_xid == xid);
    }
    assertTruncateRecord(wal, truncateLsn, physicalRelationId, xid);
    return truncateLsn;
}

std::string readBytes(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input.good());
    return {std::istreambuf_iterator<char>(input), {}};
}

void assertEmptyState(StorageEngine& engine, const std::string& database) {
    assert(rowCount(engine, database) == 0);
    int64_t rid = -1;
    auto* primary = engine.getPKIndex(database, "items");
    auto* secondary = engine.getSecondaryIndex(database, "items", "tag");
    auto* hash = engine.getHashIndex(database, "items", "tag");
    auto* bloom = engine.getBloomIndex(database, "items", "tag");
    assert(primary && secondary && hash && bloom);
    assert(!primary->search("999", rid));
    assert(secondary->allValues().empty());
    assert(hash->search("fresh").empty());
    assert(bloom->search("fresh").empty());
}

void testTransactionalTruncate(const std::string& database, Decision decision,
                               bool excludedByTarget = false) {
    fs::remove_all(database);
    const auto state = fs::path(database) / "items.truncate_state";
    const auto target = fs::path(database) / "pg_wal" / "recovery_target";
    const std::string before = "committed before truncate " + makePayload(9000);
    const std::string after = "committed after truncate " + makePayload(9000);
    const bool accepted = decision == Decision::Commit && !excludedByTarget;
    assert(!excludedByTarget || decision == Decision::Commit);
    uint64_t relationId = 0;
    uint32_t originalTimeline = 0;
    Lsn truncateLsn = INVALID_LSN;
    {
        StorageEngine engine;
        createTable(engine, database);
        relationId = engine.getTableSchema(database, "items").physicalRelationId;
        assert(relationId != 0);
        assert(engine.insert(database, "items",
                    {{"id", "999"}, {"payload", before}, {"tag", "fresh"}}) == DBStatus::OK);
        assertFreshState(engine, before, database);
        WALManager* wal = engine.getWAL(database);
        assert(wal);
        originalTimeline = wal->timelineId();
        const auto now = std::time(nullptr);
        assert(now > 0);
        const auto epoch = static_cast<uint64_t>(now);
        // Persist only WAL. Matching old heap/index bytes deliberately remain
        // on disk, and no local completion marker is created by the fixture.
        truncateLsn = appendTransactionalTruncate(
            *wal, relationId, decision, epoch + static_cast<uint64_t>(excludedByTarget));
        assert(!fs::exists(state));
        if (excludedByTarget) {
            assert(index_file::writeAtomically(target, std::to_string(epoch) + "\n"));
        }
    }

    std::string completedState;
    {
        StorageEngine recovered;
        assert(recovered.getTableSchema(database, "items").physicalRelationId == relationId);
        if (accepted) {
            assertEmptyState(recovered, database);
            completedState = readBytes(state);
            assert(completedState.size() == 24);
            // Two 32-bit format words precede the applied LSN on disk.
            Lsn appliedLsn = INVALID_LSN;
            std::memcpy(&appliedLsn, completedState.data() + 2 * sizeof(uint32_t), sizeof(appliedLsn));
            assert(appliedLsn == truncateLsn);
            assert(recovered.insert(database, "items",
                    {{"id", "999"}, {"payload", after}, {"tag", "fresh"}}) == DBStatus::OK);
            assertFreshState(recovered, after, database);
            WALManager* wal = recovered.getWAL(database);
            assert(wal);
            // A more recent pending or aborted record for this identity must
            // leave the earlier accepted cutoff and the later tuple intact.
            appendTransactionalTruncate(*wal, relationId, Decision::Pending);
            appendTransactionalTruncate(*wal, relationId, Decision::Abort);
        } else {
            assertFreshState(recovered, before, database);
            assert(!fs::exists(state));
            if (excludedByTarget) {
                assert(!fs::exists(target));
                assert(recovered.getWAL(database)->timelineId() > originalTimeline);
            }
        }
    }
    {
        StorageEngine restarted;
        assertFreshState(restarted, accepted ? after : before, database);
        if (accepted) assert(readBytes(state) == completedState);
        else assert(!fs::exists(state));
    }
    fs::remove_all(database);
    std::cout << "[TRUNCATE RECOVERY] " << database << " OK\n";
}

void testExcludedTruncateCompletion() {
    const std::string database = "truncate_excluded_completion_db";
    const auto statePath = fs::path(database) / "items.truncate_state";
    fs::remove_all(database);
    {
        StorageEngine engine;
        createTable(engine, database);
        assert(engine.insert(database, "items",
                    {{"id", "999"}, {"payload", "committed"}, {"tag", "fresh"}}) == DBStatus::OK);
        auto* wal = engine.getWAL(database);
        assert(wal);
        const auto identity = engine.getTableSchema(database, "items").physicalRelationId;
        const Lsn rejected = appendTransactionalTruncate(*wal, identity, Decision::Pending);

        // A complete sidecar for an uncommitted reset cannot establish that
        // rollback restored the previous generation.
        const uint32_t header[] = {0x314e5254u, 1};
        std::string bytes(reinterpret_cast<const char*>(header), sizeof(header));
        bytes.append(reinterpret_cast<const char*>(&rejected), sizeof(rejected));
        uint64_t checksum = 1469598103934665603ULL;
        for (unsigned char byte : bytes) {
            checksum ^= byte;
            checksum *= 1099511628211ULL;
        }
        bytes.append(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
        assert(index_file::writeAtomically(statePath, bytes));
    }
    const auto heap = fs::path(database) / "items.dt";
    const auto previousHeap = readBytes(heap);
    bool rejected = false;
    try {
        StorageEngine restarted;
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    assert(rejected && readBytes(heap) == previousHeap);
    fs::remove_all(database);
}

void testCompletedTruncate() {
    fs::remove_all(kCompletedDatabase);
    const std::string freshPayload = "freshneedle " + makePayload(9000);
    {
        StorageEngine engine;
        createTable(engine, kCompletedDatabase);

        // Fill several old heap pages. A recovery implementation that merely
        // recreates page 1 can otherwise miss resurrection of removed pages.
        for (int id = 1; id <= 96; ++id) {
            assert(engine.insert(
                       kCompletedDatabase, "items",
                       {{"id", std::to_string(id)},
                        {"payload", "old-" + std::to_string(id) + "-" +
                                        makePayload(180)},
                        {"tag", "old"}}) == DBStatus::OK);
        }
        assert(rowCount(engine, kCompletedDatabase) == 96);
        assert(engine.truncateTable(kCompletedDatabase, "items") ==
               DBStatus::OK);
        const fs::path state =
            fs::path(kCompletedDatabase) / "items.truncate_state";
        assert(fs::is_regular_file(state));
        assert(fs::file_size(state) == 24);

        assert(engine.insert(
                   kCompletedDatabase, "items",
                   {{"id", "999"}, {"payload", freshPayload},
                    {"tag", "fresh"}}) == DBStatus::OK);
        assertFreshState(engine, freshPayload);
    }

    // The first restart must ignore every pre-TRUNCATE page image without
    // clearing physical post-TRUNCATE TOAST chunks. The second proves that
    // the durable completion marker makes recovery idempotent.
    {
        StorageEngine recovered;
        assertFreshState(recovered, freshPayload);
    }
    {
        StorageEngine restarted;
        assertFreshState(restarted, freshPayload);
    }
    fs::remove_all(kCompletedDatabase);
}

void testPendingTruncate() {
    fs::remove_all(kPendingDatabase);
    uint64_t physicalRelationId = 0;
    {
        StorageEngine engine;
        createTable(engine, kPendingDatabase);
        physicalRelationId =
            engine.getTableSchema(kPendingDatabase, "items").physicalRelationId;
        assert(physicalRelationId != 0);
        assert(engine.insert(
                   kPendingDatabase, "items",
                   {{"id", "1"}, {"payload", "stale"}, {"tag", "old"}}) ==
               DBStatus::OK);

        // Model a crash after the write-ahead marker became durable but
        // before any relation fork was reset or completion state published.
        WALManager* wal = engine.getWAL(kPendingDatabase);
        assert(wal);
        const Lsn truncateLsn = wal->XLogInsert(
            RM_SMGR_ID, XLOG_SMGR_TRUNCATE, 0,
            truncatePayload("items", physicalRelationId));
        assert(truncateLsn != INVALID_LSN);
        assert(wal->XLogFlush(truncateLsn));
        assertTruncateRecord(*wal, truncateLsn, physicalRelationId);
        assert(!fs::exists(
            fs::path(kPendingDatabase) / "items.truncate_state"));
    }

    {
        StorageEngine recovered;
        assert(recovered.getTableSchema(kPendingDatabase, "items")
                   .physicalRelationId == physicalRelationId);
        assert(rowCount(recovered, kPendingDatabase) == 0);
        int64_t rid = -1;
        assert(!recovered.getPKIndex(kPendingDatabase, "items")
                    ->search("1", rid));
        assert(recovered.getSecondaryIndex(
                           kPendingDatabase, "items", "tag")
                   ->allValues()
                   .empty());
        assert(recovered.getHashIndex(kPendingDatabase, "items", "tag")
                   ->search("old")
                   .empty());
        assert(fs::file_size(
                   fs::path(kPendingDatabase) / "items.truncate_state") ==
               24);
    }
    {
        StorageEngine restarted;
        assert(restarted.getTableSchema(kPendingDatabase, "items")
                   .physicalRelationId == physicalRelationId);
        assert(rowCount(restarted, kPendingDatabase) == 0);
    }
    fs::remove_all(kPendingDatabase);
}

void testNameOnlyMarkerCannotTargetCurrentGeneration(bool transactional = false) {
    fs::remove_all(kNameOnlyDatabase);
    {
        StorageEngine engine;
        createTable(engine, kNameOnlyDatabase);
        assert(engine.getTableSchema(kNameOnlyDatabase, "items")
                   .physicalRelationId != 0);
        assert(engine.insert(
                   kNameOnlyDatabase, "items",
                   {{"id", "1"}, {"payload", "retained"}, {"tag", "old"}}) ==
               DBStatus::OK);
        WALManager* wal = engine.getWAL(kNameOnlyDatabase);
        assert(wal);
        if (transactional) {
            appendTransactionalTruncate(*wal, 0, Decision::Commit);
        } else {
            const Lsn truncateLsn = wal->XLogInsert(
                RM_SMGR_ID, XLOG_SMGR_TRUNCATE, 0, truncatePayload("items", 0));
            assert(truncateLsn != INVALID_LSN);
            assert(wal->XLogFlush(truncateLsn));
            assertTruncateRecord(*wal, truncateLsn, 0);
        }
        assert(rowCount(engine, kNameOnlyDatabase) == 1);
        assert(!fs::exists(
            fs::path(kNameOnlyDatabase) / "items.truncate_state"));
    }
    const auto heap = fs::path(kNameOnlyDatabase) / "items.dt";
    const auto index = fs::path(kNameOnlyDatabase) / "items.idx";
    const auto oldHeap = readBytes(heap);
    const auto oldIndex = readBytes(index);
    bool rejected = false;
    try {
        StorageEngine recovered;
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    assert(rejected);
    assert(readBytes(heap) == oldHeap && readBytes(index) == oldIndex);
    assert(!fs::exists(fs::path(kNameOnlyDatabase) / "items.truncate_state"));
    fs::remove_all(kNameOnlyDatabase);
}

void testCorruptStateFailsClosed() {
    fs::remove_all(kCorruptDatabase);
    {
        StorageEngine engine;
        createTable(engine, kCorruptDatabase);
        assert(engine.insert(
                   kCorruptDatabase, "items",
                   {{"id", "1"}, {"payload", "value"}, {"tag", "old"}}) ==
               DBStatus::OK);
        assert(engine.truncateTable(kCorruptDatabase, "items") ==
               DBStatus::OK);
    }
    {
        std::ofstream corrupt(
            fs::path(kCorruptDatabase) / "items.truncate_state",
            std::ios::binary | std::ios::trunc);
        const std::string zeros(24, '\0');
        corrupt.write(zeros.data(), static_cast<std::streamsize>(zeros.size()));
        assert(corrupt.good());
    }

    bool failedClosed = false;
    try {
        StorageEngine recovered;
    } catch (const std::runtime_error&) {
        failedClosed = true;
    }
    assert(failedClosed);
    fs::remove_all(kCorruptDatabase);
}

}  // namespace

int main() {
    TypeRegistry::instance().bootstrap();
    fs::remove_all(kCompletedDatabase);
    fs::remove_all(kPendingDatabase);
    fs::remove_all(kCorruptDatabase);
    fs::remove_all(kNameOnlyDatabase);
    fs::remove_all(".txnid");

    testCompletedTruncate();
    std::cout << "[TRUNCATE RECOVERY] completed reset is durable OK\n";
    testPendingTruncate();
    std::cout << "[TRUNCATE RECOVERY] interrupted reset is completed OK\n";
    testCorruptStateFailsClosed();
    std::cout << "[TRUNCATE RECOVERY] corrupt state fails closed OK\n";
    testNameOnlyMarkerCannotTargetCurrentGeneration();
    std::cout << "[TRUNCATE RECOVERY] name-only marker rejects current generation OK\n";
    testTransactionalTruncate("truncate_transaction_pending_db", Decision::Pending);
    testTransactionalTruncate("truncate_transaction_aborted_db", Decision::Abort);
    testTransactionalTruncate("truncate_transaction_committed_db", Decision::Commit);
    testTransactionalTruncate("truncate_transaction_post_target_db", Decision::Commit, true);
    testExcludedTruncateCompletion();
    std::cout << "[TRUNCATE RECOVERY] unresolved completion fails closed OK\n";
    testNameOnlyMarkerCannotTargetCurrentGeneration(true);
    std::cout << "[TRUNCATE RECOVERY] transactional name-only marker rejected OK\n";

    fs::remove_all(kCompletedDatabase);
    fs::remove_all(kPendingDatabase);
    fs::remove_all(kCorruptDatabase);
    fs::remove_all(kNameOnlyDatabase);
    fs::remove_all(".txnid");
    std::cout << "[TRUNCATE RECOVERY] all passed\n";
    return 0;
}
