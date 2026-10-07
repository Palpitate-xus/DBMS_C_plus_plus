#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "storage/HeapWalIdentity.h"
#include "storage/WAL.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
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

void assertFreshState(StorageEngine& engine, const std::string& payload) {
    assert(rowCount(engine, kCompletedDatabase) == 1);
    const auto fresh = engine.query(
        kCompletedDatabase, "items", {"=id 999"}, {"payload", "tag"});
    assert(fresh.size() == 1);
    assert(fresh.front().find(payload) != std::string::npos);
    assert(fresh.front().find("fresh") != std::string::npos);
    assert(engine.query(
               kCompletedDatabase, "items", {"=id 1"}, {"id"})
               .empty());

    int64_t rid = -1;
    BPTree* primary = engine.getPKIndex(kCompletedDatabase, "items");
    BPTree* secondary =
        engine.getSecondaryIndex(kCompletedDatabase, "items", "tag");
    HashIndex* hash =
        engine.getHashIndex(kCompletedDatabase, "items", "tag");
    BloomIndex* bloom =
        engine.getBloomIndex(kCompletedDatabase, "items", "tag");
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
                          uint64_t physicalRelationId) {
    const auto record = wal.ReadRecord(lsn);
    assert(record && record->rmid() == RM_SMGR_ID);
    assert(record->info() == XLOG_SMGR_TRUNCATE);
    assert(record->header.xl_xid == 0);
    std::string name;
    std::optional<uint64_t> identity;
    assert(heap_wal_identity::nameIdentity(record->data, name, identity));
    assert(name == "items");
    if (physicalRelationId == 0) assert(!identity);
    else assert(identity && *identity == physicalRelationId);
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

void testNameOnlyMarkerCannotTargetCurrentGeneration() {
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
        const Lsn truncateLsn = wal->XLogInsert(
            RM_SMGR_ID, XLOG_SMGR_TRUNCATE, 0, truncatePayload("items", 0));
        assert(truncateLsn != INVALID_LSN);
        assert(wal->XLogFlush(truncateLsn));
        assertTruncateRecord(*wal, truncateLsn, 0);
        assert(rowCount(engine, kNameOnlyDatabase) == 1);
        assert(!fs::exists(
            fs::path(kNameOnlyDatabase) / "items.truncate_state"));
    }
    bool rejected = false;
    try {
        StorageEngine recovered;
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    assert(rejected);
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

    fs::remove_all(kCompletedDatabase);
    fs::remove_all(kPendingDatabase);
    fs::remove_all(kCorruptDatabase);
    fs::remove_all(kNameOnlyDatabase);
    fs::remove_all(".txnid");
    std::cout << "[TRUNCATE RECOVERY] all passed\n";
    return 0;
}
