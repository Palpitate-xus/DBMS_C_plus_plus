#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "storage/PageCrypto.h"
#include "storage/WAL.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

dbms::Config g_config;

using namespace dbms;
namespace fs = std::filesystem;

namespace {

constexpr const char* kDatabase = "unlogged_recovery_db";
constexpr const char* kNoWalDatabase = "unlogged_no_wal_db";
constexpr const char* kTablespace = "unlogged_recovery_tablespace";

std::string makePayload(size_t size) {
    static constexpr char alphabet[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string value(size, '\0');
    uint32_t state = 0x2468ace1u;
    for (char& ch : value) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        ch = alphabet[state % (sizeof(alphabet) - 1)];
    }
    return value;
}

size_t rowCount(StorageEngine& engine, const std::string& database,
                const std::string& table) {
    size_t count = 0;
    assert(engine.forEachRow(
        database, table,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
    return count;
}

bool contains(const std::vector<std::string>& values,
              const std::string& value) {
    return std::find(values.begin(), values.end(), value) != values.end();
}

uint64_t readToastNextId(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input.good());
    uint32_t magic = 0;
    uint32_t version = 0;
    uint64_t nextId = 0;
    input.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    input.read(reinterpret_cast<char*>(&version), sizeof(version));
    input.read(reinterpret_cast<char*>(&nextId), sizeof(nextId));
    assert(input.good());
    assert(magic == 0x31444954u);
    assert(version == 1);
    return nextId;
}

void assertPopulatedIndexes(StorageEngine& engine,
                            const std::string& payload) {
    assert(rowCount(engine, kDatabase, "cache") == 1);
    const auto rows = engine.query(
        kDatabase, "cache", {"=id 1"}, {"payload"});
    assert(rows.size() == 1);
    assert(rows.front().find(payload) != std::string::npos);

    int64_t rid = -1;
    BPTree* primary = engine.getPKIndex(kDatabase, "cache");
    assert(primary && primary->search("1", rid));

    BPTree* secondary =
        engine.getSecondaryIndex(kDatabase, "cache", "tag");
    BPTree* expression =
        engine.getSecondaryIndex(kDatabase, "cache", "UPPER(tag)");
    BPTree* composite =
        engine.getCompositeIndexTree(kDatabase, "cache", "tag_score");
    HashIndex* hash = engine.getHashIndex(kDatabase, "cache", "tag");
    BloomIndex* bloom = engine.getBloomIndex(kDatabase, "cache", "tag");
    assert(secondary && expression && composite && hash && bloom);
    assert(secondary->searchMulti("stale") == std::vector<int64_t>{rid});
    assert(expression->searchMulti("STALE") == std::vector<int64_t>{rid});
    assert(composite->searchMulti(
               std::string("stale") + '\x01' + "42") ==
           std::vector<int64_t>{rid});
    assert(hash->search("stale") == std::vector<int64_t>{rid});
    assert(bloom->search("stale") == std::vector<int64_t>{rid});
    assert(engine.fullTextSearch(
               kDatabase, "cache", "payload", "needle") ==
           std::vector<int64_t>{rid});
    assert(engine.ginSearch(kDatabase, "cache", "payload", "needle") ==
           std::vector<int64_t>{rid});
    assert(engine.giSTSearchOverlap(
               kDatabase, "cache", "score", "42", "42") ==
           std::vector<int64_t>{rid});
    assert(engine.spGiSTSearch(
               kDatabase, "cache", "location", "=", "1.5,2.5") ==
           std::vector<int64_t>{rid});
    assert(!engine.brinSearchRange(
                kDatabase, "cache", "score", "=", "42").empty());
}

void assertEmptyIndexes(StorageEngine& engine) {
    assert(engine.query(kDatabase, "cache", {}, {"id"}).empty());
    assert(rowCount(engine, kDatabase, "cache") == 0);

    int64_t rid = -1;
    BPTree* primary = engine.getPKIndex(kDatabase, "cache");
    BPTree* secondary =
        engine.getSecondaryIndex(kDatabase, "cache", "tag");
    BPTree* expression =
        engine.getSecondaryIndex(kDatabase, "cache", "UPPER(tag)");
    BPTree* composite =
        engine.getCompositeIndexTree(kDatabase, "cache", "tag_score");
    HashIndex* hash = engine.getHashIndex(kDatabase, "cache", "tag");
    BloomIndex* bloom = engine.getBloomIndex(kDatabase, "cache", "tag");
    assert(primary && secondary && expression && composite && hash && bloom);
    assert(!primary->search("1", rid));
    assert(primary->allValues().empty());
    assert(secondary->allValues().empty());
    assert(expression->allValues().empty());
    assert(composite->allValues().empty());
    assert(hash->search("stale").empty());
    assert(hash->size() == 0);
    assert(bloom->search("stale").empty());
    assert(bloom->size() == 0);

    assert(contains(engine.getIndexedColumns(kDatabase, "cache"), "tag"));
    const auto metadata = engine.getIndexMetadata(kDatabase, "cache");
    assert(std::any_of(metadata.begin(), metadata.end(), [](const auto& item) {
        return item.isExpression && item.name == "UPPER(tag)";
    }));
    const auto compositeMetadata =
        engine.getCompositeIndexes(kDatabase, "cache");
    assert(std::any_of(
        compositeMetadata.begin(), compositeMetadata.end(),
        [](const auto& item) { return item.name == "tag_score"; }));
    assert(contains(
        engine.getHashIndexedColumns(kDatabase, "cache"), "tag"));
    assert(contains(
        engine.getBloomIndexedColumns(kDatabase, "cache"), "tag"));

    assert(engine.hasFullTextIndex(kDatabase, "cache", "payload"));
    assert(engine.hasGinIndex(kDatabase, "cache", "payload"));
    assert(engine.hasGiSTIndex(kDatabase, "cache", "score"));
    assert(engine.hasSPGiSTIndex(kDatabase, "cache", "location"));
    assert(engine.hasBrinIndex(kDatabase, "cache", "score"));
    assert(engine.fullTextSearch(
               kDatabase, "cache", "payload", "needle").empty());
    assert(engine.ginSearch(
               kDatabase, "cache", "payload", "needle").empty());
    assert(engine.giSTSearchOverlap(
               kDatabase, "cache", "score", "42", "42").empty());
    assert(engine.spGiSTSearch(
               kDatabase, "cache", "location", "=", "1.5,2.5").empty());
    assert(engine.brinSearchRange(
               kDatabase, "cache", "score", "=", "42").empty());
}

void testNoWalStartupStillResetsDefinitions() {
    fs::remove_all(kNoWalDatabase);
    {
        StorageEngine engine;
        assert(engine.createDatabase(kNoWalDatabase) == DBStatus::OK);
        TableSchema table;
        table.tablename = "empty_cache";
        table.formatVersion = 2;
        table.isUnlogged = true;
        table.append(makeIntColumn("id", false, 4, false));
        table.append(makeVarCharColumn("tag", false, 64, false));
        assert(engine.createTable(kNoWalDatabase, table) == DBStatus::OK);
        assert(engine.createFullTextIndex(
                   kNoWalDatabase, "empty_cache", "tag") == DBStatus::OK);
        WALManager* wal = engine.getWAL(kNoWalDatabase);
        assert(wal && wal->currentWriteLsn() == 0);
    }

    // Model an old unlogged sidecar left by a build that did not WAL-log
    // relation writes.  Startup must reset it even though there is no WAL to
    // replay, while retaining the file as the index definition.
    const fs::path fullText = fs::path(kNoWalDatabase) /
                              "empty_cache_tag.fti";
    {
        std::ofstream stale(fullText, std::ios::trunc);
        stale << "stale 4294967297\n";
        assert(stale.good());
    }
    {
        StorageEngine recovered;
        assert(recovered.hasFullTextIndex(
            kNoWalDatabase, "empty_cache", "tag"));
        assert(recovered.fullTextSearch(
                   kNoWalDatabase, "empty_cache", "tag", "stale").empty());
    }
    fs::remove_all(kNoWalDatabase);
    std::cout << "[UNLOGGED] zero-WAL startup reset OK\n";
}

void testAllForksAndIndexes() {
    fs::remove_all(kDatabase);
    fs::remove_all(kTablespace);
    fs::remove_all(".txnid");
    assert(PageCrypto::enable(std::string(64, '8')));

    const std::string payload = "needle " + makePayload(9000);
    {
        StorageEngine engine;
        assert(engine.createDatabase(kDatabase) == DBStatus::OK);
        assert(engine.createTablespace(
                   kDatabase, "fast_space", kTablespace) == DBStatus::OK);

        TableSchema table;
        table.tablename = "cache";
        table.formatVersion = 2;
        table.isUnlogged = true;
        table.tablespace = "fast_space";
        table.append(makeIntColumn("id", false, 4, true));
        table.append(makeVarCharColumn("payload", false, 12000, false));
        table.append(makeVarCharColumn("tag", false, 64, false));
        table.append(makePointColumn("location", false, false));
        table.append(makeIntColumn("score", false, 4, false));
        assert(engine.createTable(kDatabase, table) == DBStatus::OK);
        assert(engine.insert(
                   kDatabase, "cache",
                   {{"id", "1"}, {"payload", payload}, {"tag", "stale"},
                    {"location", "1.5,2.5"}, {"score", "42"}}) ==
               DBStatus::OK);

        assert(engine.createIndex(kDatabase, "cache", "tag") ==
               DBStatus::OK);
        assert(engine.createIndex(kDatabase, "cache", "tag", true, {}, "",
                                  "UPPER(tag)") == DBStatus::OK);
        assert(engine.createCompositeIndex(
                   kDatabase, "cache", {"tag", "score"}, "tag_score") ==
               DBStatus::OK);
        assert(engine.createHashIndex(kDatabase, "cache", "tag") ==
               DBStatus::OK);
        assert(engine.createBloomIndex(kDatabase, "cache", "tag") ==
               DBStatus::OK);
        assert(engine.createFullTextIndex(
                   kDatabase, "cache", "payload") == DBStatus::OK);
        assert(engine.createGinIndex(kDatabase, "cache", "payload") ==
               DBStatus::OK);
        assert(engine.createGiSTIndex(kDatabase, "cache", "score") ==
               DBStatus::OK);
        assert(engine.createSPGiSTIndex(
                   kDatabase, "cache", "location") == DBStatus::OK);
        assert(engine.createBrinIndex(kDatabase, "cache", "score", 1) ==
               DBStatus::OK);

        TableSchema partitioned;
        partitioned.tablename = "partition_cache";
        partitioned.formatVersion = 2;
        partitioned.isUnlogged = true;
        partitioned.tablespace = "fast_space";
        partitioned.partitionType = TableSchema::PartitionType::Hash;
        partitioned.partitionKey = "id";
        partitioned.hashPartitions = 2;
        partitioned.subPartitionType = TableSchema::PartitionType::Hash;
        partitioned.subPartitionKey = "tag";
        partitioned.subHashPartitions = 2;
        partitioned.append(makeIntColumn("id", false, 4, false));
        partitioned.append(makeVarCharColumn("tag", false, 64, false));
        assert(engine.createTable(kDatabase, partitioned) == DBStatus::OK);
        assert(engine.insert(kDatabase, "partition_cache",
                             {{"id", "10"}, {"tag", "left"}}) ==
               DBStatus::OK);
        assert(engine.insert(kDatabase, "partition_cache",
                             {{"id", "11"}, {"tag", "right"}}) ==
               DBStatus::OK);
        assert(rowCount(engine, kDatabase, "partition_cache") == 2);

        assertPopulatedIndexes(engine, payload);
        assert(readToastNextId(fs::path(kTablespace) / kDatabase /
                               "cache.toastmeta") > 1);
        WALManager* wal = engine.getWAL(kDatabase);
        assert(wal && wal->currentWriteLsn() > 0);
    }

    const fs::path relationRoot = fs::path(kTablespace) / kDatabase;
    const fs::path heap = relationRoot / "cache.dt";
    const fs::path heapTde = fs::path(heap.string() + ".tde");
    const fs::path toastHeap = relationRoot / "cache.toast.dt";
    const fs::path toastTde = fs::path(toastHeap.string() + ".tde");
    assert(fs::file_size(heap) > 8192);
    assert(fs::file_size(heapTde) > PageCrypto::kRecordSize);
    assert(fs::file_size(toastHeap) > 8192);
    assert(fs::file_size(toastTde) > PageCrypto::kRecordSize);

    {
        StorageEngine recovered;
        assertEmptyIndexes(recovered);
        assert(rowCount(recovered, kDatabase, "partition_cache") == 0);

        assert(fs::file_size(heap) == 8192);
        // Page 0 is deliberately plaintext and has no TDE envelope.  An
        // empty sidecar therefore proves that every old data-page record was
        // removed rather than merely ignored.
        assert(fs::file_size(heapTde) == 0);
        assert(fs::file_size(toastHeap) == 8192);
        assert(fs::file_size(toastTde) == 0);
        assert(!fs::exists(relationRoot / "cache.fsm"));
        assert(!fs::exists(relationRoot / "cache.vm"));
        assert(readToastNextId(relationRoot / "cache.toastmeta") == 1);

        assert(!fs::exists(relationRoot / "partition_cache.dt"));
        for (size_t partition = 0; partition < 2; ++partition) {
            const std::string prefix =
                "partition_cache#p" + std::to_string(partition);
            assert(fs::file_size(relationRoot / (prefix + ".dt")) == 8192);
            for (size_t subpartition = 0; subpartition < 2; ++subpartition) {
                assert(fs::file_size(
                    relationRoot /
                    (prefix + "#sp" + std::to_string(subpartition) +
                     ".dt")) == 8192);
            }
        }

        const std::string freshPayload = "fresh " + makePayload(8500);
        assert(recovered.insert(
                   kDatabase, "cache",
                   {{"id", "2"}, {"payload", freshPayload}, {"tag", "fresh"},
                    {"location", "3.5,4.5"}, {"score", "7"}}) ==
               DBStatus::OK);
        assert(recovered.query(
                   kDatabase, "cache", {"=id 2"}, {"payload"}).size() == 1);
        int64_t freshRid = -1;
        assert(recovered.getPKIndex(kDatabase, "cache")
                   ->search("2", freshRid));
        assert(recovered.getSecondaryIndex(kDatabase, "cache", "tag")
                   ->searchMulti("fresh") == std::vector<int64_t>{freshRid});
        assert(recovered.getHashIndex(kDatabase, "cache", "tag")
                   ->search("fresh") == std::vector<int64_t>{freshRid});
        assert(recovered.getBloomIndex(kDatabase, "cache", "tag")
                   ->search("fresh") == std::vector<int64_t>{freshRid});
    }

    // Every startup clears the unlogged relation again, including data
    // written after a previous successful recovery.
    {
        StorageEngine restarted;
        assertEmptyIndexes(restarted);
        assert(restarted.query(
                   kDatabase, "cache", {"=id 2"}, {"id"}).empty());
    }

    fs::remove_all(kDatabase);
    fs::remove_all(kTablespace);
    fs::remove_all(".txnid");
    PageCrypto::disable();
    std::cout << "[UNLOGGED] heap, partitions, TOAST, indexes and TDE reset OK\n";
}

}  // namespace

int main() {
    TypeRegistry::instance().bootstrap();
    fs::remove_all(kDatabase);
    fs::remove_all(kNoWalDatabase);
    fs::remove_all(kTablespace);
    fs::remove_all(".txnid");

    testNoWalStartupStillResetsDefinitions();
    testAllForksAndIndexes();

    std::cout << "[UNLOGGED] all passed\n";
    return 0;
}
