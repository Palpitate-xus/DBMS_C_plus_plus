#include "TableManage.h"
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "storage/PageCrypto.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

using namespace dbms;
namespace fs = std::filesystem;

namespace {

constexpr const char* kDatabase = "truncate_storage_db";
constexpr const char* kTablespace = "truncate_storage_tablespace";

std::string makePayload(size_t size) {
    static constexpr char alphabet[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string value(size, '\0');
    uint32_t state = 0x13572468u;
    for (char& ch : value) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        ch = alphabet[state % (sizeof(alphabet) - 1)];
    }
    return value;
}

size_t rowCount(const std::string& table) {
    size_t count = 0;
    assert(g_engine.forEachRow(
        kDatabase, table,
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
    assert(magic == 0x31444954u && version == 1);
    return nextId;
}

void assertOldIndexEntries(int64_t expectedRid) {
    BPTree* primary = g_engine.getPKIndex(kDatabase, "items");
    BPTree* secondary =
        g_engine.getSecondaryIndex(kDatabase, "items", "tag");
    BPTree* expression =
        g_engine.getSecondaryIndex(kDatabase, "items", "UPPER(tag)");
    BPTree* composite =
        g_engine.getCompositeIndexTree(kDatabase, "items", "tag_score");
    HashIndex* hash = g_engine.getHashIndex(kDatabase, "items", "tag");
    BloomIndex* bloom = g_engine.getBloomIndex(kDatabase, "items", "tag");
    assert(primary && secondary && expression && composite && hash && bloom);

    int64_t primaryRid = -1;
    assert(primary->search("1", primaryRid));
    assert(primaryRid == expectedRid);
    assert(secondary->searchMulti("stale") ==
           std::vector<int64_t>{expectedRid});
    assert(expression->searchMulti("STALE") ==
           std::vector<int64_t>{expectedRid});
    assert(composite->searchMulti(
               std::string("stale") + '\x01' + "42") ==
           std::vector<int64_t>{expectedRid});
    assert(hash->search("stale") == std::vector<int64_t>{expectedRid});
    assert(bloom->search("stale") == std::vector<int64_t>{expectedRid});
    assert(g_engine.fullTextSearch(
               kDatabase, "items", "payload", "needle") ==
           std::vector<int64_t>{expectedRid});
    assert(g_engine.ginSearch(kDatabase, "items", "payload", "needle") ==
           std::vector<int64_t>{expectedRid});
    assert(g_engine.giSTSearchOverlap(
               kDatabase, "items", "score", "42", "42") ==
           std::vector<int64_t>{expectedRid});
    assert(g_engine.spGiSTSearch(
               kDatabase, "items", "location", "=", "1.5,2.5") ==
           std::vector<int64_t>{expectedRid});
    assert(!g_engine.brinSearchRange(
                kDatabase, "items", "score", "=", "42").empty());
}

void assertTruncatedIndexes() {
    int64_t rid = -1;
    BPTree* primary = g_engine.getPKIndex(kDatabase, "items");
    BPTree* secondary =
        g_engine.getSecondaryIndex(kDatabase, "items", "tag");
    BPTree* expression =
        g_engine.getSecondaryIndex(kDatabase, "items", "UPPER(tag)");
    BPTree* composite =
        g_engine.getCompositeIndexTree(kDatabase, "items", "tag_score");
    HashIndex* hash = g_engine.getHashIndex(kDatabase, "items", "tag");
    BloomIndex* bloom = g_engine.getBloomIndex(kDatabase, "items", "tag");
    assert(primary && secondary && expression && composite && hash && bloom);
    assert(!primary->search("1", rid));
    assert(primary->allValues().empty());
    assert(secondary->allValues().empty());
    assert(expression->allValues().empty());
    assert(composite->allValues().empty());
    assert(hash->size() == 0 && hash->search("stale").empty());
    assert(bloom->size() == 0 && bloom->search("stale").empty());

    assert(contains(g_engine.getIndexedColumns(kDatabase, "items"), "tag"));
    const auto indexMetadata =
        g_engine.getIndexMetadata(kDatabase, "items");
    assert(std::any_of(
        indexMetadata.begin(), indexMetadata.end(), [](const auto& metadata) {
            return metadata.isExpression && metadata.name == "UPPER(tag)";
        }));
    const auto compositeMetadata =
        g_engine.getCompositeIndexes(kDatabase, "items");
    assert(std::any_of(
        compositeMetadata.begin(), compositeMetadata.end(),
        [](const auto& metadata) { return metadata.name == "tag_score"; }));
    assert(contains(
        g_engine.getHashIndexedColumns(kDatabase, "items"), "tag"));
    assert(contains(
        g_engine.getBloomIndexedColumns(kDatabase, "items"), "tag"));

    assert(g_engine.hasFullTextIndex(kDatabase, "items", "payload"));
    assert(g_engine.hasGinIndex(kDatabase, "items", "payload"));
    assert(g_engine.hasGiSTIndex(kDatabase, "items", "score"));
    assert(g_engine.hasSPGiSTIndex(kDatabase, "items", "location"));
    assert(g_engine.hasBrinIndex(kDatabase, "items", "score"));
    assert(g_engine.fullTextSearch(
               kDatabase, "items", "payload", "needle").empty());
    assert(g_engine.ginSearch(
               kDatabase, "items", "payload", "needle").empty());
    assert(g_engine.giSTSearchOverlap(
               kDatabase, "items", "score", "42", "42").empty());
    assert(g_engine.spGiSTSearch(
               kDatabase, "items", "location", "=", "1.5,2.5").empty());
    assert(g_engine.brinSearchRange(
               kDatabase, "items", "score", "=", "42").empty());
}

}  // namespace

int main() {
    TypeRegistry::instance().bootstrap();
    fs::remove_all(kDatabase);
    fs::remove_all(kTablespace);
    fs::remove_all(".txnid");
    PageCrypto::disable();
    assert(PageCrypto::enable(std::string(64, '6')));

    assert(g_engine.createDatabase(kDatabase) == DBStatus::OK);
    assert(g_engine.createTablespace(
               kDatabase, "fast_space", kTablespace) == DBStatus::OK);

    TableSchema table;
    table.tablename = "items";
    table.formatVersion = 2;
    table.tablespace = "fast_space";
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("payload", false, 12000, false));
    table.append(makeVarCharColumn("tag", false, 64, false));
    table.append(makePointColumn("location", false, false));
    table.append(makeIntColumn("score", false, 4, false));
    assert(g_engine.createTable(kDatabase, table) == DBStatus::OK);

    const std::string payload = "needle " + makePayload(9000);
    assert(g_engine.insert(
               kDatabase, "items",
               {{"id", "1"}, {"payload", payload}, {"tag", "stale"},
                {"location", "1.5,2.5"}, {"score", "42"}}) ==
           DBStatus::OK);
    assert(g_engine.createIndex(kDatabase, "items", "tag") == DBStatus::OK);
    assert(g_engine.createIndex(kDatabase, "items", "tag", true, {}, "",
                                "UPPER(tag)") == DBStatus::OK);
    assert(g_engine.createCompositeIndex(
               kDatabase, "items", {"tag", "score"}, "tag_score") ==
           DBStatus::OK);
    assert(g_engine.createHashIndex(kDatabase, "items", "tag") ==
           DBStatus::OK);
    assert(g_engine.createBloomIndex(kDatabase, "items", "tag") ==
           DBStatus::OK);
    assert(g_engine.createFullTextIndex(
               kDatabase, "items", "payload") == DBStatus::OK);
    assert(g_engine.createGinIndex(kDatabase, "items", "payload") ==
           DBStatus::OK);
    assert(g_engine.createGiSTIndex(kDatabase, "items", "score") ==
           DBStatus::OK);
    assert(g_engine.createSPGiSTIndex(
               kDatabase, "items", "location") == DBStatus::OK);
    assert(g_engine.createBrinIndex(kDatabase, "items", "score", 1) ==
           DBStatus::OK);
    assert(g_engine.analyzeTable(kDatabase, "items"));

    int64_t oldRid = -1;
    assert(g_engine.getPKIndex(kDatabase, "items")->search("1", oldRid));
    assertOldIndexEntries(oldRid);

    TableSchema partitioned;
    partitioned.tablename = "partitioned_items";
    partitioned.formatVersion = 2;
    partitioned.tablespace = "fast_space";
    partitioned.partitionType = TableSchema::PartitionType::Hash;
    partitioned.partitionKey = "id";
    partitioned.hashPartitions = 2;
    partitioned.subPartitionType = TableSchema::PartitionType::Hash;
    partitioned.subPartitionKey = "tag";
    partitioned.subHashPartitions = 2;
    partitioned.append(makeIntColumn("id", false, 4, false));
    partitioned.append(makeVarCharColumn("tag", false, 64, false));
    assert(g_engine.createTable(kDatabase, partitioned) == DBStatus::OK);
    assert(g_engine.insert(kDatabase, "partitioned_items",
                           {{"id", "10"}, {"tag", "left"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(kDatabase, "partitioned_items",
                           {{"id", "11"}, {"tag", "right"}}) ==
           DBStatus::OK);
    assert(rowCount("partitioned_items") == 2);

    const fs::path relationRoot = fs::path(kTablespace) / kDatabase;
    const fs::path heap = relationRoot / "items.dt";
    const fs::path heapTde = fs::path(heap.string() + ".tde");
    const fs::path toastHeap = relationRoot / "items.toast.dt";
    const fs::path toastTde = fs::path(toastHeap.string() + ".tde");
    assert(g_engine.checkpoint(kDatabase));
    assert(fs::file_size(heap) > 8192);
    assert(fs::file_size(heapTde) > PageCrypto::kRecordSize);
    assert(fs::file_size(toastHeap) > 8192);
    assert(fs::file_size(toastTde) > PageCrypto::kRecordSize);
    assert(readToastNextId(relationRoot / "items.toastmeta") > 1);

    assert(g_engine.truncateTable(kDatabase, "items") == DBStatus::OK);
    assert(g_engine.truncateTable(kDatabase, "partitioned_items") ==
           DBStatus::OK);
    assert(rowCount("items") == 0);
    assert(rowCount("partitioned_items") == 0);
    assertTruncatedIndexes();

    assert(fs::file_size(heap) == 8192);
    assert(fs::file_size(heapTde) == 0);
    assert(fs::file_size(toastHeap) == 8192);
    assert(fs::file_size(toastTde) == 0);
    assert(readToastNextId(relationRoot / "items.toastmeta") == 1);
    assert(!fs::exists(relationRoot / "items.fsm"));
    assert(!fs::exists(relationRoot / "items.vm"));
    assert(fs::file_size(fs::path(kDatabase) / ".stats") == 0);

    assert(!fs::exists(relationRoot / "partitioned_items.dt"));
    for (size_t partition = 0; partition < 2; ++partition) {
        const std::string prefix =
            "partitioned_items#p" + std::to_string(partition);
        assert(fs::file_size(relationRoot / (prefix + ".dt")) == 8192);
        for (size_t subpartition = 0; subpartition < 2; ++subpartition) {
            assert(fs::file_size(
                relationRoot /
                (prefix + "#sp" + std::to_string(subpartition) + ".dt")) ==
                   8192);
        }
    }

    const std::string freshPayload = "fresh " + makePayload(8500);
    assert(g_engine.insert(
               kDatabase, "items",
               {{"id", "2"}, {"payload", freshPayload}, {"tag", "fresh"},
                {"location", "3.5,4.5"}, {"score", "7"}}) ==
           DBStatus::OK);
    int64_t freshRid = -1;
    assert(g_engine.getPKIndex(kDatabase, "items")->search("2", freshRid));
    assert(g_engine.getSecondaryIndex(kDatabase, "items", "tag")
               ->searchMulti("fresh") == std::vector<int64_t>{freshRid});
    assert(g_engine.getHashIndex(kDatabase, "items", "tag")
               ->search("fresh") == std::vector<int64_t>{freshRid});
    assert(g_engine.getBloomIndex(kDatabase, "items", "tag")
               ->search("fresh") == std::vector<int64_t>{freshRid});

    assert(g_engine.dropDatabase(kDatabase) == DBStatus::OK);
    fs::remove_all(kTablespace);
    fs::remove_all(".txnid");
    PageCrypto::disable();
    std::cout << "[TRUNCATE STORAGE] forks, indexes, TOAST, partitions and TDE OK\n";
    return 0;
}
