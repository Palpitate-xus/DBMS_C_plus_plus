#include "BPTree.h"
#include "BloomIndex.h"
#include "Config.h"
#include "HashIndex.h"
#include "TableManage.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

dbms::Config g_config;

using namespace dbms;

static std::string makePayload(size_t size) {
    static constexpr char alphabet[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string value(size, '\0');
    uint32_t state = 0x13579bdfu;
    for (char& ch : value) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        ch = alphabet[state % (sizeof(alphabet) - 1)];
    }
    return value;
}

static size_t rowCount(StorageEngine& engine, const std::string& database,
                       const std::string& table) {
    size_t count = 0;
    assert(engine.forEachRow(
        database, table,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
    return count;
}

static void testHeapAndIndexes(StorageEngine& engine,
                               const std::string& database) {
    TableSchema table;
    table.tablename = "items";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("payload", false, 12000, false));
    table.append(makeVarCharColumn("tag", false, 64, false));
    table.append(makeVarCharColumn("note", true, 64, false));
    table.append(makePointColumn("location", false, false));
    assert(engine.createTable(database, table) == DBStatus::OK);

    const std::string payload = makePayload(9000);
    assert(engine.insert(database, "items",
                         {{"id", "1"}, {"payload", "discard"},
                          {"tag", "discard"}, {"note", "old"},
                          {"location", "0,0"}}) == DBStatus::OK);
    assert(engine.insert(database, "items",
                         {{"id", "2"}, {"payload", payload},
                          {"tag", "survivor_needle"},
                          {"location", "1.5,2.5"}}) == DBStatus::OK);
    assert(engine.insert(database, "items",
                         {{"id", "3"}, {"payload", "small"},
                          {"tag", "other"}, {"note", ""},
                          {"location", "3.5,4.5"}}) == DBStatus::OK);
    assert(engine.remove(database, "items", {"=id 1"}) == DBStatus::OK);

    assert(engine.createIndex(database, "items", "tag") == DBStatus::OK);
    assert(engine.createIndex(database, "items", "tag", true, {}, "",
                              "UPPER(tag)") == DBStatus::OK);
    assert(engine.createCompositeIndex(database, "items", {"tag", "id"},
                                       "tag_id") == DBStatus::OK);
    assert(engine.createHashIndex(database, "items", "tag") == DBStatus::OK);
    assert(engine.createBloomIndex(database, "items", "tag") == DBStatus::OK);
    assert(engine.createFullTextIndex(database, "items", "tag") == DBStatus::OK);
    assert(engine.createGinIndex(database, "items", "tag") == DBStatus::OK);
    assert(engine.createGiSTIndex(database, "items", "tag") == DBStatus::OK);
    assert(engine.createSPGiSTIndex(database, "items", "location") == DBStatus::OK);
    assert(engine.createBrinIndex(database, "items", "tag", 64) == DBStatus::OK);

    assert(engine.vacuumFull(database, "items") == 2);
    assert(rowCount(engine, database, "items") == 2);
    const auto payloadRows = engine.query(
        database, "items", {"=id 2"}, {"payload"});
    assert(payloadRows.size() == 1);
    assert(payloadRows.front().find(payload) != std::string::npos);

    BPTree* primary = engine.getPKIndex(database, "items");
    assert(primary != nullptr);
    int64_t survivorRid = -1;
    assert(primary->search("2", survivorRid));
    assert(engine.isColumnNullByRid(database, "items", survivorRid, 3));
    int64_t emptyNoteRid = -1;
    assert(primary->search("3", emptyNoteRid));
    assert(!engine.isColumnNullByRid(database, "items", emptyNoteRid, 3));

    BPTree* tagIndex = engine.getSecondaryIndex(database, "items", "tag");
    BPTree* expressionIndex = engine.getSecondaryIndex(
        database, "items", "UPPER(tag)");
    BPTree* compositeIndex = engine.getCompositeIndexTree(
        database, "items", "tag_id");
    HashIndex* hashIndex = engine.getHashIndex(database, "items", "tag");
    BloomIndex* bloomIndex = engine.getBloomIndex(database, "items", "tag");
    assert(tagIndex && expressionIndex && compositeIndex && hashIndex && bloomIndex);
    assert(tagIndex->searchMulti("survivor_needle") ==
           std::vector<int64_t>{survivorRid});
    assert(expressionIndex->searchMulti("SURVIVOR_NEEDLE") ==
           std::vector<int64_t>{survivorRid});
    assert(compositeIndex->searchMulti(
               std::string("survivor_needle") + '\x01' + "2") ==
           std::vector<int64_t>{survivorRid});
    assert(hashIndex->search("survivor_needle") ==
           std::vector<int64_t>{survivorRid});
    assert(bloomIndex->search("survivor_needle") ==
           std::vector<int64_t>{survivorRid});
    assert(engine.fullTextSearch(database, "items", "tag", "survivor") ==
           std::vector<int64_t>{survivorRid});
    assert(engine.ginSearch(database, "items", "tag", "survivor") ==
           std::vector<int64_t>{survivorRid});
    assert(engine.giSTSearchOverlap(database, "items", "tag",
                                    "survivor_needle", "survivor_needle") ==
           std::vector<int64_t>{survivorRid});
    assert(engine.spGiSTSearch(database, "items", "location", "=",
                               "1.5,2.5") ==
           std::vector<int64_t>{survivorRid});
    assert(!engine.brinSearchRange(database, "items", "tag", "=",
                                   "survivor_needle").empty());
    std::cout << "[VACUUM FULL] heap, NULL, TOAST and indexes preserved OK\n";
}

static void testPartitionRewrite(StorageEngine& engine,
                                 const std::string& database) {
    TableSchema table;
    table.tablename = "events";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, false));
    table.append(makeVarCharColumn("region", false, 16, false));
    table.append(makeVarCharColumn("payload", false, 12000, false));
    table.partitionType = TableSchema::PartitionType::List;
    table.partitionKey = "region";
    table.listPartitions = {{"east", {"NY"}}, {"west", {"CA"}}};
    table.defaultPartitionName = "other";
    assert(engine.createTable(database, table) == DBStatus::OK);

    const std::string payload = makePayload(8500);
    assert(engine.insert(database, "events",
                         {{"id", "1"}, {"region", "NY"},
                          {"payload", "old"}}) == DBStatus::OK);
    assert(engine.insert(database, "events",
                         {{"id", "2"}, {"region", "NY"},
                          {"payload", payload}}) == DBStatus::OK);
    assert(engine.insert(database, "events",
                         {{"id", "3"}, {"region", "CA"},
                          {"payload", "west"}}) == DBStatus::OK);
    assert(engine.insert(database, "events",
                         {{"id", "4"}, {"region", "TX"},
                          {"payload", "default"}}) == DBStatus::OK);
    assert(engine.vacuumFull(database, "events") == 4);
    assert(rowCount(engine, database, "events") == 4);
    const auto rows = engine.query(
        database, "events", {"=id 2"}, {"payload"});
    assert(rows.size() == 1);
    assert(rows.front().find(payload) != std::string::npos);
    std::cout << "[VACUUM FULL] partition files rewritten without duplicates OK\n";
}

static void testBackupFailureIsNonDestructive(StorageEngine& engine,
                                              const std::string& database) {
    TableSchema table;
    table.tablename = "guarded";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, false));
    assert(engine.createTable(database, table) == DBStatus::OK);
    assert(engine.insert(database, "guarded", {{"id", "7"}}) == DBStatus::OK);
    assert(engine.beginTransaction(database) == DBStatus::OK);
    assert(engine.vacuumFull(database, "guarded") == 0);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assert(rowCount(engine, database, "guarded") == 1);

    const auto unexpectedIndex =
        std::filesystem::path(database) / "guarded.idx";
    assert(std::filesystem::remove(unexpectedIndex));
    assert(std::filesystem::create_directory(unexpectedIndex));
    assert(engine.vacuumFull(database, "guarded") == 0);
    assert(rowCount(engine, database, "guarded") == 1);
    const auto rows = engine.query(database, "guarded", {"=id 7"}, {"id"});
    assert(rows.size() == 1);
    assert(std::filesystem::remove(unexpectedIndex));
    std::cout << "[VACUUM FULL] backup failure leaves heap untouched OK\n";
}

static void testRewriteFailureRestoresBackup(StorageEngine& engine,
                                             const std::string& database) {
    TableSchema table;
    table.tablename = "restored";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("value", false, 64, false));
    assert(engine.createTable(database, table) == DBStatus::OK);
    assert(engine.insert(database, "restored",
                         {{"id", "9"}, {"value", "keep-me"}}) ==
           DBStatus::OK);
    BPTree* primary = engine.getPKIndex(database, "restored");
    assert(primary != nullptr);
    int64_t originalRid = -1;
    assert(primary->search("9", originalRid));

    // Sidecar discovery treats this as an SP-GiST definition.  Rebuilding it
    // must fail because the named column does not exist, after the heap swap
    // has started, and exercise restoration of every prior physical file.
    const auto invalidSidecar =
        std::filesystem::path(database) / "restored_missing.spgist";
    {
        std::ofstream output(invalidSidecar, std::ios::trunc);
        output << originalRid << " 1,1\n";
        assert(output.good());
    }
    assert(engine.vacuumFull(database, "restored") == 0);
    assert(rowCount(engine, database, "restored") == 1);
    primary = engine.getPKIndex(database, "restored");
    assert(primary != nullptr);
    int64_t restoredRid = -1;
    assert(primary->search("9", restoredRid));
    assert(restoredRid == originalRid);
    const auto rows = engine.query(
        database, "restored", {"=id 9"}, {"value"});
    assert(rows.size() == 1);
    assert(rows.front().find("keep-me") != std::string::npos);
    assert(std::filesystem::is_regular_file(invalidSidecar));
    std::cout << "[VACUUM FULL] mid-rewrite failure restores backup OK\n";
}

int main() {
    const std::string database = "vacuum_full_db";
    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        assert(engine.createDatabase(database) == DBStatus::OK);
        testHeapAndIndexes(engine, database);
        testPartitionRewrite(engine, database);
        testBackupFailureIsNonDestructive(engine, database);
        testRewriteFailureRestoresBackup(engine, database);
    }

    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");
    std::filesystem::remove_all(".txnid");
    std::cout << "[VACUUM FULL] all passed\n";
    return 0;
}
