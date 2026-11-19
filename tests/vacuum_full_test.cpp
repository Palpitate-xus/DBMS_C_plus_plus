// test_link_flags: -Wl,--wrap=_ZN4dbms13PageAllocator4openEv,--wrap=_ZN4dbms13PageAllocator5closeEv,--wrap=_ZN4dbms13PageAllocator9fetchPageEj
#include "BPTree.h"
#include "BloomIndex.h"
#include "Config.h"
#include "HashIndex.h"
#include "TableManage.h"
#include "storage/HeapTupleHeader.h"
#include "storage/PageAllocator.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <tuple>
#include <vector>

dbms::Config g_config;

using namespace dbms;

namespace {
struct DestinationFetches;
thread_local DestinationFetches* activeDestination = nullptr;

struct DestinationFetches {
    explicit DestinationFetches(const std::filesystem::path& path, size_t failingFetch = 0)
        : heapPath(path), failOnFetch(failingFetch) {
        assert(!activeDestination);
        activeDestination = this;
    }
    ~DestinationFetches() { activeDestination = nullptr; }

    const std::filesystem::path& heapPath;
    const size_t failOnFetch;
    PageAllocator* owner = nullptr;
    size_t opens = 0;
    size_t closes = 0;
    size_t fetches = 0;
    bool failed = false;
};
} // namespace

extern "C" bool realOpen(PageAllocator*) asm("__real__ZN4dbms13PageAllocator4openEv");
extern "C" void realClose(PageAllocator*) asm("__real__ZN4dbms13PageAllocator5closeEv");
extern "C" char* realFetch(PageAllocator*, uint32_t) asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" bool wrapOpen(PageAllocator*) asm("__wrap__ZN4dbms13PageAllocator4openEv");
extern "C" void wrapClose(PageAllocator*) asm("__wrap__ZN4dbms13PageAllocator5closeEv");
extern "C" char* wrapFetch(PageAllocator*, uint32_t) asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");

extern "C" bool wrapOpen(PageAllocator* owner) {
    auto* trace = activeDestination;
    // The open that creates the missing target path owns the fresh heap.
    // Reindex opens the completed heap after this allocator closes.
    const bool targetMissing = trace && !std::filesystem::exists(trace->heapPath);
    const bool opened = realOpen(owner);
    if (targetMissing && opened && std::filesystem::is_regular_file(trace->heapPath)) {
        assert(!trace->owner && owner->numPages() == 1);
        trace->owner = owner;
        ++trace->opens;
    }
    return opened;
}

extern "C" void wrapClose(PageAllocator* owner) {
    if (activeDestination && activeDestination->owner == owner) {
        activeDestination->owner = nullptr;
        ++activeDestination->closes;
    }
    realClose(owner);
}

extern "C" char* wrapFetch(PageAllocator* owner, uint32_t pageId) {
    if (activeDestination && activeDestination->owner == owner &&
        ++activeDestination->fetches == activeDestination->failOnFetch) {
        activeDestination->failed = true;
        return nullptr;
    }
    return realFetch(owner, pageId);
}

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

static void testFirstFitRewrite(StorageEngine& engine, const std::string& database) {
    TableSchema table;
    table.tablename = "first_fit";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 4, true));
    constexpr std::array<const char*, 4> columns{"a", "b", "c", "d"};
    for (const char* name : columns) table.append(makeVarCharColumn(name, false, 2048));
    assert(engine.createTable(database, table) == DBStatus::OK);

    constexpr int lastId = 14;
    constexpr int firstFiller = 99;
    constexpr int secondFiller = 98;
    const std::map<int, std::array<size_t, 4>> widths{
        {1, {1500, 1500, 1500, 1500}},
        {2, {1600, 1600, 1600, 1600}},
        {lastId, {1200, 0, 0, 0}},
        {firstFiller, {1800, 0, 0, 0}},
        {secondFiller, {1500, 0, 0, 0}},
    };
    const auto value = [&](int id, size_t column) {
        const auto found = widths.find(id);
        const size_t width = found == widths.end() ? 1800 : found->second[column];
        return std::string(width, static_cast<char>('A' + id % 26));
    };
    const auto insert = [&](int id) {
        std::map<std::string, std::string> row{{"id", std::to_string(id)}};
        for (size_t i = 0; i < columns.size(); ++i) row.emplace(columns[i], value(id, i));
        assert(engine.insert(database, table.tablename, row) == DBStatus::OK);
    };
    // Occupy the available space on both early pages until the small row
    // lands at the end. Once the fillers are deleted, either page can fit it.
    insert(1);
    insert(firstFiller);
    insert(2);
    insert(secondFiller);
    for (int id = 3; id <= lastId; ++id) insert(id);
    for (const auto filler : {firstFiller, secondFiller})
        assert(engine.remove(database, table.tablename, {"=id " + std::to_string(filler)}) == DBStatus::OK);
    assert(engine.checkpoint(database));

    using Placement = std::tuple<int, uint32_t, uint16_t>;
    const auto placements = [&] {
        std::vector<Placement> result;
        assert(engine.forEachRow(database, table.tablename,
            [&](uint32_t page, uint16_t slot, const char* data, size_t length) {
                const std::string row(data, length);
                const auto id = std::stoi(engine.extractColumnValue(row, table, 0));
                assert(id >= 1 && id <= lastId);
                for (size_t i = 0; i < columns.size(); ++i)
                    assert(engine.extractColumnValue(row, table, i + 1) == value(id, i));
                result.emplace_back(id, page, slot);
            }));
        return result;
    };
    const auto heapPath = std::filesystem::path(database) / (table.tablename + ".dt");
    const auto indexPath = std::filesystem::path(database) / (table.tablename + ".idx");
    const size_t pageSize = StorageEngine::pageSizeForFormatVersion(table.formatVersion);
    std::vector<Placement> expected;
    for (uint32_t page = 1; page <= lastId; ++page)
        expected.emplace_back(static_cast<int>(page), page, 0);
    assert(placements() == expected);
    assert(std::filesystem::file_size(heapPath) == (lastId + 1) * pageSize);

    const auto readFile = [](const std::filesystem::path& path) {
        std::ifstream input(path, std::ios::binary);
        assert(input);
        std::string bytes{std::istreambuf_iterator<char>(input), {}};
        assert(!input.bad() && !bytes.empty());
        return bytes;
    };
    const auto originalHeap = readFile(heapPath);
    const auto originalIndex = readFile(indexPath);
    {
        DestinationFetches trace(heapPath, 3);
        bool succeeded = true;
        assert(engine.vacuumFull(database, table.tablename, &succeeded) == 0 && !succeeded);
        assert(trace.opens == 1 && trace.failed);
    }
    assert(!engine.inTransaction());
    const auto receiptPath = heapPath.parent_path() / ("." + table.tablename + ".vacuum_full_pending");
    assert(!std::filesystem::exists(receiptPath));
    assert(readFile(heapPath) == originalHeap && readFile(indexPath) == originalIndex);
    assert(placements() == expected);

    size_t fetches = 0;
    {
        DestinationFetches trace(heapPath);
        bool succeeded = false;
        assert(engine.vacuumFull(database, table.tablename, &succeeded) == lastId && succeeded);
        assert(trace.opens == 1 && trace.closes == 1 && !trace.owner);
        fetches = trace.fetches;
    }
    expected.pop_back();
    expected.insert(expected.begin() + 1, Placement{lastId, 1, 1});
    assert(placements() == expected);
    assert(std::filesystem::file_size(heapPath) == lastId * pageSize);
    BPTree* primary = engine.getPKIndex(database, table.tablename);
    assert(primary != nullptr);
    for (const auto& [id, page, slot] : expected) {
        int64_t rid = -1;
        assert(primary->search(std::to_string(id), rid));
        assert(rid == StorageEngine::encodeRid(page, slot));
    }

    auto* allocator = engine.getPageAllocator(database, table.tablename);
    assert(allocator && allocator->numPages() == lastId);
    size_t tupleCount = 0;
    size_t movedTupleSize = 0;
    for (uint32_t block = 1; block < lastId; ++block) {
        char* buffer = allocator->fetchPage(block);
        assert(buffer != nullptr);
        PageWrapper page(buffer, pageSize, table.formatVersion);
        assert(page.isValid(block) && page.verifyChecksum());
        if (block == 2) assert(movedTupleSize > 0 && page.canFit(movedTupleSize));
        page.forEachLive([&](uint16_t slot, const char* tuple, size_t length) {
            assert(length >= sizeof(HeapTupleHeaderData));
            const auto ctid = getCtid(castHeapHeader(tuple));
            assert(ctid.pageId == block && ctid.offset == slot + 1);
            if (block == 1 && slot == 1) movedTupleSize = length;
            ++tupleCount;
        });
        allocator->unpinPage(block);
    }
    assert(tupleCount == expected.size());

    std::cout << "[VACUUM FULL] first-fit rows=" << lastId
              << " destination_fetches=" << fetches << " restored_after_fetch_error=1" << std::endl;
    assert(fetches > 0 && fetches <= expected.size());
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
    // This table has no primary key, so there is no primary index to remove.
    // Inject a directory at the discovered index path directly; the backup
    // must reject it without replacing or losing the existing heap.
    assert(!std::filesystem::exists(unexpectedIndex));
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
        testFirstFitRewrite(engine, database);
    }

    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");
    std::filesystem::remove_all(".txnid");
    std::cout << "[VACUUM FULL] all passed\n";
    return 0;
}
