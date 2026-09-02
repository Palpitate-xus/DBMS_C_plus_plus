// TOAST chunked relation test.

#include "TableManage.h"
#include "Config.h"
#include "BPTree.h"
#include "PageAllocator.h"
#include "PageWrapper.h"
#include "ExecutionPlan.h"
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cassert>
#include <limits>
#include <thread>
#include <vector>

dbms::Config g_config;

using namespace dbms;

static std::string makeIncompressiblePayload(size_t size, uint32_t seed) {
    static constexpr char alphabet[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string value(size, '\0');
    uint32_t state = seed;
    for (char& ch : value) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        ch = alphabet[state % (sizeof(alphabet) - 1)];
    }
    return value;
}

int main() {
    std::string dbname = "toast_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    uint64_t markerId = 0;
    assert(StorageEngine::parseToastMarker("__TOAST__42", markerId));
    assert(markerId == 42);
    assert(!StorageEngine::parseToastMarker("__TOAST__0", markerId));
    assert(!StorageEngine::parseToastMarker("__TOAST__-1", markerId));
    assert(!StorageEngine::parseToastMarker("__TOAST__+1", markerId));
    assert(!StorageEngine::parseToastMarker("__TOAST__1junk", markerId));

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.formatVersion = 2;
        tbl.append(makeVarCharColumn("payload", false, 12000, false));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);

        // Verify TOAST relation and index files were created.
        assert(std::filesystem::exists(std::filesystem::path(dbname) / "t.toast.dt"));
        assert(std::filesystem::exists(std::filesystem::path(dbname) / "t.toast.idx"));

        const auto metaPath = std::filesystem::path(dbname) / "t.toastmeta";
        assert(std::filesystem::file_size(metaPath) == 24);
        std::ifstream metaIn(metaPath, std::ios::binary);
        const std::string validMeta((std::istreambuf_iterator<char>(metaIn)),
                                    std::istreambuf_iterator<char>());
        assert(validMeta.size() == 24);

        // A truncated or checksum-corrupt allocator state must not silently
        // restart at ID 1 and overwrite an existing external value.
        {
            std::ofstream out(metaPath, std::ios::binary | std::ios::trunc);
            out.write(validMeta.data(), 7);
        }
        assert(engine.insert(dbname, "t", {{"payload", std::string(10000, 'x')}})
               == DBStatus::IO_ERROR);
        {
            std::string corrupt = validMeta;
            corrupt.back() ^= 1;
            std::ofstream out(metaPath, std::ios::binary | std::ios::trunc);
            out.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
        }
        assert(engine.insert(dbname, "t", {{"payload", std::string(10000, 'y')}})
               == DBStatus::IO_ERROR);
        {
            std::ofstream out(metaPath, std::ios::binary | std::ios::trunc);
            out.write(validMeta.data(), static_cast<std::streamsize>(validMeta.size()));
        }
        assert(engine.query(dbname, "t", {}, {"payload"}).empty());
        std::cout << "[TOAST] corrupt ID allocator fails closed OK\n";

        std::string largeValue(10000, 'a');

        // Insert a row with a large value that exceeds the TOAST threshold.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["payload"] = largeValue;
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        // Query should return the original large value.
        auto rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.size() == 1);
        assert(rows[0].find(largeValue) != std::string::npos);
        // The current TOAST format compresses repetitive values before
        // chunking.  Account for the fixed 8 KiB relation header page: the
        // compressed payload should fit in one additional data page, while
        // an uncompressed 10 KiB value would require multiple chunks/pages.
        constexpr uintmax_t toastPageSize = 8192;
        assert(std::filesystem::file_size(std::filesystem::path(dbname) / "t.toast.dt") <=
               toastPageSize * 2);
        std::cout << "[TOAST] insert + query large value OK\n";

        // Update with another large value.
        std::string updatedValue(12000, 'b');
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> newVals;
        newVals["payload"] = updatedValue;
        assert(engine.update(dbname, "t", newVals, {}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.size() == 1);
        assert(rows[0].find(updatedValue) != std::string::npos);
        assert(rows[0].find(largeValue) == std::string::npos);
        std::cout << "[TOAST] update large value OK\n";

        // Delete the row.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.remove(dbname, "t", {}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.empty());
        std::cout << "[TOAST] delete large value OK\n";

        TableSchema concurrent;
        concurrent.tablename = "tc";
        concurrent.formatVersion = 2;
        concurrent.append(makeVarCharColumn("payload", false, 12000, false));
        assert(engine.createTable(dbname, concurrent) == DBStatus::OK);

        constexpr int workerCount = 12;
        std::atomic<int> ready{0};
        std::atomic<bool> start{false};
        std::atomic<bool> failed{false};
        std::vector<std::thread> workers;
        for (int worker = 0; worker < workerCount; ++worker) {
            workers.emplace_back([&, worker] {
                std::string payload(10000, '\0');
                for (size_t i = 0; i < payload.size(); ++i)
                    payload[i] = static_cast<char>('!' + ((i * 17 + worker * 29) % 90));
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
                if (engine.insert(dbname, "tc", {{"payload", payload}}) != DBStatus::OK)
                    failed.store(true, std::memory_order_release);
            });
        }
        while (ready.load(std::memory_order_acquire) != workerCount)
            std::this_thread::yield();
        start.store(true, std::memory_order_release);
        for (auto& worker : workers) worker.join();
        assert(!failed.load(std::memory_order_acquire));
        assert(engine.query(dbname, "tc", {}, {"payload"}).size() == workerCount);
        std::cout << "[TOAST] concurrent ID allocation unique OK\n";

        // De-TOASTing is an executor-only operation.  Two values can exceed
        // the on-page 16-bit offset range after expansion and must still be
        // extracted independently without wrapping the second offset.
        TableSchema wide;
        wide.tablename = "wide";
        wide.formatVersion = 2;
        wide.append(makeVarCharColumn("left_value", false, 65535, false));
        wide.append(makeVarCharColumn("right_value", false, 65535, false));
        assert(engine.createTable(dbname, wide) == DBStatus::OK);
        const std::string leftValue = makeIncompressiblePayload(40000, 0x12345678u);
        const std::string rightValue = makeIncompressiblePayload(40000, 0x9abcdef0u);
        assert(engine.insert(dbname, "wide",
                             {{"left_value", leftValue}, {"right_value", rightValue}})
               == DBStatus::OK);
        rows = engine.query(dbname, "wide", {}, {"left_value", "right_value"});
        assert(rows.size() == 1);
        assert(rows[0].find(leftValue) != std::string::npos);
        assert(rows[0].find(rightValue) != std::string::npos);
        assert(engine.query(dbname, "wide", {"=right_value " + rightValue},
                            {"left_value"}).size() == 1);
        std::cout << "[TOAST] multi-column expansion + predicate OK\n";

        // User data that has the internal marker spelling must round-trip as
        // literal text rather than aliasing toast ID 1 from another row.
        TableSchema markerTable;
        markerTable.tablename = "marker_literal";
        markerTable.formatVersion = 2;
        markerTable.append(makeVarCharColumn("payload", false, 10000, false));
        assert(engine.createTable(dbname, markerTable) == DBStatus::OK);
        const std::string markerSource = makeIncompressiblePayload(9000, 0x2468ace0u);
        assert(engine.insert(dbname, "marker_literal", {{"payload", markerSource}})
               == DBStatus::OK);
        assert(engine.insert(dbname, "marker_literal", {{"payload", "__TOAST__1"}})
               == DBStatus::OK);
        rows = engine.query(dbname, "marker_literal", {}, {"payload"});
        assert(rows.size() == 2);
        bool sawSource = false;
        bool sawLiteral = false;
        for (const auto& row : rows) {
            sawSource |= row.find(markerSource) != std::string::npos;
            sawLiteral |= row == "__TOAST__1 ";
        }
        assert(sawSource && sawLiteral);
        std::cout << "[TOAST] marker-shaped user value round-trip OK\n";

        // Values above a declared VARCHAR limit used to bypass the normal
        // inline truncation path simply by being large enough for TOAST.
        TableSchema bounded;
        bounded.tablename = "bounded";
        bounded.formatVersion = 2;
        bounded.append(makeVarCharColumn("payload", false, 3000, false));
        assert(engine.createTable(dbname, bounded) == DBStatus::OK);
        const std::string atLimit = makeIncompressiblePayload(3000, 0x55667788u);
        const std::string aboveLimit = makeIncompressiblePayload(3001, 0x55667788u);
        assert(engine.insert(dbname, "bounded", {{"payload", aboveLimit}})
               == DBStatus::INVALID_VALUE);
        assert(engine.insert(dbname, "bounded", {{"payload", atLimit}})
               == DBStatus::OK);
        assert(engine.update(dbname, "bounded", {{"payload", aboveLimit}}, {})
               == DBStatus::INVALID_VALUE);
        rows = engine.query(dbname, "bounded", {}, {"payload"});
        assert(rows.size() == 1);
        assert(rows[0].find(atLimit) != std::string::npos);
        std::cout << "[TOAST] declared variable-width limit enforced OK\n";

        TableSchema corruptGap;
        corruptGap.tablename = "corrupt_gap";
        corruptGap.formatVersion = 2;
        corruptGap.append(makeVarCharColumn("payload", false, 12000, false));
        assert(engine.createTable(dbname, corruptGap) == DBStatus::OK);
        assert(engine.insert(dbname, "corrupt_gap",
                             {{"payload", makeIncompressiblePayload(10000, 0x13579bdfu)}})
               == DBStatus::OK);

        TableSchema corruptSize = corruptGap;
        corruptSize.tablename = "corrupt_size";
        assert(engine.createTable(dbname, corruptSize) == DBStatus::OK);
        assert(engine.insert(dbname, "corrupt_size",
                             {{"payload", makeIncompressiblePayload(10000, 0x10203040u)}})
               == DBStatus::OK);
    }

    // Removing an interior chunk must not turn the prefix into a valid value.
    // The stored original length makes this detectable even for an
    // uncompressed payload.
    int64_t missingChunkRid = -1;
    {
        BPTree index(std::filesystem::path(dbname) / "corrupt_gap.toast.idx");
        assert(index.open());
        assert(index.search("T1:1", missingChunkRid));
    }
    {
        uint32_t pageId = 0;
        uint16_t slotId = 0;
        StorageEngine::decodeRid(missingChunkRid, pageId, slotId);
        PageAllocator pages((std::filesystem::path(dbname) /
                             "corrupt_gap.toast.dt").string(),
                            0, 8192, 2);
        assert(pages.open());
        char* buffer = pages.fetchPage(pageId);
        assert(buffer != nullptr);
        PageWrapper page(buffer, pages.pageSize(), 2);
        assert(page.remove(slotId));
        page.writeChecksum();
        pages.markDirty(pageId);
        pages.unpinPage(pageId);
        assert(pages.flush());
    }

    // A forged decompressed length is bounded by the column's declared
    // maximum before allocating the destination buffer.
    {
        const auto indexPath = std::filesystem::path(dbname) /
                               "corrupt_size.toast.idx";
        BPTree index(indexPath);
        assert(index.open());
        int64_t firstRid = -1;
        assert(index.search("T1:0", firstRid));
        uint32_t pageId = 0;
        uint16_t slotId = 0;
        StorageEngine::decodeRid(firstRid, pageId, slotId);

        PageAllocator pages((std::filesystem::path(dbname) /
                             "corrupt_size.toast.dt").string(),
                            0, 8192, 2);
        assert(pages.open());
        char* buffer = pages.fetchPage(pageId);
        assert(buffer != nullptr);
        PageWrapper page(buffer, pages.pageSize(), 2);
        const char* chunk = nullptr;
        size_t chunkLength = 0;
        assert(page.read(slotId, chunk, chunkLength));
        constexpr size_t originalSizeOffset = sizeof(uint64_t) +
                                              sizeof(uint32_t) + sizeof(uint8_t);
        assert(chunkLength >= originalSizeOffset + sizeof(uint64_t));
        const uint64_t forgedSize = std::numeric_limits<uint64_t>::max();
        std::memcpy(const_cast<char*>(chunk) + originalSizeOffset,
                    &forgedSize, sizeof(forgedSize));
        page.writeChecksum();
        pages.markDirty(pageId);
        pages.unpinPage(pageId);
        assert(pages.flush());
    }

    {
        StorageEngine engine;
        assert(engine.query(dbname, "corrupt_gap", {}, {"payload"}).empty());
        assert(engine.query(dbname, "corrupt_size", {}, {"payload"}).empty());
        TableScanOp corruptScan(&engine, dbname, "corrupt_gap");
        assert(!corruptScan.open());
        std::cout << "[TOAST] corrupt chunk metadata fails closed OK\n";
    }

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[TOAST] all passed\n";
    return 0;
}
