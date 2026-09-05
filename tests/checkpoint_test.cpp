// Checkpoint test: verify checkpoint record and persistent checkpoint file.

#include "storage/BufferPool.h"
#include "storage/PageAllocator.h"
#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cassert>
#include <cstring>
#include <thread>

dbms::Config g_config;

using namespace dbms;

int main() {
    // A pinned frame must not be force-evicted, and a dirty frame must survive
    // a normal clock-sweep eviction and reload from disk.
    {
        const std::filesystem::path poolPath = "checkpoint_buffer_pool.dat";
        std::filesystem::remove(poolPath);
        BufferPool pool(poolPath.string(), 1, 128);
        assert(pool.open());
        char* page = pool.fetchPage(0);
        assert(page != nullptr);
        std::memcpy(page, "durable-page", 12);
        pool.markDirty(0);
        pool.unpinPage(0);

        char* other = pool.fetchPage(1);
        assert(other != nullptr);
        pool.unpinPage(1);
        char* reloaded = pool.fetchPage(0);
        assert(reloaded != nullptr);
        assert(std::memcmp(reloaded, "durable-page", 12) == 0);

        // Keep page 0 pinned.  With a one-frame pool, fetching page 2 must
        // fail closed instead of evicting the live page.
        assert(pool.fetchPage(2) == nullptr);
        pool.unpinPage(0);
        assert(pool.fetchPage(2) != nullptr);
        pool.unpinPage(2);
        assert(pool.flush());

        // PageAllocator exposes close/open as a reusable lifecycle. close()
        // frees the frame array, so open() must recreate it before fetch.
        pool.close();
        assert(pool.open());
        reloaded = pool.fetchPage(0);
        assert(reloaded != nullptr);
        assert(std::memcmp(reloaded, "durable-page", 12) == 0);
        pool.unpinPage(0);

        // COMMIT writeback must rewrite a cached page even if an overlapping
        // background pass already cleared its dirty bit after publishing a
        // bad/torn disk copy. Model that state by corrupting the file behind
        // a clean cached frame, then require flushPage() to repair it.
        {
            std::fstream disk(poolPath, std::ios::in | std::ios::out |
                                           std::ios::binary);
            assert(disk);
            disk.seekp(0);
            disk.write("corrupt-page", 12);
            disk.flush();
            assert(disk.good());
        }
        assert(pool.flushPage(0));
        pool.invalidatePage(0);
        reloaded = pool.fetchPage(0);
        assert(reloaded != nullptr);
        assert(std::memcmp(reloaded, "durable-page", 12) == 0);
        pool.unpinPage(0);
        pool.close();
        std::filesystem::remove(poolPath);
        std::cout << "[CHECKPOINT] BufferPool eviction/pin safety OK\n";
    }

    std::string dbname = "checkpoint_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        // Let a possible first 200ms worker pass finish, then keep the
        // background writer out of the assertions below. They must prove the
        // commit path itself wrote its pages, not pass due to a timed flush.
        engine.setBackgroundIntervals(60000, 60000);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.formatVersion = 2;
        tbl.append(makeIntColumn("id", false, 0, true));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);

        // A newly allocated data page and its allocator-header numPages
        // update form one physical extent change. COMMIT must not write just
        // the data page and leave an immediately crashed file inconsistent;
        // WAL redo/checkpoint will publish the complete extent later.
        TableSchema fresh = tbl;
        fresh.tablename = "fresh_extent";
        assert(engine.createTable(dbname, fresh) == DBStatus::OK);
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> freshValues{{"id", "1"}};
        assert(engine.insert(
                   dbname, fresh.tablename, freshValues) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
        PageAllocator* freshAllocator =
            engine.getPageAllocator(dbname, fresh.tablename);
        assert(freshAllocator != nullptr &&
               freshAllocator->bufferPool() != nullptr);
        const auto freshFrames =
            freshAllocator->bufferPool()->getFrameInfo();
        const auto freshPage = std::find_if(
            freshFrames.begin(), freshFrames.end(),
            [](const BufferPool::FrameInfo& frame) {
                return frame.pageId == 1;
            });
        assert(freshPage != freshFrames.end() && freshPage->dirty);
        assert(std::filesystem::file_size(
                   std::filesystem::path(dbname) / "fresh_extent.dt") ==
               freshAllocator->pageSize());
        std::cout << "[CHECKPOINT] fresh extent remains WAL-backed OK\n";

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "1";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(!engine.checkpoint(dbname));
        assert(engine.rollbackTransaction() == DBStatus::OK);

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        vals["id"] = "1";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        const auto heapPageIsClean = [&]() {
            PageAllocator* allocator = engine.getPageAllocator(dbname, "t");
            assert(allocator != nullptr && allocator->bufferPool() != nullptr);
            const auto frames = allocator->bufferPool()->getFrameInfo();
            const auto page = std::find_if(
                frames.begin(), frames.end(),
                [](const BufferPool::FrameInfo& frame) {
                    return frame.pageId == 1;
                });
            return page != frames.end() && !page->dirty;
        };
        assert(heapPageIsClean());

        // SERIALIZABLE cleanup must retain the same writeback list until the
        // WAL commit and heap publication boundary has completed.
        engine.setIsolationLevel(IsolationLevel::SERIALIZABLE);
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        vals["id"] = "2";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
        assert(heapPageIsClean());
        std::cout << "[CHECKPOINT] transaction-scoped heap writeback OK\n";

        assert(engine.checkpoint(dbname));
    }

    // Verify checkpoint file exists with LSN.
    std::filesystem::path cpPath = std::filesystem::path(dbname) / "checkpoint";
    assert(std::filesystem::exists(cpPath));
    assert(std::filesystem::file_size(cpPath) == 3 * sizeof(uint64_t));
    {
        std::ifstream cp(cpPath, std::ios::binary);
        uint64_t timestamp = 0, maxTxId = 0, ckptLsn = 0;
        cp.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
        cp.read(reinterpret_cast<char*>(&maxTxId), sizeof(maxTxId));
        cp.read(reinterpret_cast<char*>(&ckptLsn), sizeof(ckptLsn));
        assert(cp.gcount() == static_cast<std::streamsize>(sizeof(ckptLsn)));
        assert(cp.peek() == std::char_traits<char>::eof());
        assert(ckptLsn > 0);
        std::cout << "[CHECKPOINT] checkpoint file contains LSN " << ckptLsn << "\n";
    }

    // Verify WAL checkpoint record.
    std::filesystem::path walDir = std::filesystem::path(dbname) / "pg_wal";
    WALManager wal(walDir);
    assert(wal.ensureOpen());
    auto ckptLsnOpt = wal.findLastCheckpointLsn();
    assert(ckptLsnOpt.has_value());
    auto recOpt = wal.ReadRecord(*ckptLsnOpt);
    assert(recOpt.has_value());
    assert(recOpt->rmid() == RM_CHECKPOINT_ID);
    assert(recOpt->info() == XLOG_CHECKPOINT_SHUTDOWN);
    assert(recOpt->data.size() >= sizeof(uint64_t));
    std::cout << "[CHECKPOINT] WAL checkpoint record found at LSN " << *ckptLsnOpt << "\n";

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[CHECKPOINT] all passed\n";
    return 0;
}
