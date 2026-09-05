// Checkpoint test: verify checkpoint record and persistent checkpoint file.

#include "storage/BufferPool.h"
#include "storage/PageAllocator.h"
#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <algorithm>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cassert>
#include <cstring>
#include <sys/resource.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

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

        // Background writeback must defer a dirty pinned frame without even
        // invoking its WAL barrier. Once unpinned, the barrier runs while the
        // old disk image is still present, then the new image is written.
        char* backgroundPage = pool.fetchPage(0);
        assert(backgroundPage != nullptr);
        std::memcpy(backgroundPage, "background!!", 12);
        pool.markDirty(0);
        bool barrierCalled = false;
        assert(pool.flushDirtyUnpinned([&]() {
            barrierCalled = true;
            return true;
        }));
        assert(!barrierCalled);
        pool.unpinPage(0);
        assert(!pool.flushDirtyUnpinned([] { return false; }));
        const auto dirtyAfterBarrierFailure = pool.getFrameInfo();
        assert(dirtyAfterBarrierFailure.size() == 1);
        assert(dirtyAfterBarrierFailure.front().dirty);
        assert(pool.flushDirtyUnpinned([&]() {
            barrierCalled = true;
            std::ifstream disk(poolPath, std::ios::binary);
            char oldBytes[12]{};
            assert(disk.read(oldBytes, sizeof(oldBytes)));
            return std::memcmp(oldBytes, "durable-page", 12) == 0;
        }));
        assert(barrierCalled);
        pool.invalidatePage(0);
        reloaded = pool.fetchPage(0);
        assert(reloaded != nullptr);
        assert(std::memcmp(reloaded, "background!!", 12) == 0);
        pool.unpinPage(0);
        barrierCalled = false;
        assert(pool.flushDirtyUnpinned([&]() {
            barrierCalled = true;
            return true;
        }));
        assert(!barrierCalled);  // a clean pool performs no fsync/barrier
        pool.close();
        std::filesystem::remove(poolPath);
        std::cout << "[CHECKPOINT] BufferPool eviction/pin safety OK\n";
    }

    // Crash between an appended page write and the final allocator-header
    // write must not leave an unopenable relation. Force the child to hit its
    // file-size limit partway through page 1, then exit without destructors;
    // the parent's open must consume the pending marker and restore page 0.
    {
        const std::filesystem::path extentPath =
            "checkpoint_extent_recovery.dt";
        std::filesystem::remove(extentPath);
        std::filesystem::remove(extentPath.string() + ".tde");
        std::filesystem::remove(extentPath.string() + ".extent_pending");
        {
            PageAllocator initial(extentPath.string(), 32);
            assert(initial.open());
            assert(initial.flush());
        }
        assert(std::filesystem::file_size(extentPath) == PgPage::PAGE_SIZE);

        const pid_t child = ::fork();
        assert(child >= 0);
        if (child == 0) {
            std::signal(SIGXFSZ, SIG_IGN);
            PageAllocator* interrupted =
                new PageAllocator(extentPath.string(), 32);
            if (!interrupted->open() || interrupted->allocPage() != 1) {
                ::_exit(2);
            }
            struct rlimit limit {};
            if (::getrlimit(RLIMIT_FSIZE, &limit) != 0) ::_exit(3);
            limit.rlim_cur = std::min<rlim_t>(limit.rlim_max, 12000);
            if (limit.rlim_cur <= PgPage::PAGE_SIZE ||
                ::setrlimit(RLIMIT_FSIZE, &limit) != 0) {
                ::_exit(4);
            }
            if (interrupted->flush()) ::_exit(5);
            ::_exit(0);  // model kill -9: intentionally skip destructors
        }
        int status = 0;
        assert(::waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        assert(std::filesystem::exists(
            extentPath.string() + ".extent_pending"));
        {
            PageAllocator recovered(extentPath.string(), 32);
            assert(recovered.open());
            assert(recovered.numPages() == 1);
            assert(std::filesystem::file_size(extentPath) ==
                   PgPage::PAGE_SIZE);

            // The same allocator must also retry its own marker in place;
            // restoring the old disk image underneath a live cache would
            // lose frames already made clean by a completed writeback.
            assert(recovered.allocPage() == 1);
            struct rlimit savedLimit {};
            assert(::getrlimit(RLIMIT_FSIZE, &savedLimit) == 0);
            struct rlimit shortLimit = savedLimit;
            shortLimit.rlim_cur =
                std::min<rlim_t>(shortLimit.rlim_max, 12000);
            const auto savedHandler = std::signal(SIGXFSZ, SIG_IGN);
            assert(shortLimit.rlim_cur > PgPage::PAGE_SIZE);
            assert(::setrlimit(RLIMIT_FSIZE, &shortLimit) == 0);
            assert(!recovered.flush());
            assert(std::filesystem::exists(
                extentPath.string() + ".extent_pending"));
            assert(::setrlimit(RLIMIT_FSIZE, &savedLimit) == 0);
            std::signal(SIGXFSZ, savedHandler);
            assert(recovered.flush());
            assert(recovered.numPages() == 2);
            assert(std::filesystem::file_size(extentPath) ==
                   2 * PgPage::PAGE_SIZE);
        }
        assert(!std::filesystem::exists(
            extentPath.string() + ".extent_pending"));
        std::filesystem::remove(extentPath);
        std::filesystem::remove(extentPath.string() + ".tde");
        std::cout << "[CHECKPOINT] interrupted extent publication recovers OK\n";
    }

    std::string dbname = "checkpoint_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        // Construct the observer before the database exists. It therefore
        // cannot repair the writer through startup WAL recovery; seeing a
        // later commit proves the commit path itself published the extent.
        StorageEngine observer;
        // Let a possible first 200ms worker pass finish, then keep the
        // background writer out of the assertions below. They must prove the
        // commit path itself wrote its pages, not pass due to a timed flush.
        engine.setBackgroundIntervals(60000, 60000);
        observer.setBackgroundIntervals(60000, 60000);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.formatVersion = 2;
        tbl.append(makeIntColumn("id", false, 0, true));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);

        // A newly allocated data page and its allocator-header numPages
        // update form one physical extent change. COMMIT publishes the data
        // page first and the header last, so another already-running engine
        // sees the row immediately without relying on BEGIN/bgwriter flushes.
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
        assert(freshPage != freshFrames.end() && !freshPage->dirty);
        const std::filesystem::path freshPath =
            std::filesystem::path(dbname) / "fresh_extent.dt";
        assert(std::filesystem::file_size(freshPath) ==
               2 * freshAllocator->pageSize());
        DataFileHeader durableHeader{};
        {
            std::ifstream input(freshPath, std::ios::binary);
            assert(input.read(
                reinterpret_cast<char*>(&durableHeader),
                sizeof(durableHeader)));
        }
        assert(durableHeader.magic == DATA_FILE_MAGIC);
        assert(durableHeader.numPages == 2);
        assert(durableHeader.headerChecksum ==
               computeDataFileHeaderChecksum(durableHeader));
        assert(observer.query(
                   dbname, fresh.tablename, {}, {"id"}).size() == 1);
        assert(!std::filesystem::exists(
            freshPath.string() + ".extent_pending"));
        std::cout << "[CHECKPOINT] commit publishes fresh extent atomically OK\n";

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "1";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(!engine.checkpoint(dbname));
        // The database mutex is process-wide, so a different engine cannot
        // pass the active-set check/flush boundary either.
        assert(!observer.checkpoint(dbname));
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

        // BEGIN only creates logical snapshots. It must not flush a dirty
        // page owned by another active transaction on the same database.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        vals["id"] = "3";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        const auto heapPageIsDirty = [&]() {
            PageAllocator* allocator = engine.getPageAllocator(dbname, "t");
            assert(allocator != nullptr && allocator->bufferPool() != nullptr);
            const auto frames = allocator->bufferPool()->getFrameInfo();
            const auto page = std::find_if(
                frames.begin(), frames.end(),
                [](const BufferPool::FrameInfo& frame) {
                    return frame.pageId == 1;
                });
            return page != frames.end() && page->dirty;
        };
        assert(heapPageIsDirty());
        DBStatus peerBegin = DBStatus::IO_ERROR;
        DBStatus peerCommit = DBStatus::IO_ERROR;
        std::thread peer([&]() {
            peerBegin = engine.beginTransaction(dbname);
            if (peerBegin == DBStatus::OK) {
                peerCommit = engine.commitTransaction();
            }
        });
        peer.join();
        assert(peerBegin == DBStatus::OK && peerCommit == DBStatus::OK);
        assert(heapPageIsDirty());
        assert(engine.rollbackTransaction() == DBStatus::OK);
        std::cout << "[CHECKPOINT] BEGIN leaves peer dirty pages untouched OK\n";

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
