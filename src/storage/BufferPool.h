#pragma once

#include "storage/PageCrypto.h"

#include <cstdint>
#include <cstring>
#include <condition_variable>
#include <fcntl.h>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace dbms {

// Sentinel pageId for a frame whose page was invalidated while readers
// still held it pinned: the buffer must stay valid until the last unpin,
// but the frame is no longer any page's cached copy and cannot be evicted
// or reloaded underneath those readers.
constexpr uint32_t kOrphanedPage = 0xFFFFFFFE;

// ========================================================================
// BufferPool - Shared buffer cache with Clock Sweep eviction
// ========================================================================
//
// Design goals for Step 1.7:
//   1. Thread-safe: all public methods are protected by a single mutex.
//      (Future: partition into multiple bucket locks.)
//   2. Clock Sweep eviction: O(1) amortized, no list splicing on hits.
//   3. Pin / usage-count separation:
//      - pinCount   = "this frame is in use, do NOT evict"
//      - usageCount = "recently accessed, give a second chance"
//   4. Runtime page size (carried over from Step 1.4).
//
// An optional page validator runs once per page when it is loaded from disk.
// Cache hits skip validation: a validated page cannot change while pinned in
// the pool, so re-running a whole-page checksum on every fetch only burns
// CPU. Callers that mutate pages directly through the frame pointer remain
// responsible for the invariants they write.

class BufferPool {
public:
    explicit BufferPool(const std::string& filename, size_t numFrames = 16,
                        size_t pageSize = 4096);
    ~BufferPool();

    bool open();
    // Runtime readers must not create an empty relation when its file is lost.
    bool openExisting();
    // close(false) discards cached frames without another writeback attempt.
    // PageAllocator uses it after its marker-protected flush succeeds or
    // fails, so close cannot bypass that recovery protocol.
    void close(bool flushPages = true);
    bool isOpen() const { return fd_ >= 0; }
    // True when the open main/TDE descriptors still name the same filesystem
    // objects as their paths. Atomic index replacement changes the inode even
    // though the pathname is unchanged.
    bool refersToCurrentFiles() const;
    // Under one pool lock, prove that the replaced regular main is idle:
    // no dirty frames, ordinary/orphan pins or in-flight loads. A closed or
    // missing main and sidecar-only mismatch never satisfy this predicate.
    bool hasCleanRetiredMainFile(const std::string& currentFilename = "") const;

    // Install (or clear) the on-load page validator. Must be called before
    // open() or with no pages cached. Returning false rejects the page: the
    // load fails and nothing is inserted into the cache.
    void setPageValidator(std::function<bool(uint32_t, const char*)> validator) {
        pageValidator_ = std::move(validator);
    }

    // Install the durability boundary that must succeed before any cached
    // page writeback (clock eviction, flush, flushPage, or close). Heap
    // owners use this to enforce WAL-before-data regardless of which path
    // initiates persistence.
    void setWritebackBarrier(std::function<bool()> barrier) {
        std::lock_guard<std::mutex> lock(mutex_);
        writebackBarrier_ = std::move(barrier);
    }

    // Designate a structural page that must be written only after every
    // other dirty frame has reached stable storage.  PageAllocator uses page
    // zero here: publishing numPages before a newly appended heap page would
    // leave a relation that cannot be opened after a crash.  A dirty
    // write-last page is also kept out of ordinary clock eviction.
    void setWriteLastPage(std::optional<uint32_t> pageId) {
        std::lock_guard<std::mutex> lock(mutex_);
        writeLastPage_ = pageId;
    }

    // Invalidate cached page(s) so the next fetchPage reads from disk.
    void invalidatePage(uint32_t pageId);
    void invalidateAll();

    // Read a page. Returns pointer to cached page data.
    // The page is pinned until unpinPage is called.
    char* fetchPage(uint32_t pageId);

    // Mark a page as dirty (will be written back on eviction/flush).
    void markDirty(uint32_t pageId);

    // Per-page writeback used by COMMIT while the engine page lock is held.
    // A cached page is rewritten even if its dirty bit was cleared by an
    // overlapping bgwriter pass, so COMMIT repairs any earlier torn copy.
    bool flushPage(uint32_t pageId);

    // Unpin a page (allow eviction when pinCount reaches 0).
    void unpinPage(uint32_t pageId);

    // Write all dirty pages to disk and fsync.  Dirty frames remain dirty when
    // a write or fsync fails so callers can retry instead of losing the only
    // in-memory copy of the page.
    bool flush();

    // Background writeback: do nothing when the pool is clean or any dirty
    // frame is pinned. Once eligible frames are frozen under mutex_, invoke
    // the caller's WAL barrier and only then write them. Returning true can
    // therefore mean either "flushed" or "safely deferred".
    bool flushDirtyUnpinned(const std::function<bool()>& walBarrier);

    // Whether a cached page currently carries unwritten changes.
    bool isPageDirty(uint32_t pageId) const;

    // Stats
    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    double hitRate() const {
        size_t total = hits_ + misses_;
        return total == 0 ? 0.0 : 100.0 * static_cast<double>(hits_) / static_cast<double>(total);
    }
    void resetStats() { hits_ = 0; misses_ = 0; }

    // Frame info for pg_buffercache
    struct FrameInfo {
        uint32_t pageId;
        bool dirty;
        int pinCount;
        uint8_t usageCount;
        bool valid;
    };
    std::vector<FrameInfo> getFrameInfo() const;

private:
    bool openInternal(bool createIfMissing);
    struct Frame {
        uint32_t pageId = static_cast<uint32_t>(-1);
        bool dirty = false;
        int pinCount = 0;
        uint8_t usageCount = 0;
        std::vector<char> data;
    };

    std::string filename_;
    int fd_ = -1;
    // TDE sidecar (<filename>.tde): 48 bytes per page (nonce || MAC).
    // Zero-initialized records mark plaintext pages.
    int tdeFd_ = -1;
    size_t numFrames_;
    size_t pageSize_;
    std::vector<Frame> frames_;
    std::unordered_map<uint32_t, size_t> pageMap_; // pageId -> frame index
    size_t hits_ = 0;
    size_t misses_ = 0;

    // Runs once per disk load; cache hits skip it.
    std::function<bool(uint32_t, const char*)> pageValidator_;

    // Optional structural page whose contents publish the preceding data
    // pages.  It is flushed last and cannot be evicted while dirty.
    std::optional<uint32_t> writeLastPage_;

    // Called under mutex_ immediately before cached pages are written.
    // Failure leaves the dirty frame(s) and page-map entries intact.
    std::function<bool()> writebackBarrier_;

    // Clock sweep hand
    size_t clockHand_ = 0;

    mutable std::mutex mutex_;

    // Pages currently being loaded from disk. The loader released mutex_
    // for the pread, so a concurrent fetch of the same page must not start
    // a second read; it waits for the in-flight one instead.
    std::unordered_map<uint32_t, size_t> loadingPages_;   // pageId -> frame idx
    std::unordered_map<uint32_t, std::vector<std::condition_variable*>> loadWaiters_;

    // Pages invalidated while readers were still pinned: pageId -> (remaining
    // pins, frame idx). The frame carries kOrphanedPage until the last unpin.
    // INVARIANT (mutual exclusion): for any pageId, at most one of
    // pageMap_[pageId] / orphanedPins_[pageId] / loadingPages_[pageId] holds
    // an entry at a time. unpinPage routes by that rule, so a pin is always
    // released to the frame it was taken on.
    std::unordered_map<uint32_t, std::pair<size_t, size_t>> orphanedPins_;
    std::vector<std::condition_variable*> orphanWaiters_;  // fetches blocked on drain

    bool readFromDisk(uint32_t pageId, char* buf, bool* fullPageRead = nullptr);
    bool writeToDisk(uint32_t pageId, const char* buf);
    bool readTdeRecord(uint32_t pageId, uint8_t record[PageCrypto::kRecordSize]);
    bool writeTdeRecord(uint32_t pageId,
                        const uint8_t record[PageCrypto::kRecordSize]);
    // Recreate the frame array after close() releases its memory. mutex_ is
    // held by open(); construction calls this before the object is shared.
    void initializeFramesUnlocked();
    bool flushUnlocked(bool useInstalledBarrier = true);
    std::optional<size_t> evictFrame();
    // Fast path: pin a cached page; waits on an in-flight load of it.
    // Returns nullptr on miss. mutex_ is NOT held on return.
    char* tryPinCached(uint32_t pageId);
    // Complete a load into frame idx (validator + publish). mutex_ held.
    bool publishLoadedPage(uint32_t pageId, size_t idx, bool fullPageRead);
    // Abort the in-flight load of pageId and wake all waiters. mutex_ held.
    void failLoad(uint32_t pageId, size_t idx);
    // Retire an invalidated frame whose readers have all unpinned. mutex_ held.
    void reclaimOrphanedFrame(size_t idx);
    // Wake fetches waiting for an orphan of pageId to drain. mutex_ held.
    void notifyOrphanDrained(uint32_t pageId);
};

} // namespace dbms
