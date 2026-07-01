#pragma once

#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace dbms {

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

class BufferPool {
public:
    explicit BufferPool(const std::string& filename, size_t numFrames = 16,
                        size_t pageSize = 4096);
    ~BufferPool();

    bool open();
    void close();
    bool isOpen() const { return fd_ >= 0; }

    // Invalidate cached page(s) so the next fetchPage reads from disk.
    void invalidatePage(uint32_t pageId);
    void invalidateAll();

    // Read a page. Returns pointer to cached page data.
    // The page is pinned until unpinPage is called.
    char* fetchPage(uint32_t pageId);

    // Mark a page as dirty (will be written back on eviction/flush).
    void markDirty(uint32_t pageId);

    // Unpin a page (allow eviction when pinCount reaches 0).
    void unpinPage(uint32_t pageId);

    // Write all dirty pages to disk and fsync.
    void flush();

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
    struct Frame {
        uint32_t pageId = static_cast<uint32_t>(-1);
        bool dirty = false;
        int pinCount = 0;
        uint8_t usageCount = 0;
        std::vector<char> data;
    };

    std::string filename_;
    int fd_ = -1;
    size_t numFrames_;
    size_t pageSize_;
    std::vector<Frame> frames_;
    std::unordered_map<uint32_t, size_t> pageMap_; // pageId -> frame index
    size_t hits_ = 0;
    size_t misses_ = 0;

    // Clock sweep hand
    size_t clockHand_ = 0;

    mutable std::mutex mutex_;

    bool readFromDisk(uint32_t pageId, char* buf);
    bool writeToDisk(uint32_t pageId, const char* buf);
    size_t evictFrame();
};

} // namespace dbms
