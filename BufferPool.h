#pragma once

#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

namespace dbms {

constexpr size_t BP_POOL_PAGE_SIZE = 4096;

// Simple page buffer pool with LRU eviction
class BufferPool {
public:
    explicit BufferPool(const std::string& filename, size_t numFrames = 16);
    ~BufferPool();

    bool open();
    void close();
    bool isOpen() const { return fd_ >= 0; }

    // Read a page. Returns pointer to cached page data (4096 bytes).
    // The page is pinned until unpinPage is called.
    char* fetchPage(uint32_t pageId);

    // Mark a page as dirty (will be written back on eviction/flush).
    void markDirty(uint32_t pageId);

    // Unpin a page (allow eviction).
    void unpinPage(uint32_t pageId);

    // Write all dirty pages to disk.
    void flush();

    // Stats
    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }

private:
    struct Frame {
        uint32_t pageId = static_cast<uint32_t>(-1);
        bool dirty = false;
        int pinCount = 0;
        std::vector<char> data;
        std::list<size_t>::iterator lruIter;
    };

    std::string filename_;
    int fd_ = -1;
    size_t numFrames_;
    std::vector<Frame> frames_;
    std::unordered_map<uint32_t, size_t> pageMap_; // pageId -> frame index
    std::list<size_t> lruList_; // frame indices, front = LRU, back = MRU
    size_t hits_ = 0;
    size_t misses_ = 0;

    bool readFromDisk(uint32_t pageId, char* buf);
    bool writeToDisk(uint32_t pageId, const char* buf);
    size_t evictFrame();
};

} // namespace dbms
