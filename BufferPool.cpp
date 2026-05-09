#include "BufferPool.h"

#include <iostream>
#include <unistd.h>

namespace dbms {

BufferPool::BufferPool(const std::string& filename, size_t numFrames)
    : filename_(filename), numFrames_(numFrames) {
    frames_.resize(numFrames_);
    for (size_t i = 0; i < numFrames_; ++i) {
        frames_[i].pageId = static_cast<uint32_t>(-1);
        frames_[i].dirty = false;
        frames_[i].pinCount = 0;
        frames_[i].data.resize(BP_POOL_PAGE_SIZE);
        frames_[i].lruIter = lruList_.end();
    }
}

BufferPool::~BufferPool() {
    flush();
    close();
}

bool BufferPool::open() {
    if (fd_ >= 0) return true;
    fd_ = ::open(filename_.c_str(), O_RDWR | O_CREAT, 0644);
    return fd_ >= 0;
}

void BufferPool::close() {
    if (fd_ >= 0) {
        flush();
        ::close(fd_);
        fd_ = -1;
    }
    frames_.clear();
    pageMap_.clear();
    lruList_.clear();
}

bool BufferPool::readFromDisk(uint32_t pageId, char* buf) {
    if (fd_ < 0) return false;
    off_t offset = static_cast<off_t>(pageId) * BP_POOL_PAGE_SIZE;
    ssize_t n = ::pread(fd_, buf, BP_POOL_PAGE_SIZE, offset);
    if (n < static_cast<ssize_t>(BP_POOL_PAGE_SIZE)) {
        // New page: zero-fill remainder
        if (n < 0) n = 0;
        std::memset(buf + n, 0, BP_POOL_PAGE_SIZE - n);
    }
    return true;
}

bool BufferPool::writeToDisk(uint32_t pageId, const char* buf) {
    if (fd_ < 0) return false;
    off_t offset = static_cast<off_t>(pageId) * BP_POOL_PAGE_SIZE;
    ssize_t n = ::pwrite(fd_, buf, BP_POOL_PAGE_SIZE, offset);
    return n == static_cast<ssize_t>(BP_POOL_PAGE_SIZE);
}

size_t BufferPool::evictFrame() {
    // Find LRU unpinned frame
    for (auto it = lruList_.begin(); it != lruList_.end(); ++it) {
        size_t idx = *it;
        if (frames_[idx].pinCount == 0) {
            if (frames_[idx].dirty) {
                writeToDisk(frames_[idx].pageId, frames_[idx].data.data());
            }
            pageMap_.erase(frames_[idx].pageId);
            frames_[idx].pageId = static_cast<uint32_t>(-1);
            frames_[idx].dirty = false;
            frames_[idx].pinCount = 0;
            lruList_.erase(it);
            return idx;
        }
    }
    // All frames pinned: force evict the true LRU (shouldn't happen often)
    size_t idx = lruList_.front();
    lruList_.pop_front();
    if (frames_[idx].dirty) {
        writeToDisk(frames_[idx].pageId, frames_[idx].data.data());
    }
    pageMap_.erase(frames_[idx].pageId);
    frames_[idx].pageId = static_cast<uint32_t>(-1);
    frames_[idx].dirty = false;
    frames_[idx].pinCount = 0;
    return idx;
}

char* BufferPool::fetchPage(uint32_t pageId) {
    auto it = pageMap_.find(pageId);
    if (it != pageMap_.end()) {
        size_t idx = it->second;
        frames_[idx].pinCount++;
        // Move to MRU position
        lruList_.erase(frames_[idx].lruIter);
        lruList_.push_back(idx);
        frames_[idx].lruIter = std::prev(lruList_.end());
        ++hits_;
        return frames_[idx].data.data();
    }

    ++misses_;
    // Need to load from disk
    size_t idx;
    if (pageMap_.size() < numFrames_) {
        // Find a free frame
        for (size_t i = 0; i < numFrames_; ++i) {
            if (frames_[i].pageId == static_cast<uint32_t>(-1)) {
                idx = i;
                break;
            }
        }
    } else {
        idx = evictFrame();
    }

    frames_[idx].pageId = pageId;
    frames_[idx].dirty = false;
    frames_[idx].pinCount = 1;
    readFromDisk(pageId, frames_[idx].data.data());
    pageMap_[pageId] = idx;
    lruList_.push_back(idx);
    frames_[idx].lruIter = std::prev(lruList_.end());
    return frames_[idx].data.data();
}

void BufferPool::markDirty(uint32_t pageId) {
    auto it = pageMap_.find(pageId);
    if (it != pageMap_.end()) {
        frames_[it->second].dirty = true;
    }
}

void BufferPool::unpinPage(uint32_t pageId) {
    auto it = pageMap_.find(pageId);
    if (it != pageMap_.end()) {
        if (frames_[it->second].pinCount > 0) {
            frames_[it->second].pinCount--;
        }
    }
}

void BufferPool::flush() {
    for (auto& frame : frames_) {
        if (frame.dirty && frame.pageId != static_cast<uint32_t>(-1)) {
            writeToDisk(frame.pageId, frame.data.data());
            frame.dirty = false;
        }
    }
}

} // namespace dbms
