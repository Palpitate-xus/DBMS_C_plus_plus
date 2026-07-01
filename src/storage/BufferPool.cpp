#include "BufferPool.h"

#include <unistd.h>

namespace dbms {

BufferPool::BufferPool(const std::string& filename, size_t numFrames, size_t pageSize)
    : filename_(filename), numFrames_(numFrames), pageSize_(pageSize), clockHand_(0) {
    frames_.resize(numFrames_);
    for (size_t i = 0; i < numFrames_; ++i) {
        frames_[i].pageId = static_cast<uint32_t>(-1);
        frames_[i].dirty = false;
        frames_[i].pinCount = 0;
        frames_[i].usageCount = 0;
        frames_[i].data.resize(pageSize_);
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
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        // flush under lock
        for (auto& frame : frames_) {
            if (frame.dirty && frame.pageId != static_cast<uint32_t>(-1)) {
                writeToDisk(frame.pageId, frame.data.data());
                frame.dirty = false;
            }
        }
        ::fsync(fd_);
        ::close(fd_);
        fd_ = -1;
    }
    frames_.clear();
    pageMap_.clear();
}

void BufferPool::invalidatePage(uint32_t pageId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = pageMap_.find(pageId);
    if (it != pageMap_.end()) {
        size_t idx = it->second;
        frames_[idx].pageId = static_cast<uint32_t>(-1);
        frames_[idx].dirty = false;
        frames_[idx].pinCount = 0;
        frames_[idx].usageCount = 0;
        pageMap_.erase(it);
    }
}

void BufferPool::invalidateAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& frame : frames_) {
        frame.pageId = static_cast<uint32_t>(-1);
        frame.dirty = false;
        frame.pinCount = 0;
        frame.usageCount = 0;
    }
    pageMap_.clear();
}

bool BufferPool::readFromDisk(uint32_t pageId, char* buf) {
    if (fd_ < 0) return false;
    off_t offset = static_cast<off_t>(pageId) * pageSize_;
    ssize_t n = ::pread(fd_, buf, pageSize_, offset);
    if (n < static_cast<ssize_t>(pageSize_)) {
        // New page: zero-fill remainder
        if (n < 0) n = 0;
        std::memset(buf + n, 0, pageSize_ - n);
    }
    return true;
}

bool BufferPool::writeToDisk(uint32_t pageId, const char* buf) {
    if (fd_ < 0) {
        return false;
    }
    off_t offset = static_cast<off_t>(pageId) * pageSize_;
    ssize_t n = ::pwrite(fd_, buf, pageSize_, offset);
    return n == static_cast<ssize_t>(pageSize_);
}

size_t BufferPool::evictFrame() {
    // Clock sweep: scan frames in circular order.
    //   pinCount  > 0  -> pinned, skip
    //   usageCount > 0 -> recently used, decrement and skip
    //   otherwise      -> evict this frame
    const size_t start = clockHand_;
    do {
        size_t idx = clockHand_;
        clockHand_ = (clockHand_ + 1) % numFrames_;

        Frame& f = frames_[idx];
        if (f.pinCount > 0) {
            continue; // cannot evict pinned frame
        }
        if (f.usageCount > 0) {
            f.usageCount--;
            continue; // second chance
        }

        // Evict
        if (f.dirty) {
            writeToDisk(f.pageId, f.data.data());
        }
        pageMap_.erase(f.pageId);
        f.pageId = static_cast<uint32_t>(-1);
        f.dirty = false;
        f.pinCount = 0;
        f.usageCount = 0;
        return idx;
    } while (clockHand_ != start);

    // All frames pinned or recently used: force evict the current hand.
    // This should be extremely rare.
    size_t idx = clockHand_;
    clockHand_ = (clockHand_ + 1) % numFrames_;
    Frame& f = frames_[idx];
    if (f.dirty) {
        writeToDisk(f.pageId, f.data.data());
    }
    pageMap_.erase(f.pageId);
    f.pageId = static_cast<uint32_t>(-1);
    f.dirty = false;
    f.pinCount = 0;
    f.usageCount = 0;
    return idx;
}

char* BufferPool::fetchPage(uint32_t pageId) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = pageMap_.find(pageId);
    if (it != pageMap_.end()) {
        size_t idx = it->second;
        frames_[idx].pinCount++;
        frames_[idx].usageCount = 3; // boost usage count on hit
        ++hits_;
        return frames_[idx].data.data();
    }

    ++misses_;
    // Need to load from disk
    size_t idx;
    if (pageMap_.size() < numFrames_) {
        // Find a free frame
        idx = static_cast<size_t>(-1);
        for (size_t i = 0; i < numFrames_; ++i) {
            if (frames_[i].pageId == static_cast<uint32_t>(-1)) {
                idx = i;
                break;
            }
        }
        if (idx == static_cast<size_t>(-1)) {
            idx = evictFrame(); // should not happen, but be safe
        }
    } else {
        idx = evictFrame();
    }

    Frame& f = frames_[idx];
    f.pageId = pageId;
    f.dirty = false;
    f.pinCount = 1;
    f.usageCount = 3;
    readFromDisk(pageId, f.data.data());
    pageMap_[pageId] = idx;
    return f.data.data();
}

void BufferPool::markDirty(uint32_t pageId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = pageMap_.find(pageId);
    if (it != pageMap_.end()) {
        frames_[it->second].dirty = true;
    }
}

void BufferPool::unpinPage(uint32_t pageId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = pageMap_.find(pageId);
    if (it != pageMap_.end()) {
        if (frames_[it->second].pinCount > 0) {
            frames_[it->second].pinCount--;
        }
    }
}

void BufferPool::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& frame : frames_) {
        if (frame.dirty && frame.pageId != static_cast<uint32_t>(-1)) {
            writeToDisk(frame.pageId, frame.data.data());
            frame.dirty = false;
        }
    }
    if (fd_ >= 0) {
        ::fsync(fd_);
    }
}

std::vector<BufferPool::FrameInfo> BufferPool::getFrameInfo() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<FrameInfo> result;
    for (const auto& frame : frames_) {
        if (frame.pageId != static_cast<uint32_t>(-1)) {
            result.push_back({frame.pageId, frame.dirty, frame.pinCount,
                              frame.usageCount, true});
        }
    }
    return result;
}

} // namespace dbms
