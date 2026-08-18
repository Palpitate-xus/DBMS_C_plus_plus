#include "BufferPool.h"

#include <unistd.h>

namespace dbms {

BufferPool::BufferPool(const std::string& filename, size_t numFrames, size_t pageSize)
    : filename_(filename), numFrames_(numFrames == 0 ? 1 : numFrames),
      pageSize_(pageSize), clockHand_(0) {
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
        // Flush under the same lock as close.  Destructors cannot propagate
        // the error, but flushUnlocked still preserves dirty flags until the
        // last write attempt rather than falsely declaring pages clean.
        flushUnlocked();
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

bool BufferPool::readFromDisk(uint32_t pageId, char* buf, bool* fullPageRead) {
    if (fd_ < 0) return false;
    off_t offset = static_cast<off_t>(pageId) * pageSize_;
    ssize_t n = ::pread(fd_, buf, pageSize_, offset);
    if (n < 0) return false;
    if (n < static_cast<ssize_t>(pageSize_)) {
        // New page: zero-fill remainder
        std::memset(buf + n, 0, pageSize_ - n);
        if (fullPageRead) *fullPageRead = false;
    } else if (fullPageRead) {
        *fullPageRead = true;
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

std::optional<size_t> BufferPool::evictFrame() {
    // Clock sweep: scan frames in circular order.
    //   pinCount  > 0  -> pinned, skip
    //   usageCount > 0 -> recently used, decrement and skip
    //   otherwise      -> evict this frame
    while (true) {
        bool usageChanged = false;
        const size_t start = clockHand_;
        do {
            size_t idx = clockHand_;
            clockHand_ = (clockHand_ + 1) % numFrames_;

            Frame& f = frames_[idx];
            if (f.pinCount > 0) {
                continue; // pinned pages are never evicted
            }
            if (f.usageCount > 0) {
                f.usageCount--;
                usageChanged = true;
                continue; // second chance
            }

            // A dirty page must be written successfully before its only
            // cached copy is discarded. Leave the frame and mapping intact
            // on failure; checkpoint/fsync provides durable ordering.
            if (f.dirty && !writeToDisk(f.pageId, f.data.data())) {
                return std::nullopt;
            }
            pageMap_.erase(f.pageId);
            f.pageId = static_cast<uint32_t>(-1);
            f.dirty = false;
            f.pinCount = 0;
            f.usageCount = 0;
            return idx;
        } while (clockHand_ != start);

        // A second sweep is needed after usage counts receive their second
        // chance.  If nothing changed, every frame is pinned.
        if (!usageChanged) return std::nullopt;
    }
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
    size_t idx = static_cast<size_t>(-1);
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
            auto evicted = evictFrame();
            if (!evicted) return nullptr;
            idx = *evicted;
        }
    } else {
        auto evicted = evictFrame();
        if (!evicted) return nullptr;
        idx = *evicted;
    }

    Frame& f = frames_[idx];
    f.pageId = pageId;
    f.dirty = false;
    f.pinCount = 1;
    f.usageCount = 3;
    bool fullPageRead = false;
    if (!readFromDisk(pageId, f.data.data(), &fullPageRead)) {
        f.pageId = static_cast<uint32_t>(-1);
        f.pinCount = 0;
        f.usageCount = 0;
        return nullptr;
    }
    // A short read means the page was never written; the caller initializes
    // it (e.g. allocPage's newPage.init). Only fully written pages carry a
    // checksum worth verifying.
    if (fullPageRead && pageValidator_ && !pageValidator_(pageId, f.data.data())) {
        // Corrupt on disk: do not cache or expose it.
        f.pageId = static_cast<uint32_t>(-1);
        f.pinCount = 0;
        f.usageCount = 0;
        return nullptr;
    }
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

bool BufferPool::flushUnlocked() {
    if (fd_ < 0) return false;
    bool ok = true;
    std::vector<size_t> writtenFrames;
    for (size_t index = 0; index < frames_.size(); ++index) {
        auto& frame = frames_[index];
        if (frame.dirty && frame.pageId != static_cast<uint32_t>(-1)) {
            if (writeToDisk(frame.pageId, frame.data.data())) {
                writtenFrames.push_back(index);
            } else {
                ok = false;
            }
        }
    }
    if (::fsync(fd_) != 0) ok = false;
    if (!ok) return false;
    for (size_t index : writtenFrames) {
        frames_[index].dirty = false;
    }
    return ok;
}

bool BufferPool::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    return flushUnlocked();
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
