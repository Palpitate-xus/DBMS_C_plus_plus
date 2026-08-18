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
    std::unique_lock<std::mutex> lock(mutex_);
    // Drain in-flight loads: a loader still holds a Frame& reference and a
    // spare-buffer copy pending memcpy into a frame that close() clears.
    std::condition_variable cv;
    while (!loadingPages_.empty()) {
        const uint32_t pid = loadingPages_.begin()->first;
        loadWaiters_[pid].push_back(&cv);
        cv.wait(lock);
    }
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
    std::unique_lock<std::mutex> lock(mutex_);
    // If a peer thread is mid-load for this page, wait it out: publishing
    // after our erase would resurrect the invalidated page, and resetting
    // the frame underneath the loader would recycle its buffer.
    std::condition_variable cv;
    while (loadingPages_.count(pageId)) {
        loadWaiters_[pageId].push_back(&cv);
        cv.wait(lock);
    }
    auto it = pageMap_.find(pageId);
    if (it != pageMap_.end()) {
        size_t idx = it->second;
        pageMap_.erase(it);
        Frame& f = frames_[idx];
        f.dirty = false;
        f.usageCount = 0;
        // Pre-existing note: engine-level locks serialize invalidate vs.
        // fetch in all current callers; a reader that pinned this frame
        // concurrently would observe the frame being recycled. The orphaned
        // pin-count bookkeeping needed to close that window fully requires
        // pin handles in the unpin API and is left as future work.
        f.pageId = static_cast<uint32_t>(-1);
        f.pinCount = 0;
    }
}

void BufferPool::invalidateAll() {
    std::unique_lock<std::mutex> lock(mutex_);
    // Drain every in-flight load before dropping the frames.
    std::condition_variable cv;
    while (!loadingPages_.empty()) {
        // Register on one in-flight page; notified loads erase their waiter
        // lists, so re-register until the map drains.
        const uint32_t pid = loadingPages_.begin()->first;
        loadWaiters_[pid].push_back(&cv);
        cv.wait(lock);
    }
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
                continue; // pinned pages and orphaned buffers are never evicted
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

char* BufferPool::tryPinCached(uint32_t pageId) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
        auto lit = loadingPages_.find(pageId);
        if (lit != loadingPages_.end()) {
            // Another thread is loading this page right now: wait for it.
            // Stack cv is safe: we only return after being notified AND
            // erasing ourselves is unnecessary — publishLoad/failLoad erase
            // the whole waiter list before notifying.
            std::condition_variable cv;
            loadWaiters_[pageId].push_back(&cv);
            cv.wait(lock);
            continue;  // re-check cache (and loadingPages_) after wakeup
        }
        auto it = pageMap_.find(pageId);
        if (it != pageMap_.end()) {
            size_t idx = it->second;
            frames_[idx].pinCount++;
            frames_[idx].usageCount = 3; // boost usage count on hit
            ++hits_;
            return frames_[idx].data.data();
        }
        return nullptr;
    }
}

void BufferPool::failLoad(uint32_t pageId, size_t idx) {
    Frame& f = frames_[idx];
    f.pageId = static_cast<uint32_t>(-1);
    f.dirty = false;
    f.pinCount = 0;
    f.usageCount = 0;
    loadingPages_.erase(pageId);
    auto w = loadWaiters_.find(pageId);
    if (w != loadWaiters_.end()) {
        for (auto* cv : w->second) cv->notify_all();
        loadWaiters_.erase(w);
    }
}

bool BufferPool::publishLoadedPage(uint32_t pageId, size_t idx, bool fullPageRead) {
    Frame& f = frames_[idx];
    // A short read means the page was never written; the caller initializes
    // it (e.g. allocPage's newPage.init). Only fully written pages carry a
    // checksum worth verifying.
    if (fullPageRead && pageValidator_ && !pageValidator_(pageId, f.data.data())) {
        // Corrupt on disk: do not cache or expose it.
        failLoad(pageId, idx);
        return false;
    }
    pageMap_[pageId] = idx;
    loadingPages_.erase(pageId);
    auto w = loadWaiters_.find(pageId);
    if (w != loadWaiters_.end()) {
        for (auto* cv : w->second) cv->notify_all();
        loadWaiters_.erase(w);
    }
    return true;
}

char* BufferPool::fetchPage(uint32_t pageId) {
    // Fast path: page already cached (or being loaded by a peer).
    if (char* cached = tryPinCached(pageId)) return cached;

    // Slow path: reserve a frame under the lock, then do the disk I/O with
    // the lock RELEASED so concurrent hits on other pages are not blocked
    // behind one pread.
    std::unique_lock<std::mutex> lock(mutex_);
    ++misses_;

    // Exactly one loader per page: if a peer is mid-load, wait for it and
    // use its result (two loaders of one page would race to publish two
    // frames for the same mapping).
    while (loadingPages_.count(pageId)) {
        std::condition_variable cv;
        loadWaiters_[pageId].push_back(&cv);
        cv.wait(lock);
        // The peer published (or failed). Try the cache first.
        auto it = pageMap_.find(pageId);
        if (it != pageMap_.end()) {
            Frame& pf = frames_[it->second];
            pf.pinCount++;
            pf.usageCount = 3;
            return pf.data.data();
        }
        // Peer failed: retry the load ourselves (loop re-checks loadingPages_).
    }

    // Select a victim frame. A frame mid-load is pinned by its loader, so
    // the sweep below (and evictFrame) already skip it.
    size_t idx = static_cast<size_t>(-1);
    for (size_t i = 0; i < numFrames_; ++i) {
        if (frames_[i].pageId == static_cast<uint32_t>(-1) &&
            frames_[i].pinCount == 0) {
            idx = i;
            break;
        }
    }
    if (idx == static_cast<size_t>(-1)) {
        auto evicted = evictFrame();
        if (!evicted) return nullptr;
        idx = *evicted;
    }

    Frame& f = frames_[idx];
    f.pageId = pageId;
    f.dirty = false;
    f.pinCount = 1;   // this thread owns the frame while loading
    f.usageCount = 3;
    loadingPages_[pageId] = idx;

    // Read into a PRIVATE scratch buffer WITHOUT the lock, then copy into
    // the reserved frame. pread on a cold file can take milliseconds;
    // holding mutex_ through it serialized every concurrent cache hit, and
    // a shared scratch buffer would race with a concurrent loader.
    std::vector<char> scratch(pageSize_);
    lock.unlock();
    bool fullPageRead = false;
    const bool readOk = readFromDisk(pageId, scratch.data(), &fullPageRead);
    lock.lock();
    if (!readOk) {
        failLoad(pageId, idx);
        return nullptr;
    }
    std::memcpy(f.data.data(), scratch.data(), pageSize_);
    // Another loader may have published this page while we held the lock
    // off. Only one frame may own the page; the loser retires its frame and
    // redirects to the winner so no mapping ever points at two buffers.
    {
        auto pit = pageMap_.find(pageId);
        if (pit != pageMap_.end() && pit->second != idx) {
            const size_t winner = pit->second;
            // Give our pin to the winner's frame.
            frames_[winner].pinCount++;
            frames_[winner].usageCount = 3;
            // Retire our frame (it was only ever ours; readers never saw it).
            f.pageId = static_cast<uint32_t>(-1);
            f.pinCount = 0;
            f.usageCount = 0;
            f.dirty = false;
            loadingPages_.erase(pageId);
            auto w = loadWaiters_.find(pageId);
            if (w != loadWaiters_.end()) {
                for (auto* cv : w->second) cv->notify_all();
                loadWaiters_.erase(w);
            }
            return frames_[winner].data.data();
        }
    }
    if (!publishLoadedPage(pageId, idx, fullPageRead)) return nullptr;
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
        Frame& f = frames_[it->second];
        if (f.pinCount > 0) f.pinCount--;
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
