#include "VisibilityMap.h"
#include "DerivedMapFile.h"

namespace dbms {

VisibilityMap::VisibilityMap(const std::string& filename) : filename_(filename) {}
VisibilityMap::~VisibilityMap() {
    flush(); close();
    if (fd_ >= 0) ::close(fd_);
}

bool VisibilityMap::open() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) return derived_map_file::owned(fd_, filename_);
    std::vector<uint8_t> initial;
    fd_ = derived_map_file::open(filename_, initial);
    if (fd_ < 0) return false;
    cache_ = derived_map_file::sharedCache(fd_, filename_, 1, std::move(initial));
    if (!cache_) { ::close(fd_); fd_ = -1; return false; }
    return true;
}

void VisibilityMap::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        std::lock_guard<std::mutex> sharedLock(cache_->mutex);
        if (cache_->dirty && !derived_map_file::publish(fd_, filename_, *cache_, 0) &&
            !derived_map_file::retainPending(cache_, fd_)) return;
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }
    cache_.reset();
}

bool VisibilityMap::isOpen() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return fd_ >= 0;
}

bool VisibilityMap::isAllVisible(uint32_t pageId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return false;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    const size_t index = byteIndex(pageId);
    if (!derived_map_file::refresh(fd_, filename_, *cache_, 0) ||
        index >= cache_->bytes.size()) return false;
    return (cache_->bytes[index] & bitMask(pageId)) != 0;
}

void VisibilityMap::setAllVisible(uint32_t pageId, bool visible) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    if (!derived_map_file::owned(fd_, filename_)) return;
    const bool refreshed = derived_map_file::refresh(fd_, filename_, *cache_, 0);
    const size_t index = byteIndex(pageId);
    const bool grew = index >= cache_->bytes.size();
    if (grew) cache_->bytes.resize(index + 1, 0);
    const uint8_t mask = bitMask(pageId);
    const uint8_t old = cache_->bytes[index];
    cache_->bytes[index] = visible ? static_cast<uint8_t>(old | mask)
                                  : static_cast<uint8_t>(old & ~mask);
    if (grew || !refreshed || cache_->bytes[index] != old) {
        cache_->pendingMasks[index] |= mask;
        cache_->dirty = true;
    }
}

uint32_t VisibilityMap::capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return 0;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    if (!derived_map_file::refresh(fd_, filename_, *cache_, 0)) return 0;
    return static_cast<uint32_t>(cache_->bytes.size() * 8);
}

void VisibilityMap::flush() { (void)flushChecked(); }

bool VisibilityMap::flushChecked() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return false;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    return derived_map_file::publish(fd_, filename_, *cache_, 0);
}

bool VisibilityMap::quiescentForSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return false;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    return !cache_->dirty && derived_map_file::matches(fd_, filename_, cache_->bytes);
}

bool VisibilityMap::refreshPublishedChecked() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return false;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    return derived_map_file::refreshPublished(fd_, filename_, *cache_);
}

} // namespace dbms
