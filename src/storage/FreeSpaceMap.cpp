#include "FreeSpaceMap.h"
#include "DerivedMapFile.h"

namespace dbms {

FreeSpaceMap::FreeSpaceMap(const std::string& filename) : filename_(filename) {}
FreeSpaceMap::~FreeSpaceMap() { flush(); close(); }

bool FreeSpaceMap::open() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) return derived_map_file::owned(fd_, filename_);
    std::vector<uint8_t> initial;
    fd_ = derived_map_file::open(filename_, initial);
    if (fd_ < 0) return false;
    cache_ = derived_map_file::sharedCache(fd_, filename_, 0, std::move(initial));
    if (!cache_) { ::close(fd_); fd_ = -1; return false; }
    return true;
}

void FreeSpaceMap::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        std::lock_guard<std::mutex> sharedLock(cache_->mutex);
        if (cache_->dirty) (void)derived_map_file::publish(fd_, filename_, *cache_, 255);
        ::close(fd_);
        fd_ = -1;
    }
    cache_.reset();
}

bool FreeSpaceMap::isOpen() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return fd_ >= 0;
}

uint8_t FreeSpaceMap::getFreePercent(uint32_t pageId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return 255;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    if (!derived_map_file::refresh(fd_, filename_, *cache_, 255) ||
        pageId >= cache_->bytes.size()) return 255;
    return cache_->bytes[pageId];
}

void FreeSpaceMap::setFreePercent(uint32_t pageId, uint8_t percent) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    if (!derived_map_file::owned(fd_, filename_)) return;
    const bool refreshed = derived_map_file::refresh(fd_, filename_, *cache_, 255);
    const bool grew = pageId >= cache_->bytes.size();
    if (grew) cache_->bytes.resize(static_cast<size_t>(pageId) + 1, 255);
    if (grew || !refreshed || cache_->bytes[pageId] != percent) {
        cache_->bytes[pageId] = percent;
        cache_->pendingMasks[pageId] = 255;
        cache_->dirty = true;
    }
}

uint32_t FreeSpaceMap::findPage(uint8_t minPercent, uint32_t startPage) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return 0;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    if (!derived_map_file::refresh(fd_, filename_, *cache_, 255)) return 0;
    for (size_t i = startPage; i < cache_->bytes.size(); ++i)
        if (cache_->bytes[i] >= minPercent && cache_->bytes[i] != 255)
            return static_cast<uint32_t>(i);
    return 0;
}

uint32_t FreeSpaceMap::numPages() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return 0;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    if (!derived_map_file::refresh(fd_, filename_, *cache_, 255)) return 0;
    return static_cast<uint32_t>(cache_->bytes.size());
}

void FreeSpaceMap::flush() { (void)flushChecked(); }

bool FreeSpaceMap::flushChecked() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return false;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    return derived_map_file::publish(fd_, filename_, *cache_, 255);
}

bool FreeSpaceMap::quiescentForSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!cache_) return false;
    std::lock_guard<std::mutex> sharedLock(cache_->mutex);
    return !cache_->dirty && derived_map_file::matches(fd_, filename_, cache_->bytes);
}

} // namespace dbms
