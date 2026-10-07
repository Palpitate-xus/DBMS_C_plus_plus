#include "VisibilityMap.h"
#include "DerivedMapFile.h"

namespace dbms {

VisibilityMap::VisibilityMap(const std::string& filename) : filename_(filename) {}

VisibilityMap::~VisibilityMap() {
    flush();
    close();
}

bool VisibilityMap::open() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) return derived_map_file::owned(fd_, filename_);
    fd_ = derived_map_file::open(filename_, cache_);
    if (fd_ >= 0) dirty_ = false;
    return fd_ >= 0;
}

void VisibilityMap::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        if (dirty_) (void)writeToDiskLocked();
        ::close(fd_);
        fd_ = -1;
    }
    cache_.clear();
    dirty_ = false;
}

bool VisibilityMap::isOpen() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return fd_ >= 0;
}

bool VisibilityMap::writeToDiskLocked() const {
    return derived_map_file::write(fd_, filename_, cache_);
}

void VisibilityMap::ensureSizeLocked(uint32_t pageId) {
    size_t need = byteIndex(pageId) + 1;
    if (need > cache_.size()) {
        cache_.resize(need, 0);
        dirty_ = true;
    }
}

bool VisibilityMap::isAllVisible(uint32_t pageId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t bi = byteIndex(pageId);
    if (bi >= cache_.size()) return false;
    return (cache_[bi] & bitMask(pageId)) != 0;
}

void VisibilityMap::setAllVisible(uint32_t pageId, bool visible) {
    std::lock_guard<std::mutex> lock(mutex_);
    ensureSizeLocked(pageId);
    size_t bi = byteIndex(pageId);
    uint8_t mask = bitMask(pageId);
    uint8_t old = cache_[bi];
    if (visible) {
        cache_[bi] = old | mask;
    } else {
        cache_[bi] = old & ~mask;
    }
    if (cache_[bi] != old) {
        dirty_ = true;
    }
}

uint32_t VisibilityMap::capacity() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<uint32_t>(cache_.size() * 8);
}

void VisibilityMap::flush() {
    (void)flushChecked();
}

bool VisibilityMap::flushChecked() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (dirty_) {
        if (!writeToDiskLocked()) return false;
        dirty_ = false;
    }
    return derived_map_file::matches(fd_, filename_, cache_);
}

bool VisibilityMap::quiescentForSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !dirty_ && derived_map_file::matches(fd_, filename_, cache_);
}

} // namespace dbms
