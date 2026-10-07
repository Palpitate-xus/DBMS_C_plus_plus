#include "FreeSpaceMap.h"
#include "DerivedMapFile.h"

namespace dbms {

FreeSpaceMap::FreeSpaceMap(const std::string& filename) : filename_(filename) {}

FreeSpaceMap::~FreeSpaceMap() {
    flush();
    close();
}

bool FreeSpaceMap::open() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) return derived_map_file::owned(fd_, filename_);
    fd_ = derived_map_file::open(filename_, cache_);
    if (fd_ >= 0) {
        numPages_ = static_cast<uint32_t>(cache_.size());
        dirty_ = false;
    }
    return fd_ >= 0;
}

void FreeSpaceMap::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        if (dirty_) (void)writeToDiskLocked();
        ::close(fd_);
        fd_ = -1;
    }
    cache_.clear();
    numPages_ = 0;
    dirty_ = false;
}

bool FreeSpaceMap::isOpen() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return fd_ >= 0;
}

bool FreeSpaceMap::writeToDiskLocked() const {
    return derived_map_file::write(fd_, filename_, cache_);
}

void FreeSpaceMap::ensureSizeLocked(uint32_t pageId) {
    if (pageId >= cache_.size()) {
        cache_.resize(pageId + 1, 255);
        numPages_ = static_cast<uint32_t>(cache_.size());
        dirty_ = true;
    }
}

uint8_t FreeSpaceMap::getFreePercent(uint32_t pageId) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pageId >= cache_.size()) return 255;
    return cache_[pageId];
}

void FreeSpaceMap::setFreePercent(uint32_t pageId, uint8_t percent) {
    std::lock_guard<std::mutex> lock(mutex_);
    ensureSizeLocked(pageId);
    if (cache_[pageId] != percent) {
        cache_[pageId] = percent;
        dirty_ = true;
    }
}

uint32_t FreeSpaceMap::findPage(uint8_t minPercent, uint32_t startPage) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (uint32_t i = startPage; i < cache_.size(); ++i) {
        if (cache_[i] >= minPercent && cache_[i] != 255) {
            return i;
        }
    }
    return 0;
}

uint32_t FreeSpaceMap::numPages() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return numPages_;
}

void FreeSpaceMap::flush() {
    (void)flushChecked();
}

bool FreeSpaceMap::flushChecked() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (dirty_) {
        if (!writeToDiskLocked()) return false;
        dirty_ = false;
    }
    return derived_map_file::matches(fd_, filename_, cache_);
}

bool FreeSpaceMap::quiescentForSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !dirty_ && derived_map_file::matches(fd_, filename_, cache_);
}

} // namespace dbms
