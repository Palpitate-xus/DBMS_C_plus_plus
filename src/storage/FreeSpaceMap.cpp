#include "FreeSpaceMap.h"

#include <iostream>

namespace dbms {

FreeSpaceMap::FreeSpaceMap(const std::string& filename) : filename_(filename) {}

FreeSpaceMap::~FreeSpaceMap() {
    flush();
    close();
}

bool FreeSpaceMap::open() {
    if (f_.is_open()) return true;
    f_.open(filename_, std::ios::in | std::ios::out | std::ios::binary);
    if (!f_.is_open()) {
        // Try create
        f_.open(filename_, std::ios::out | std::ios::binary);
        if (f_.is_open()) {
            f_.close();
            f_.open(filename_, std::ios::in | std::ios::out | std::ios::binary);
        }
    }
    if (f_.is_open()) {
        loadFromDisk();
    }
    return f_.is_open();
}

void FreeSpaceMap::close() {
    if (f_.is_open()) {
        flush();
        f_.close();
    }
    cache_.clear();
    numPages_ = 0;
}

void FreeSpaceMap::loadFromDisk() const {
    f_.seekg(0, std::ios::end);
    auto size = f_.tellg();
    if (size <= 0) {
        numPages_ = 0;
        return;
    }
    numPages_ = static_cast<uint32_t>(size);
    cache_.resize(numPages_, 255);
    f_.seekg(0, std::ios::beg);
    f_.read(reinterpret_cast<char*>(cache_.data()), numPages_);
}

void FreeSpaceMap::writeToDisk() const {
    if (!f_.is_open() || cache_.empty()) return;
    f_.seekp(0, std::ios::beg);
    f_.write(reinterpret_cast<const char*>(cache_.data()), cache_.size());
    f_.flush();
}

void FreeSpaceMap::ensureSize(uint32_t pageId) {
    if (pageId >= cache_.size()) {
        cache_.resize(pageId + 1, 255);
        numPages_ = static_cast<uint32_t>(cache_.size());
        dirty_ = true;
    }
}

uint8_t FreeSpaceMap::getFreePercent(uint32_t pageId) const {
    if (pageId >= cache_.size()) return 255;
    return cache_[pageId];
}

void FreeSpaceMap::setFreePercent(uint32_t pageId, uint8_t percent) {
    ensureSize(pageId);
    if (cache_[pageId] != percent) {
        cache_[pageId] = percent;
        dirty_ = true;
    }
}

uint32_t FreeSpaceMap::findPage(uint8_t minPercent, uint32_t startPage) const {
    for (uint32_t i = startPage; i < cache_.size(); ++i) {
        if (cache_[i] >= minPercent && cache_[i] != 255) {
            return i;
        }
    }
    return 0;
}

void FreeSpaceMap::flush() {
    if (dirty_) {
        writeToDisk();
        dirty_ = false;
    }
}

} // namespace dbms
