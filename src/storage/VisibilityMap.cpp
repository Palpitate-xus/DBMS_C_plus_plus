#include "VisibilityMap.h"

namespace dbms {

VisibilityMap::VisibilityMap(const std::string& filename) : filename_(filename) {}

VisibilityMap::~VisibilityMap() {
    flush();
    close();
}

bool VisibilityMap::open() {
    if (f_.is_open()) return true;
    f_.open(filename_, std::ios::in | std::ios::out | std::ios::binary);
    if (!f_.is_open()) {
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

void VisibilityMap::close() {
    if (f_.is_open()) {
        flush();
        f_.close();
    }
    cache_.clear();
}

void VisibilityMap::loadFromDisk() const {
    f_.seekg(0, std::ios::end);
    auto size = f_.tellg();
    if (size <= 0) {
        cache_.clear();
        return;
    }
    cache_.resize(static_cast<size_t>(size), 0);
    f_.seekg(0, std::ios::beg);
    f_.read(reinterpret_cast<char*>(cache_.data()), size);
}

void VisibilityMap::writeToDisk() const {
    if (!f_.is_open() || cache_.empty()) return;
    f_.seekp(0, std::ios::beg);
    f_.write(reinterpret_cast<const char*>(cache_.data()), cache_.size());
    f_.flush();
}

void VisibilityMap::ensureSize(uint32_t pageId) {
    size_t need = byteIndex(pageId) + 1;
    if (need > cache_.size()) {
        cache_.resize(need, 0);
        dirty_ = true;
    }
}

bool VisibilityMap::isAllVisible(uint32_t pageId) const {
    size_t bi = byteIndex(pageId);
    if (bi >= cache_.size()) return false;
    return (cache_[bi] & bitMask(pageId)) != 0;
}

void VisibilityMap::setAllVisible(uint32_t pageId, bool visible) {
    ensureSize(pageId);
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
    return static_cast<uint32_t>(cache_.size() * 8);
}

void VisibilityMap::flush() {
    if (dirty_) {
        writeToDisk();
        dirty_ = false;
    }
}

} // namespace dbms
