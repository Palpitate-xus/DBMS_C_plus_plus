#include "oid.h"
#include <algorithm>
#include <fstream>
#include <iostream>

namespace dbms {

OidGenerator::OidGenerator(const std::string& persistPath)
    : persistPath_(persistPath)
    , freeListPath_(persistPath + ".free") {
    std::ifstream in(persistPath);
    uint32_t val = 0;
    if (in >> val) {
        nextOid_.store(val, std::memory_order_relaxed);
    }
    loadFreeList();
}

void OidGenerator::loadFreeList() {
    std::ifstream in(freeListPath_);
    if (!in) return;
    Oid oid = 0;
    while (in >> oid) {
        if (oid >= kFirstUserOid) {
            freeList_.insert(oid);
        }
    }
}

void OidGenerator::persistFreeList() {
    std::ofstream out(freeListPath_);
    if (!out) return;
    for (Oid oid : freeList_) {
        out << oid << "\n";
    }
}

Oid OidGenerator::allocate() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!freeList_.empty()) {
            auto it = freeList_.begin();
            Oid oid = *it;
            freeList_.erase(it);
            persistFreeList();
            return oid;
        }
    }

    Oid oid = nextOid_.fetch_add(1, std::memory_order_relaxed);
    // Lazy persist: write every 100 allocations
    if (oid % 100 == 0) {
        (void)persist();
    }
    return oid;
}

Oid OidGenerator::allocateBatch(uint32_t count) {
    if (count == 0) return kInvalidOid;
    Oid start = nextOid_.fetch_add(count, std::memory_order_relaxed);
    (void)persist();
    return start;
}

Oid OidGenerator::peekNext() const {
    return nextOid_.load(std::memory_order_relaxed);
}

bool OidGenerator::persist() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::ofstream out(persistPath_);
    if (!out) return false;
    out << nextOid_.load(std::memory_order_relaxed) << "\n";
    out.flush();
    return out.good();
}

void OidGenerator::setNext(Oid next) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        nextOid_.store(next, std::memory_order_relaxed);
    }
    (void)persist();
}

void OidGenerator::deallocate(Oid oid) {
    if (oid < kFirstUserOid) return;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        freeList_.insert(oid);
    }
    persistFreeList();
}

size_t OidGenerator::freeCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return freeList_.size();
}

} // namespace dbms
