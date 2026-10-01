#include "oid.h"
#include "common/DbError.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <unordered_map>

namespace dbms {

namespace {
// Physical DDL rollback can restore an older counter file, but active
// backends still hold object identities allocated after that snapshot.
// Preserve the allocation watermark independently of transactional files.
struct OidWatermarkState {
    std::mutex mutex;
    std::unordered_map<std::string, uint64_t> values;
};

OidWatermarkState& oidWatermarkState() {
    // StorageEngine can construct catalog generators from global startup.
    // Initialize on first use so their translation-unit order cannot access
    // an unordered_map whose constructor has not run yet.
    // Global catalogs may persist from their destructors after ordinary
    // function-local statics have been destroyed. This process-lifetime
    // registry must outlive them too.
    static OidWatermarkState* state = new OidWatermarkState;
    return *state;
}

std::string oidCounterKey(const std::string& path) {
    return std::filesystem::absolute(path).lexically_normal().string();
}

Oid rememberOidWatermark(const std::string& path, Oid next) {
    auto& state = oidWatermarkState();
    std::lock_guard<std::mutex> lock(state.mutex);
    auto& watermark = state.values[oidCounterKey(path)];
    watermark = std::max(watermark, static_cast<uint64_t>(next));
    return static_cast<Oid>(watermark);
}

Oid reserveOidRange(const std::string& path, std::atomic<Oid>& next,
                    uint32_t count) {
    auto& state = oidWatermarkState();
    std::lock_guard<std::mutex> lock(state.mutex);
    auto& watermark = state.values[oidCounterKey(path)];
    const uint64_t start = std::max(watermark,
        static_cast<uint64_t>(next.load(std::memory_order_relaxed)));
    const uint64_t end = start + count;
    if (end > std::numeric_limits<Oid>::max()) {
        throw DbError("54000", "object identifier space exhausted");
    }
    watermark = end;
    next.store(static_cast<Oid>(end), std::memory_order_relaxed);
    return static_cast<Oid>(start);
}
} // namespace

OidGenerator::OidGenerator(const std::string& persistPath)
    : persistPath_(persistPath)
    , freeListPath_(persistPath + ".free") {
    std::ifstream in(persistPath);
    uint32_t val = 0;
    if (in >> val) {
        nextOid_.store(val, std::memory_order_relaxed);
    }
    nextOid_.store(rememberOidWatermark(persistPath_, nextOid_.load()),
                   std::memory_order_relaxed);
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

    Oid oid = reserveOidRange(persistPath_, nextOid_, 1);
    // Lazy persist: write every 100 allocations
    if (oid % 100 == 0) {
        (void)persist();
    }
    return oid;
}

Oid OidGenerator::allocateBatch(uint32_t count) {
    if (count == 0) return kInvalidOid;
    Oid start = reserveOidRange(persistPath_, nextOid_, count);
    (void)persist();
    return start;
}

Oid OidGenerator::peekNext() const {
    return nextOid_.load(std::memory_order_relaxed);
}

bool OidGenerator::persist() {
    std::lock_guard<std::mutex> lock(mutex_);
    // An older instance must not publish a counter below a range reserved by
    // another instance for this same file.
    auto& state = oidWatermarkState();
    std::lock_guard<std::mutex> watermarkLock(state.mutex);
    auto& watermark = state.values[oidCounterKey(persistPath_)];
    watermark = std::max(watermark,
        static_cast<uint64_t>(nextOid_.load(std::memory_order_relaxed)));
    nextOid_.store(static_cast<Oid>(watermark), std::memory_order_relaxed);
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
