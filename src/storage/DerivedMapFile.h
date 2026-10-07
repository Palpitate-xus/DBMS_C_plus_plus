#pragma once

#include "DerivedMapPublication.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <fcntl.h>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <sys/file.h>
#include <sys/stat.h>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace dbms::derived_map_file {

// Map state may only be read/published through its actual opened inode.
// A table restore/rename can republish the path while raw callers retain a map.
inline bool owned(int fd, const std::string& filename, struct stat* result = nullptr) {
    struct stat opened{}, current{};
    if (fd < 0 || ::fstat(fd, &opened) != 0 || ::lstat(filename.c_str(), &current) != 0 ||
        !S_ISREG(opened.st_mode) || !S_ISREG(current.st_mode) ||
        opened.st_dev != current.st_dev || opened.st_ino != current.st_ino) return false;
    if (result) *result = opened;
    return true;
}

inline bool read(int fd, const std::string& filename, std::vector<uint8_t>& data,
                 struct stat* generation = nullptr) {
    struct stat before{};
    if (!owned(fd, filename, &before) || before.st_size < 0 ||
        static_cast<uint64_t>(before.st_size) > UINT32_MAX) return false;
    std::vector<uint8_t> bytes;
    try { bytes.resize(static_cast<size_t>(before.st_size)); }
    catch (...) { return false; }
    size_t offset = 0;
    while (offset < bytes.size()) {
        const auto count = ::pread(fd, bytes.data() + offset, bytes.size() - offset,
                                   static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        offset += static_cast<size_t>(count);
    }
    struct stat after{};
    if (!owned(fd, filename, &after) || before.st_size != after.st_size ||
        before.st_mtim.tv_sec != after.st_mtim.tv_sec || before.st_mtim.tv_nsec != after.st_mtim.tv_nsec ||
        before.st_ctim.tv_sec != after.st_ctim.tv_sec || before.st_ctim.tv_nsec != after.st_ctim.tv_nsec)
        return false;
    data.swap(bytes);
    if (generation) *generation = after;
    return true;
}

inline int open(const std::string& filename, std::vector<uint8_t>& data) {
    const int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0644);
    if (fd < 0) return -1;
    if (!read(fd, filename, data)) { ::close(fd); return -1; }
    return fd;
}

inline bool write(int fd, const std::string& filename, const std::vector<uint8_t>& data,
                  uint8_t kind, std::vector<uint8_t> expectedBefore,
                  std::string& acceptedProgress) {
    if (!owned(fd, filename)) return false;
    // An interrupted local publication still has to be retryable. Only exact
    // bytes produced by our acknowledged base plus successful writes qualify;
    // this is not a durability acknowledgement or permission for raw edits.
    const auto failed = [&]() {
        std::vector<uint8_t> actual;
        if (read(fd, filename, actual) && actual == expectedBefore)
            acceptedProgress = derived_map_publication::digest(actual);
        return false;
    };
    size_t offset = 0;
    while (offset < data.size()) {
        const auto count = ::pwrite(fd, data.data() + offset, data.size() - offset,
                                    static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return failed();
        if (expectedBefore.size() < offset + static_cast<size_t>(count))
            expectedBefore.resize(offset + static_cast<size_t>(count), 0);
        std::copy(data.begin() + offset, data.begin() + offset + count,
                  expectedBefore.begin() + offset);
        offset += static_cast<size_t>(count);
    }
    if (::ftruncate(fd, static_cast<off_t>(data.size())) != 0) return failed();
    expectedBefore.resize(data.size());
    if (!derived_map_publication::stamp(fd, filename, kind, data)) return failed();
    int synced;
    do { synced = ::fsync(fd); } while (synced != 0 && errno == EINTR);
    return (synced == 0 && owned(fd, filename)) || failed();
}

inline bool matches(int fd, const std::string& filename, const std::vector<uint8_t>& data) {
    std::vector<uint8_t> actual;
    return read(fd, filename, actual) && actual == data;
}

// Pending cells are shared by actual inode, not StorageEngine or pathname.
struct Cache {
    std::mutex mutex;
    std::vector<uint8_t> bytes;
    std::map<size_t, uint8_t> pendingMasks;
    bool dirty = false;
    uint8_t kind = 0;
    std::string acknowledgedDigest;
    struct stat generation{};
};

using CacheKey = std::tuple<uint64_t, uint64_t, uint8_t>;
struct CacheEntry {
    std::weak_ptr<Cache> observed;
    std::shared_ptr<Cache> pending;
    int pendingFd = -1;
    CacheEntry() = default;
    CacheEntry(const CacheEntry&) = delete;
    CacheEntry& operator=(const CacheEntry&) = delete;
    ~CacheEntry() { if (pendingFd >= 0) ::close(pendingFd); }
    void clearPending() {
        if (pendingFd >= 0) ::close(pendingFd);
        pendingFd = -1;
        pending.reset();
    }
};
struct CacheRegistry {
    std::mutex mutex;
    std::map<CacheKey, CacheEntry> entries;
    size_t lookups = 0;
};

inline CacheRegistry& cacheRegistry() {
    // Global engines may close maps after normal function-static destruction.
    // The process-owned registry has explicit per-entry cleanup instead.
    static auto* registry = new CacheRegistry;
    return *registry;
}

inline CacheKey cacheKey(const Cache& state) {
    return {static_cast<uint64_t>(state.generation.st_dev),
            static_cast<uint64_t>(state.generation.st_ino), state.kind};
}

inline bool sameGeneration(const struct stat& left, const struct stat& right) {
    return left.st_dev == right.st_dev && left.st_ino == right.st_ino &&
        left.st_size == right.st_size &&
        left.st_mtim.tv_sec == right.st_mtim.tv_sec &&
        left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
        left.st_ctim.tv_sec == right.st_ctim.tv_sec &&
        left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

inline std::shared_ptr<Cache> sharedCache(int fd, const std::string& filename,
                                        uint8_t kind, std::vector<uint8_t> initial) {
    struct stat generation{};
    if (!read(fd, filename, initial, &generation)) return {};
    auto& registry = cacheRegistry();
    std::lock_guard<std::mutex> guard(registry.mutex);
    if (++registry.lookups % 64 == 0) {
        for (auto it = registry.entries.begin(); it != registry.entries.end();) {
            if (it->second.pending && it->second.pendingFd >= 0) {
                struct stat pinned{};
                // A completed physical unlink retires this exact inode. An
                // intent, missing pathname, IO error, or rename is not proof.
                if (::fstat(it->second.pendingFd, &pinned) == 0 && pinned.st_nlink == 0)
                    it->second.clearPending();
            }
            if (it->second.observed.expired()) it = registry.entries.erase(it);
            else ++it;
        }
    }
    auto& entry = registry.entries[{static_cast<uint64_t>(generation.st_dev),
                                    static_cast<uint64_t>(generation.st_ino), kind}];
    if (auto existing = entry.observed.lock()) return existing;
    auto state = std::make_shared<Cache>();
    state->bytes = std::move(initial);
    state->acknowledgedDigest = derived_map_publication::digest(state->bytes);
    state->generation = generation;
    state->kind = kind;
    entry.observed = state;
    return state;
}

// Caller holds the cache mutex. Failed close transfers the actual fd rather
// than needing another fd/allocation; a new opener can retry the same pending
// inode even after the last old map object has been destroyed.
inline bool retainPending(const std::shared_ptr<Cache>& state, int& fd) {
    struct stat opened{};
    if (fd < 0 || ::fstat(fd, &opened) != 0 || !S_ISREG(opened.st_mode) ||
        opened.st_dev != state->generation.st_dev ||
        opened.st_ino != state->generation.st_ino) return false;
    auto& registry = cacheRegistry();
    std::lock_guard<std::mutex> guard(registry.mutex);
    const auto found = registry.entries.find(cacheKey(*state));
    if (found == registry.entries.end() ||
        found->second.observed.lock().get() != state.get()) return false;
    auto& entry = found->second;
    entry.pending = state;
    if (entry.pendingFd < 0) {
        entry.pendingFd = fd;
        fd = -1;
    }
    return true;
}

inline void releasePending(const Cache& state) {
    auto& registry = cacheRegistry();
    std::lock_guard<std::mutex> guard(registry.mutex);
    const auto found = registry.entries.find(cacheKey(state));
    if (found != registry.entries.end() && found->second.pending.get() == &state)
        found->second.clearPending();
}

inline void overlayPending(const Cache& state, std::vector<uint8_t>& actual,
                           uint8_t emptyByte) {
    if (state.dirty && actual.size() < state.bytes.size())
        actual.resize(state.bytes.size(), emptyByte);
    for (const auto& [index, mask] : state.pendingMasks) {
        if (index >= actual.size()) actual.resize(index + 1, emptyByte);
        actual[index] = static_cast<uint8_t>((actual[index] & ~mask) |
                                            (state.bytes[index] & mask));
    }
}

// Caller owns the actual source's flock. Matching a candidate is not enough:
// independently sync and recheck both its complete bytes and receipt.
inline bool readPublishedLocked(int fd, const std::string& filename, uint8_t kind,
                                std::vector<uint8_t>& actual, struct stat& generation) {
    struct stat before{}, after{};
    if (!read(fd, filename, actual, &before) ||
        !derived_map_publication::matches(fd, filename, kind, before, actual, true) ||
        !derived_map_publication::sync(fd)) return false;
    std::vector<uint8_t> verified;
    if (!read(fd, filename, verified, &after) || !sameGeneration(before, after) ||
        verified != actual ||
        !derived_map_publication::matches(fd, filename, kind, after, verified, false)) return false;
    actual.swap(verified);
    generation = after;
    return true;
}

// Caller holds the shared cache mutex; actual bytes are loaded before overlay.
inline bool refresh(int fd, const std::string& filename, Cache& state,
                    uint8_t emptyByte) {
    struct stat observed{};
    if (!owned(fd, filename, &observed)) return false;
    if (sameGeneration(observed, state.generation)) return true;
    int locked;
    do { locked = ::flock(fd, LOCK_SH); } while (locked != 0 && errno == EINTR);
    if (locked != 0) return false;
    struct Unlock { int fd; ~Unlock() { (void)::flock(fd, LOCK_UN); } } unlock{fd};
    std::vector<uint8_t> actual;
    if (!read(fd, filename, actual, &observed)) return false;
    auto digest = derived_map_publication::digest(actual);
    if (digest != state.acknowledgedDigest) {
        if (!readPublishedLocked(fd, filename, state.kind, actual, observed)) return false;
        digest = derived_map_publication::digest(actual);
    }
    overlayPending(state, actual, emptyByte);
    state.bytes.swap(actual);
    state.generation = observed;
    state.acknowledgedDigest = std::move(digest);
    return true;
}

inline bool publish(int fd, const std::string& filename, Cache& state,
                    uint8_t emptyByte) {
    if (!state.dirty) return matches(fd, filename, state.bytes);
    int locked;
    do { locked = ::flock(fd, LOCK_EX); } while (locked != 0 && errno == EINTR);
    if (locked != 0) return false;
    struct Unlock { int fd; ~Unlock() { (void)::flock(fd, LOCK_UN); } } unlock{fd};
    std::vector<uint8_t> actual;
    if (!read(fd, filename, actual)) return false;
    struct stat observed{};
    if (derived_map_publication::digest(actual) != state.acknowledgedDigest &&
        (!owned(fd, filename, &observed) ||
         !derived_map_publication::matches(fd, filename, state.kind, observed, actual, false)))
        return false;
    auto acceptedBase = actual;
    overlayPending(state, actual, emptyByte);
    std::string acceptedProgress;
    if (!write(fd, filename, actual, state.kind, std::move(acceptedBase), acceptedProgress)) {
        if (!acceptedProgress.empty()) state.acknowledgedDigest = std::move(acceptedProgress);
        return false;
    }
    if (!matches(fd, filename, actual) ||
        !owned(fd, filename, &state.generation)) return false;
    state.bytes.swap(actual);
    state.acknowledgedDigest = derived_map_publication::digest(state.bytes);
    state.pendingMasks.clear();
    state.dirty = false;
    releasePending(state);
    return true;
}

// Explicit engine-owned reconciliation; generic flush/snapshot remains strict.
// A publication candidate alone is never an fsync/durability acknowledgement.
inline bool refreshPublished(int fd, const std::string& filename, Cache& state) {
    if (state.dirty || !owned(fd, filename)) return false;
    int locked;
    do { locked = ::flock(fd, LOCK_SH); } while (locked != 0 && errno == EINTR);
    if (locked != 0) return false;
    struct Unlock { int fd; ~Unlock() { (void)::flock(fd, LOCK_UN); } } unlock{fd};
    std::vector<uint8_t> actual;
    struct stat generation{};
    if (!readPublishedLocked(fd, filename, state.kind, actual, generation)) return false;
    state.bytes.swap(actual);
    state.acknowledgedDigest = derived_map_publication::digest(state.bytes);
    state.generation = generation;
    return true;
}

} // namespace dbms::derived_map_file
