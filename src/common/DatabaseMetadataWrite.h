#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <mutex>

namespace dbms {

// The transaction that holds a metadata before-image excludes other metadata
// publishers until its durable outcome. Nested writes by that owner may
// reenter. Other publishers return a lock conflict without holding any
// storage or catalog mutex while waiting for the database owner.
class DatabaseMetadataWrite {
public:
    explicit DatabaseMetadataWrite(const std::filesystem::path& database) {
        std::error_code error;
        path_ = std::filesystem::weakly_canonical(database, error);
        if (error || path_.empty()) return;
        // Catalogs can still publish during global destruction, after
        // destructible objects in the main thread's TLS have been released.
        static std::atomic<uint64_t> nextThread{1};
        static thread_local const uint64_t thread =
            nextThread.fetch_add(1, std::memory_order_relaxed);
        auto& registry = entries();
        std::lock_guard<std::mutex> lock(registry.mutex);
        auto [it, inserted] = registry.owners.try_emplace(path_, Owner{thread, 0});
        (void)inserted;
        if (it->second.thread != thread) {
            contended_ = true;
            return;
        }
        ++it->second.count;
        acquired_ = true;
    }

    ~DatabaseMetadataWrite() {
        if (!acquired_) return;
        auto& registry = entries();
        std::lock_guard<std::mutex> lock(registry.mutex);
        const auto it = registry.owners.find(path_);
        if (--it->second.count == 0) registry.owners.erase(it);
    }

    DatabaseMetadataWrite(const DatabaseMetadataWrite&) = delete;
    DatabaseMetadataWrite& operator=(const DatabaseMetadataWrite&) = delete;

    bool acquired() const { return acquired_; }
    bool contended() const { return contended_; }

private:
    struct Owner {
        uint64_t thread;
        size_t count;
    };
    struct Registry {
        std::mutex mutex;
        std::map<std::filesystem::path, Owner> owners;
    };
    static Registry& entries() {
        static auto& registry = *new Registry;
        return registry;
    }

    std::filesystem::path path_;
    bool acquired_ = false;
    bool contended_ = false;
};

}  // namespace dbms
