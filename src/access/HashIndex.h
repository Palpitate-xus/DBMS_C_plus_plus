#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace dbms {

// In-memory hash index with file persistence.
// All public methods are serialized by an internal mutex: concurrent DML
// under table intent locks may reach the same index object from different
// threads (foreground statements and background flush alike).
class HashIndex {
public:
    explicit HashIndex(const std::filesystem::path& indexFile);
    ~HashIndex() { close(); }

    bool open();
    bool close();
    // Persist the current in-memory mapping without closing the index.
    bool flush();

    // Insert key->rid mapping (allows duplicates)
    bool insert(const std::string& key, int64_t rid);

    // Remove one occurrence of rid for key
    bool remove(const std::string& key, int64_t rid);

    // Search: return all rids matching key
    std::vector<int64_t> search(const std::string& key) const;

    // Check if key exists
    bool contains(const std::string& key) const;

    void clear();

    bool isOpen() const;
    const std::filesystem::path& filePath() const { return filePath_; }
    bool hasDirtyData() const;

    size_t size() const;

private:
    std::filesystem::path filePath_;
    std::unordered_map<std::string, std::vector<int64_t>> map_;
    bool loaded_ = false;
    bool dirty_ = false;
    mutable std::mutex mutex_;

    bool loadFromFile();
    bool saveToFile();
};

} // namespace dbms
