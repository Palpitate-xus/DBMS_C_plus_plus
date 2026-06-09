#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace dbms {

// In-memory hash index with file persistence
class HashIndex {
public:
    explicit HashIndex(const std::filesystem::path& indexFile);
    ~HashIndex() { close(); }

    bool open();
    void close();

    // Insert key->rid mapping (allows duplicates)
    void insert(const std::string& key, int64_t rid);

    // Remove one occurrence of rid for key
    void remove(const std::string& key, int64_t rid);

    // Search: return all rids matching key
    std::vector<int64_t> search(const std::string& key) const;

    // Check if key exists
    bool contains(const std::string& key) const;

    void clear();

    bool isOpen() const { return loaded_; }

    size_t size() const { return map_.size(); }

private:
    std::filesystem::path filePath_;
    std::unordered_map<std::string, std::vector<int64_t>> map_;
    bool loaded_ = false;
    bool dirty_ = false;

    void loadFromFile();
    void saveToFile();
};

} // namespace dbms
