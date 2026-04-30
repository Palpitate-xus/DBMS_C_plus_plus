#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace dbms {

// Fixed-length key size for B+ tree nodes
constexpr size_t BP_KEY_LEN = 20;
constexpr size_t BP_PAGE_SIZE = 4096;

// Disk-based B+ Tree index
class BPTree {
public:
    explicit BPTree(const std::filesystem::path& indexFile);
    ~BPTree();

    // Open or create index file
    bool open();
    void close();

    // Insert key-value pair. Returns false if key already exists.
    bool insert(const std::string& key, int64_t value);

    // Remove key. Returns false if key not found.
    bool remove(const std::string& key);

    // Search for key. Returns true if found, sets value.
    bool search(const std::string& key, int64_t& value) const;

    // Range scan: [startKey, endKey] inclusive
    std::vector<int64_t> rangeScan(const std::string& startKey, const std::string& endKey) const;

    // Get all values in key order
    std::vector<int64_t> allValues() const;

    bool isOpen() const { return fd_ >= 0; }

private:
    std::filesystem::path filePath_;
    int fd_ = -1;  // POSIX file descriptor for direct IO

    struct FileHeader {
        uint32_t rootPage = 0;      // page number of root node
        uint32_t nextFreePage = 1;  // next unallocated page
        uint16_t order = 100;       // max keys per node
        uint16_t reserved = 0;
    } header_;

    struct Node {
        uint8_t isLeaf = 0;
        uint16_t numKeys = 0;
        std::vector<std::string> keys;
        std::vector<uint32_t> children;  // internal node: child page numbers
        std::vector<int64_t> values;     // leaf node: row indices
        uint32_t nextLeaf = 0;           // leaf node: next sibling page
    };

    void writeHeader();
    void readHeader();

    void writeNode(uint32_t pageNum, const Node& node);
    Node readNode(uint32_t pageNum) const;

    uint32_t allocPage();

    bool insertNonFull(uint32_t pageNum, const std::string& key, int64_t value);
    void splitChild(uint32_t parentPage, int childIdx, uint32_t childPage);

    bool removeFromNode(uint32_t pageNum, const std::string& key);
    bool searchNode(uint32_t pageNum, const std::string& key, int64_t& value) const;
    void collectRange(uint32_t pageNum, const std::string& startKey,
                      const std::string& endKey, std::vector<int64_t>& out) const;

    static void serializeNode(char* buf, const Node& node, uint16_t order);
    static void deserializeNode(const char* buf, Node& node, uint16_t order);

    static std::string normalizeKey(const std::string& s);
};

} // namespace dbms
