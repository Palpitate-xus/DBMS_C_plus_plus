#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "BufferPool.h"

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
    // Flush all dirty index pages and fsync the index file.
    bool flush();

    // Insert key-value pair. Keys are normalized to BP_KEY_LEN bytes; values
    // beyond that boundary are truncated consistently by every public API.
    // Returns false if the normalized key already exists.
    bool insert(const std::string& key, int64_t value);

    // Remove key. Returns false if key not found.
    bool remove(const std::string& key);

    // Remove one exact key-value pair from a multi-value index.
    bool removeMulti(const std::string& key, int64_t value);

    // Search for a key using the same fixed-width ordering as the on-disk tree.
    // Returns true if found, sets value.
    bool search(const std::string& key, int64_t& value) const;

    // Multi-value search: returns all values for a key (allows duplicates)
    std::vector<int64_t> searchMulti(const std::string& key) const;

    // Insert allowing duplicate keys (for secondary indexes)
    bool insertMulti(const std::string& key, int64_t value);

    // Range scan: [startKey, endKey] inclusive
    std::vector<int64_t> rangeScan(const std::string& startKey, const std::string& endKey) const;

    // Get all values in key order
    std::vector<int64_t> allValues() const;

    // Use the same fixed-width ordering as on-disk index keys.  SSI
    // predicate coverage must compare values in index order, not in the
    // variable-width order of the SQL literals.
    static std::string normalizeKeyForComparison(const std::string& key);

    bool isOpen() const { return bp_ != nullptr && bp_->isOpen(); }

    const std::filesystem::path& filePath() const { return filePath_; }
    bool hasDirtyPages() const {
        if (!bp_ || !bp_->isOpen()) return false;
        for (const auto& frame : bp_->getFrameInfo()) {
            if (frame.dirty) return true;
        }
        return false;
    }

    uint32_t rootPage() const { return header_.rootPage; }

    // Buffer pool stats
    size_t cacheHits() const { return bp_ ? bp_->hits() : 0; }
    size_t cacheMisses() const { return bp_ ? bp_->misses() : 0; }

private:
    std::filesystem::path filePath_;
    std::unique_ptr<BufferPool> bp_;

    // Serializes node mutations. The tree deserializes whole nodes, mutates
    // them locally and writes them back, so two writers could otherwise
    // interleave their read-modify-write cycles and lose entries. Insert
    // paths hold the table intent lock only, which does not exclude other
    // writers inside this process. Recursive so public mutators may call the
    // locked public search() internally.
    mutable std::recursive_mutex writeMutex_;

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

    bool writeHeader();
    bool readHeader();

    bool writeNode(uint32_t pageNum, const Node& node);
    std::optional<Node> readNode(uint32_t pageNum) const;

    uint32_t allocPage();

    bool insertNonFull(uint32_t pageNum, const std::string& key, int64_t value);
    bool splitChild(uint32_t parentPage, int childIdx, uint32_t childPage);

    bool removeFromNode(uint32_t pageNum, const std::string& key,
                        const std::optional<int64_t>& value);
    bool searchNode(uint32_t pageNum, const std::string& key, int64_t& value) const;
    bool collectRange(uint32_t pageNum, const std::string& startKey,
                      const std::string& endKey, std::vector<int64_t>& out) const;

    static void serializeNode(char* buf, const Node& node, uint16_t order);
    static void deserializeNode(const char* buf, Node& node, uint16_t order);

    static std::string normalizeKey(const std::string& s);
};

} // namespace dbms
