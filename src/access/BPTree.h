#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

    // Checked reads distinguish a valid empty lookup from a failed traversal.
    // The legacy bool/vector APIs remain available to mutation code that
    // reports failures through DBStatus and statement rollback.
    enum class SearchResult { Found, NotFound, Error };
    SearchResult searchChecked(const std::string& key, int64_t& value) const;
    bool searchMultiChecked(const std::string& key,
                            std::vector<int64_t>& values) const;
    bool rangeScanChecked(const std::string& startKey, const std::string& endKey,
                          std::vector<int64_t>& values) const;
    bool allValuesChecked(std::vector<int64_t>& values) const;

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

    bool isOpen() const {
        std::shared_lock<std::shared_mutex> lock(treeMutex_);
        return bp_ != nullptr && bp_->isOpen();
    }

    bool hasStaleFileGeneration() const {
        std::shared_lock<std::shared_mutex> lock(treeMutex_);
        return bp_ != nullptr && bp_->isOpen() &&
               !bp_->refersToCurrentFiles();
    }

    const std::filesystem::path& filePath() const { return filePath_; }
    bool hasDirtyPages() const {
        std::shared_lock<std::shared_mutex> lock(treeMutex_);
        if (!bp_ || !bp_->isOpen()) return false;
        for (const auto& frame : bp_->getFrameInfo()) {
            if (frame.dirty) return true;
        }
        return false;
    }

    uint32_t rootPage() const {
        std::shared_lock<std::shared_mutex> lock(treeMutex_);
        return header_.rootPage;
    }

    // Buffer pool stats
    size_t cacheHits() const {
        std::shared_lock<std::shared_mutex> lock(treeMutex_);
        return bp_ ? bp_->hits() : 0;
    }
    size_t cacheMisses() const {
        std::shared_lock<std::shared_mutex> lock(treeMutex_);
        return bp_ ? bp_->misses() : 0;
    }

private:
    std::filesystem::path filePath_;
    std::unique_ptr<BufferPool> bp_;

    // Writers serialize whole nodes directly into buffer-pool frames, so
    // readers must not deserialize those frames concurrently. Shared tree
    // locking permits parallel lookups while excluding every mutation and
    // open/close transition.
    mutable std::shared_mutex treeMutex_;

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

    // ---- Node cache ----------------------------------------------------
    // Every descent used to deserialize the full node (up to `order` heap
    // strings + vectors) per tree level per operation. Internal nodes deep
    // in the tree are touched by nearly every lookup, so a small read-mostly
    // cache in front of readNode removes most of that work. Writers publish
    // their new node through writeNode, which updates the cache in place.
    static constexpr size_t kNodeCacheCapacity = 64;
    mutable std::shared_mutex nodeCacheMutex_;
    mutable std::unordered_map<uint32_t, std::shared_ptr<const Node>> nodeCache_;
    mutable std::deque<uint32_t> nodeCacheOrder_;  // front = most recent

    std::shared_ptr<const Node> cachedNode(uint32_t pageNum) const;
    // Insert/refresh a cache entry (also used by the read path to warm it).
    void cacheNode(uint32_t pageNum, const Node& node) const;
    void dropNodeFromCache(uint32_t pageNum);

    uint32_t allocPage();

    enum class RemoveResult { Removed, NotFound, Error };

    // A valid order>=2 B+ tree backed by 32-bit page numbers cannot approach
    // this depth.  A fixed path avoids allocating a hash table on every point
    // lookup while still rejecting cycles and maliciously deep chains.
    static constexpr size_t kMaxTraversalDepth = 64;
    struct TraversalPath {
        std::array<uint32_t, kMaxTraversalDepth> pages{};
        size_t length = 0;

        bool visit(uint32_t page) {
            if (length >= pages.size()) return false;
            if (std::find(pages.begin(), pages.begin() + length, page) !=
                pages.begin() + length) {
                return false;
            }
            pages[length++] = page;
            return true;
        }
        void clear() { length = 0; }
    };

    bool insertNonFull(uint32_t pageNum, const std::string& key, int64_t value,
                       TraversalPath& visited);
    bool splitChild(uint32_t parentPage, int childIdx, uint32_t childPage);

    RemoveResult removeFromNode(uint32_t pageNum, const std::string& key,
                                const std::optional<int64_t>& value,
                                std::unordered_set<uint32_t>& visited);
    SearchResult searchNode(uint32_t pageNum, const std::string& key, int64_t& value,
                            TraversalPath& visited) const;
    bool collectRange(uint32_t pageNum, const std::string& startKey,
                      const std::string& endKey, std::vector<int64_t>& out,
                      std::unordered_set<uint32_t>& visited) const;

    static void serializeNode(char* buf, const Node& node, uint16_t order);
    static bool deserializeNode(const char* buf, Node& node, uint16_t order);

    static std::string normalizeKey(const std::string& s);
};

} // namespace dbms
