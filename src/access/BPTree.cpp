#include "BPTree.h"

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>

namespace dbms {

static_assert(BP_KEY_LEN >= 4, "BP_KEY_LEN too small");

// ========================================================================
// Key normalization: pad with '\0' to fixed length
// ========================================================================
std::string BPTree::normalizeKey(const std::string& s) {
    std::string k = s;
    if (k.size() > BP_KEY_LEN) k.resize(BP_KEY_LEN);
    else if (k.size() < BP_KEY_LEN) k.append(BP_KEY_LEN - k.size(), '\0');
    return k;
}

// ========================================================================
// Node serialization: pack into BP_PAGE_SIZE bytes
// ========================================================================
void BPTree::serializeNode(char* buf, const Node& node, uint16_t /*order*/) {
    std::memset(buf, 0, BP_PAGE_SIZE);
    buf[0] = node.isLeaf;
    std::memcpy(buf + 1, &node.numKeys, sizeof(uint16_t));
    size_t pos = 3;
    for (size_t i = 0; i < node.numKeys; ++i) {
        std::string k = normalizeKey(node.keys[i]);
        std::memcpy(buf + pos, k.data(), BP_KEY_LEN);
        pos += BP_KEY_LEN;
    }
    if (node.isLeaf) {
        for (size_t i = 0; i < node.numKeys; ++i) {
            std::memcpy(buf + pos, &node.values[i], sizeof(int64_t));
            pos += sizeof(int64_t);
        }
        std::memcpy(buf + pos, &node.nextLeaf, sizeof(uint32_t));
    } else {
        for (size_t i = 0; i <= node.numKeys; ++i) {
            uint32_t child = (i < node.children.size()) ? node.children[i] : 0;
            std::memcpy(buf + pos, &child, sizeof(uint32_t));
            pos += sizeof(uint32_t);
        }
    }
}

void BPTree::deserializeNode(const char* buf, Node& node, uint16_t /*order*/) {
    node.isLeaf = static_cast<uint8_t>(buf[0]);
    std::memcpy(&node.numKeys, buf + 1, sizeof(uint16_t));
    node.keys.clear();
    node.values.clear();
    node.children.clear();
    size_t pos = 3;
    for (size_t i = 0; i < node.numKeys; ++i) {
        std::string k(buf + pos, BP_KEY_LEN);
        auto nul = k.find('\0');
        if (nul != std::string::npos) k.resize(nul);
        node.keys.push_back(k);
        pos += BP_KEY_LEN;
    }
    if (node.isLeaf) {
        for (size_t i = 0; i < node.numKeys; ++i) {
            int64_t v;
            std::memcpy(&v, buf + pos, sizeof(int64_t));
            node.values.push_back(v);
            pos += sizeof(int64_t);
        }
        std::memcpy(&node.nextLeaf, buf + pos, sizeof(uint32_t));
    } else {
        for (size_t i = 0; i <= node.numKeys; ++i) {
            uint32_t child;
            std::memcpy(&child, buf + pos, sizeof(uint32_t));
            node.children.push_back(child);
            pos += sizeof(uint32_t);
        }
    }
}

// ========================================================================
// File I/O
// ========================================================================
BPTree::BPTree(const std::filesystem::path& indexFile)
    : filePath_(indexFile), bp_(std::make_unique<BufferPool>(indexFile.string(), 16)) {}

BPTree::~BPTree() {
    close();
}

bool BPTree::open() {
    if (bp_ && bp_->isOpen()) return true;
    if (!bp_) bp_ = std::make_unique<BufferPool>(filePath_.string(), 16);

    // Check existence BEFORE opening (O_CREAT would create the file)
    bool exists = std::filesystem::exists(filePath_);
    if (!bp_->open()) return false;

    if (exists) {
        if (!readHeader()) {
            bp_->close();
            return false;
        }
    } else {
        header_.rootPage = 0;
        header_.nextFreePage = 1;
        size_t maxLeaf = (BP_PAGE_SIZE - 7) / (BP_KEY_LEN + 8);
        size_t maxInternal = (BP_PAGE_SIZE - 7 - 4) / (BP_KEY_LEN + 4);
        header_.order = static_cast<uint16_t>(std::min(size_t(100), std::min(maxLeaf, maxInternal)));
        if (header_.order < 2) header_.order = 2;
        if (!writeHeader()) {
            bp_->close();
            return false;
        }
    }
    return true;
}

void BPTree::close() {
    if (bp_) {
        bp_->close();
    }
}

bool BPTree::writeHeader() {
    if (!bp_ || !bp_->isOpen()) return false;
    char* buf = bp_->fetchPage(0);
    if (!buf) return false;
    std::memcpy(buf, &header_, sizeof(FileHeader));
    bp_->markDirty(0);
    bp_->unpinPage(0);
    return true;
}

bool BPTree::readHeader() {
    if (!bp_ || !bp_->isOpen()) return false;
    char* buf = bp_->fetchPage(0);
    if (!buf) return false;
    std::memcpy(&header_, buf, sizeof(FileHeader));
    bp_->unpinPage(0);
    if (header_.order < 2 || header_.order > 1000) return false;
    return true;
}

uint32_t BPTree::allocPage() {
    uint32_t page = header_.nextFreePage++;
    if (page == 0 || !writeHeader()) {
        --header_.nextFreePage;
        return 0;
    }
    char* buf = bp_->fetchPage(page);
    if (!buf) {
        --header_.nextFreePage;
        writeHeader();
        return 0;
    }
    std::memset(buf, 0, BP_PAGE_SIZE);
    bp_->markDirty(page);
    bp_->unpinPage(page);
    return page;
}

bool BPTree::writeNode(uint32_t pageNum, const Node& node) {
    if (!bp_ || !bp_->isOpen()) return false;
    char* buf = bp_->fetchPage(pageNum);
    if (!buf) return false;
    serializeNode(buf, node, header_.order);
    bp_->markDirty(pageNum);
    bp_->unpinPage(pageNum);
    return true;
}

std::optional<BPTree::Node> BPTree::readNode(uint32_t pageNum) const {
    if (!bp_ || !bp_->isOpen()) return std::nullopt;
    Node node;
    char* buf = const_cast<BufferPool*>(bp_.get())->fetchPage(pageNum);
    if (!buf) return std::nullopt;
    deserializeNode(buf, node, header_.order);
    bp_->unpinPage(pageNum);
    return node;
}

// ========================================================================
// Search
// ========================================================================
bool BPTree::search(const std::string& key, int64_t& value) const {
    if (!bp_ || !bp_->isOpen() || header_.rootPage == 0) return false;
    return searchNode(header_.rootPage, key, value);
}

bool BPTree::searchNode(uint32_t pageNum, const std::string& key, int64_t& value) const {
    auto nodeOpt = readNode(pageNum);
    if (!nodeOpt) return false;
    Node node = std::move(*nodeOpt);
    if (node.isLeaf) {
        for (size_t i = 0; i < node.numKeys; ++i) {
            if (node.keys[i] == key) {
                value = node.values[i];
                return true;
            }
        }
        return false;
    }
    // Internal node: find child to descend
    size_t i = 0;
    while (i < node.numKeys && key >= node.keys[i]) ++i;
    if (i < node.children.size()) {
        return searchNode(node.children[i], key, value);
    }
    return false;
}

// ========================================================================
// Insert
// ========================================================================
bool BPTree::insert(const std::string& key, int64_t value) {
    if (!bp_ || !bp_->isOpen()) return false;
    if (header_.rootPage == 0) {
        // Create root leaf
        uint32_t root = allocPage();
        if (root == 0) return false;
        Node node;
        node.isLeaf = 1;
        node.numKeys = 1;
        node.keys.push_back(key);
        node.values.push_back(value);
        node.nextLeaf = 0;
        if (!writeNode(root, node)) return false;
        header_.rootPage = root;
        return writeHeader();
    }

    // Check if key already exists
    int64_t dummy;
    if (search(key, dummy)) return false;

    auto rootOpt = readNode(header_.rootPage);
    if (!rootOpt) return false;
    Node root = std::move(*rootOpt);
    if (root.numKeys == header_.order) {
        // Split root
        const uint32_t oldRoot = header_.rootPage;
        uint32_t newRoot = allocPage();
        if (newRoot == 0) return false;
        Node nr;
        nr.isLeaf = 0;
        nr.numKeys = 0;
        nr.children.push_back(oldRoot);
        if (!writeNode(newRoot, nr)) return false;
        if (!splitChild(newRoot, 0, oldRoot)) return false;
        header_.rootPage = newRoot;
        if (!writeHeader()) return false;
        // Re-read root after split
    }
    return insertNonFull(header_.rootPage, key, value);
}

// ========================================================================
// Multi-value search (for secondary indexes with duplicate keys)
// ========================================================================
std::vector<int64_t> BPTree::searchMulti(const std::string& key) const {
    std::vector<int64_t> results;
    if (!bp_ || !bp_->isOpen() || header_.rootPage == 0) return results;
    auto nodeOpt = readNode(header_.rootPage);
    if (!nodeOpt) return results;
    Node node = std::move(*nodeOpt);
    while (!node.isLeaf) {
        size_t i = 0;
        // Descend to the leftmost possible leaf for this key.  Equal keys
        // may span multiple leaves after a split; using >= here would start
        // at the rightmost equal separator and silently miss earlier rows.
        while (i < node.numKeys && key > node.keys[i]) ++i;
        if (i >= node.children.size()) return results;
        auto next = readNode(node.children[i]);
        if (!next) return {};
        node = std::move(*next);
    }
    // Scan leaf for matching keys (including duplicates)
    while (true) {
        for (size_t i = 0; i < node.numKeys; ++i) {
            if (node.keys[i] == key) {
                results.push_back(node.values[i]);
            }
        }
        if (node.nextLeaf == 0) break;
        auto next = readNode(node.nextLeaf);
        if (!next) return {};
        node = std::move(*next);
    }
    return results;
}

bool BPTree::insertMulti(const std::string& key, int64_t value) {
    if (!bp_ || !bp_->isOpen()) return false;
    if (header_.rootPage == 0) {
        uint32_t root = allocPage();
        if (root == 0) return false;
        Node node;
        node.isLeaf = 1;
        node.numKeys = 1;
        node.keys.push_back(key);
        node.values.push_back(value);
        node.nextLeaf = 0;
        if (!writeNode(root, node)) return false;
        header_.rootPage = root;
        return writeHeader();
    }
    auto rootOpt = readNode(header_.rootPage);
    if (!rootOpt) return false;
    Node root = std::move(*rootOpt);
    if (root.numKeys == header_.order) {
        const uint32_t oldRoot = header_.rootPage;
        uint32_t newRoot = allocPage();
        if (newRoot == 0) return false;
        Node nr;
        nr.isLeaf = 0;
        nr.numKeys = 0;
        nr.children.push_back(oldRoot);
        if (!writeNode(newRoot, nr)) return false;
        if (!splitChild(newRoot, 0, oldRoot)) return false;
        header_.rootPage = newRoot;
        if (!writeHeader()) return false;
    }
    return insertNonFull(header_.rootPage, key, value);
}

bool BPTree::insertNonFull(uint32_t pageNum, const std::string& key, int64_t value) {
    auto nodeOpt = readNode(pageNum);
    if (!nodeOpt) return false;
    Node node = std::move(*nodeOpt);
    if (node.isLeaf) {
        // Insert into leaf in sorted order
        size_t i = 0;
        while (i < node.numKeys && key > node.keys[i]) ++i;
        node.keys.insert(node.keys.begin() + i, key);
        node.values.insert(node.values.begin() + i, value);
        node.numKeys++;
        return writeNode(pageNum, node);
    }
    // Find child
    size_t i = 0;
    while (i < node.numKeys && key >= node.keys[i]) ++i;
    if (i >= node.children.size()) return false;
    uint32_t childPage = node.children[i];
    auto childOpt = readNode(childPage);
    if (!childOpt) return false;
    Node child = std::move(*childOpt);
    if (child.numKeys == header_.order) {
        if (!splitChild(pageNum, static_cast<int>(i), childPage)) return false;
        auto parentOpt = readNode(pageNum);
        if (!parentOpt) return false;
        node = std::move(*parentOpt);
        if (i >= node.children.size()) return false;
        if (key >= node.keys[i]) ++i;
        if (i >= node.children.size()) return false;
        childPage = node.children[i];
    }
    return insertNonFull(childPage, key, value);
}

bool BPTree::splitChild(uint32_t parentPage, int childIdx, uint32_t childPage) {
    auto parentOpt = readNode(parentPage);
    auto childOpt = readNode(childPage);
    if (!parentOpt || !childOpt || childIdx < 0 ||
        static_cast<size_t>(childIdx) > parentOpt->numKeys) return false;
    Node parent = std::move(*parentOpt);
    Node child = std::move(*childOpt);
    uint32_t newPage = allocPage();
    if (newPage == 0 || child.numKeys < 2) return false;
    Node newNode;
    newNode.isLeaf = child.isLeaf;

    size_t mid = header_.order / 2;
    // Leaf separators are the first key of the right leaf, so the median
    // must remain in the right leaf. Internal separators are promoted and
    // removed from both child key arrays.
    const size_t rightBegin = child.isLeaf ? mid : mid + 1;
    for (size_t i = rightBegin; i < child.numKeys; ++i) {
        newNode.keys.push_back(child.keys[i]);
    }
    if (child.isLeaf) {
        for (size_t i = rightBegin; i < child.numKeys; ++i) {
            newNode.values.push_back(child.values[i]);
        }
        newNode.nextLeaf = child.nextLeaf;
        child.nextLeaf = newPage;
    } else {
        for (size_t i = mid + 1; i <= child.numKeys; ++i) {
            newNode.children.push_back(child.children[i]);
        }
    }
    newNode.numKeys = static_cast<uint16_t>(child.numKeys - rightBegin);

    // Move parent's keys and children to make room
    std::string midKey = child.keys[mid];
    parent.keys.insert(parent.keys.begin() + childIdx, midKey);
    parent.children.insert(parent.children.begin() + childIdx + 1, newPage);
    parent.numKeys++;

    // Truncate child
    child.numKeys = static_cast<uint16_t>(mid);
    child.keys.resize(mid);
    if (child.isLeaf) {
        child.values.resize(mid);
    } else {
        child.children.resize(mid + 1);
    }

    if (!writeNode(childPage, child)) return false;
    if (!writeNode(newPage, newNode)) return false;
    return writeNode(parentPage, parent);
}

// ========================================================================
// Remove (simplified: no merging, just remove key from leaf)
// ========================================================================
bool BPTree::remove(const std::string& key) {
    if (!bp_ || !bp_->isOpen() || header_.rootPage == 0) return false;
    return removeFromNode(header_.rootPage, key);
}

bool BPTree::removeFromNode(uint32_t pageNum, const std::string& key) {
    auto nodeOpt = readNode(pageNum);
    if (!nodeOpt) return false;
    Node node = std::move(*nodeOpt);
    if (node.isLeaf) {
        for (size_t i = 0; i < node.numKeys; ++i) {
            if (node.keys[i] == key) {
                node.keys.erase(node.keys.begin() + i);
                node.values.erase(node.values.begin() + i);
                node.numKeys--;
                return writeNode(pageNum, node);
            }
        }
        return false;
    }
    size_t i = 0;
    while (i < node.numKeys && key >= node.keys[i]) ++i;
    if (i < node.children.size()) {
        bool removed = removeFromNode(node.children[i], key);
        if (removed) {
            // Update key if first key of child changed (simplified)
            // For a full implementation we'd handle underflow here
        }
        return removed;
    }
    return false;
}

// ========================================================================
// Range scan
// ========================================================================
std::vector<int64_t> BPTree::rangeScan(const std::string& startKey, const std::string& endKey) const {
    std::vector<int64_t> result;
    if (!bp_ || !bp_->isOpen() || header_.rootPage == 0) return result;
    if (!collectRange(header_.rootPage, startKey, endKey, result)) return {};
    return result;
}

bool BPTree::collectRange(uint32_t pageNum, const std::string& startKey,
                          const std::string& endKey, std::vector<int64_t>& out) const {
    auto nodeOpt = readNode(pageNum);
    if (!nodeOpt) return false;
    Node node = std::move(*nodeOpt);
    if (node.isLeaf) {
        for (size_t i = 0; i < node.numKeys; ++i) {
            if (node.keys[i] >= startKey && node.keys[i] <= endKey) {
                out.push_back(node.values[i]);
            }
        }
        return true;
    }
    // Internal node: visit all children that might contain keys in range
    for (size_t i = 0; i < node.children.size(); ++i) {
        if (!collectRange(node.children[i], startKey, endKey, out)) return false;
    }
    return true;
}

std::vector<int64_t> BPTree::allValues() const {
    return rangeScan("", std::string(BP_KEY_LEN, '\xFF'));
}

} // namespace dbms
