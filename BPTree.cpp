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
void BPTree::serializeNode(char* buf, const Node& node, uint16_t order) {
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

void BPTree::deserializeNode(const char* buf, Node& node, uint16_t order) {
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
BPTree::BPTree(const std::filesystem::path& indexFile) : filePath_(indexFile) {}

BPTree::~BPTree() {
    close();
}

bool BPTree::open() {
    if (fd_ >= 0) return true;
    bool exists = std::filesystem::exists(filePath_);
    fd_ = ::open(filePath_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) return false;

    if (exists) {
        readHeader();
    } else {
        header_.rootPage = 0;
        header_.nextFreePage = 1;
        // Calculate order based on page size
        // Leaf: 3 + order*BP_KEY_LEN + order*8 + 4 <= BP_PAGE_SIZE
        // Internal: 3 + order*BP_KEY_LEN + (order+1)*4 <= BP_PAGE_SIZE
        size_t maxLeaf = (BP_PAGE_SIZE - 7) / (BP_KEY_LEN + 8);
        size_t maxInternal = (BP_PAGE_SIZE - 7 - 4) / (BP_KEY_LEN + 4);
        header_.order = static_cast<uint16_t>(std::min(size_t(100), std::min(maxLeaf, maxInternal)));
        if (header_.order < 2) header_.order = 2;
        writeHeader();
    }
    return true;
}

void BPTree::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void BPTree::writeHeader() {
    char buf[BP_PAGE_SIZE] = {};
    std::memcpy(buf, &header_, sizeof(FileHeader));
    ::lseek(fd_, 0, SEEK_SET);
    ::write(fd_, buf, BP_PAGE_SIZE);
}

void BPTree::readHeader() {
    char buf[BP_PAGE_SIZE] = {};
    ::lseek(fd_, 0, SEEK_SET);
    ssize_t n = ::read(fd_, buf, BP_PAGE_SIZE);
    if (n >= static_cast<ssize_t>(sizeof(FileHeader))) {
        std::memcpy(&header_, buf, sizeof(FileHeader));
    }
}

uint32_t BPTree::allocPage() {
    uint32_t page = header_.nextFreePage++;
    writeHeader();
    char buf[BP_PAGE_SIZE] = {};
    ::lseek(fd_, static_cast<off_t>(page) * BP_PAGE_SIZE, SEEK_SET);
    ::write(fd_, buf, BP_PAGE_SIZE);
    return page;
}

void BPTree::writeNode(uint32_t pageNum, const Node& node) {
    char buf[BP_PAGE_SIZE];
    serializeNode(buf, node, header_.order);
    ::lseek(fd_, static_cast<off_t>(pageNum) * BP_PAGE_SIZE, SEEK_SET);
    ::write(fd_, buf, BP_PAGE_SIZE);
}

BPTree::Node BPTree::readNode(uint32_t pageNum) const {
    Node node;
    char buf[BP_PAGE_SIZE];
    ::lseek(fd_, static_cast<off_t>(pageNum) * BP_PAGE_SIZE, SEEK_SET);
    ssize_t n = ::read(fd_, buf, BP_PAGE_SIZE);
    if (n == BP_PAGE_SIZE) {
        deserializeNode(buf, node, header_.order);
    }
    return node;
}

// ========================================================================
// Search
// ========================================================================
bool BPTree::search(const std::string& key, int64_t& value) const {
    if (fd_ < 0 || header_.rootPage == 0) return false;
    return searchNode(header_.rootPage, key, value);
}

bool BPTree::searchNode(uint32_t pageNum, const std::string& key, int64_t& value) const {
    Node node = readNode(pageNum);
    std::string k = normalizeKey(key);
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
    if (fd_ < 0) return false;
    if (header_.rootPage == 0) {
        // Create root leaf
        uint32_t root = allocPage();
        Node node;
        node.isLeaf = 1;
        node.numKeys = 1;
        node.keys.push_back(key);
        node.values.push_back(value);
        node.nextLeaf = 0;
        writeNode(root, node);
        header_.rootPage = root;
        writeHeader();
        return true;
    }

    // Check if key already exists
    int64_t dummy;
    if (search(key, dummy)) return false;

    Node root = readNode(header_.rootPage);
    if (root.numKeys == header_.order) {
        // Split root
        uint32_t newRoot = allocPage();
        Node nr;
        nr.isLeaf = 0;
        nr.numKeys = 0;
        nr.children.push_back(header_.rootPage);
        writeNode(newRoot, nr);
        header_.rootPage = newRoot;
        writeHeader();
        splitChild(newRoot, 0, root.numKeys > 0 ? root.children[0] : 0);
        // Re-read root after split
    }
    return insertNonFull(header_.rootPage, key, value);
}

bool BPTree::insertNonFull(uint32_t pageNum, const std::string& key, int64_t value) {
    Node node = readNode(pageNum);
    if (node.isLeaf) {
        // Insert into leaf in sorted order
        size_t i = 0;
        while (i < node.numKeys && key > node.keys[i]) ++i;
        node.keys.insert(node.keys.begin() + i, key);
        node.values.insert(node.values.begin() + i, value);
        node.numKeys++;
        writeNode(pageNum, node);
        return true;
    }
    // Find child
    size_t i = 0;
    while (i < node.numKeys && key >= node.keys[i]) ++i;
    uint32_t childPage = node.children[i];
    Node child = readNode(childPage);
    if (child.numKeys == header_.order) {
        splitChild(pageNum, static_cast<int>(i), childPage);
        node = readNode(pageNum);
        if (key > node.keys[i]) ++i;
        childPage = node.children[i];
    }
    return insertNonFull(childPage, key, value);
}

void BPTree::splitChild(uint32_t parentPage, int childIdx, uint32_t childPage) {
    Node parent = readNode(parentPage);
    Node child = readNode(childPage);
    uint32_t newPage = allocPage();
    Node newNode;
    newNode.isLeaf = child.isLeaf;

    size_t mid = header_.order / 2;
    // New node gets keys [mid+1, order-1]
    for (size_t i = mid + 1; i < child.numKeys; ++i) {
        newNode.keys.push_back(child.keys[i]);
    }
    if (child.isLeaf) {
        for (size_t i = mid + 1; i < child.numKeys; ++i) {
            newNode.values.push_back(child.values[i]);
        }
        newNode.nextLeaf = child.nextLeaf;
        child.nextLeaf = newPage;
    } else {
        for (size_t i = mid + 1; i <= child.numKeys; ++i) {
            newNode.children.push_back(child.children[i]);
        }
    }
    newNode.numKeys = child.numKeys - mid - 1;

    // Move parent's keys and children to make room
    std::string midKey = child.keys[mid];
    parent.keys.insert(parent.keys.begin() + childIdx, midKey);
    parent.children.insert(parent.children.begin() + childIdx + 1, newPage);
    parent.numKeys++;

    // Truncate child
    child.numKeys = mid;
    child.keys.resize(mid);
    if (child.isLeaf) {
        child.values.resize(mid);
    } else {
        child.children.resize(mid + 1);
    }

    writeNode(parentPage, parent);
    writeNode(childPage, child);
    writeNode(newPage, newNode);
}

// ========================================================================
// Remove (simplified: no merging, just remove key from leaf)
// ========================================================================
bool BPTree::remove(const std::string& key) {
    if (fd_ < 0 || header_.rootPage == 0) return false;
    return removeFromNode(header_.rootPage, key);
}

bool BPTree::removeFromNode(uint32_t pageNum, const std::string& key) {
    Node node = readNode(pageNum);
    if (node.isLeaf) {
        for (size_t i = 0; i < node.numKeys; ++i) {
            if (node.keys[i] == key) {
                node.keys.erase(node.keys.begin() + i);
                node.values.erase(node.values.begin() + i);
                node.numKeys--;
                writeNode(pageNum, node);
                return true;
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
    if (fd_ < 0 || header_.rootPage == 0) return result;
    collectRange(header_.rootPage, startKey, endKey, result);
    return result;
}

void BPTree::collectRange(uint32_t pageNum, const std::string& startKey,
                          const std::string& endKey, std::vector<int64_t>& out) const {
    Node node = readNode(pageNum);
    if (node.isLeaf) {
        for (size_t i = 0; i < node.numKeys; ++i) {
            if (node.keys[i] >= startKey && node.keys[i] <= endKey) {
                out.push_back(node.values[i]);
            }
        }
        if (node.nextLeaf > 0) {
            collectRange(node.nextLeaf, startKey, endKey, out);
        }
        return;
    }
    // Internal node: visit all children that might contain keys in range
    for (size_t i = 0; i < node.children.size(); ++i) {
        collectRange(node.children[i], startKey, endKey, out);
    }
}

std::vector<int64_t> BPTree::allValues() const {
    return rangeScan("", std::string(BP_KEY_LEN, '\xFF'));
}

} // namespace dbms
