#include "BPTree.h"

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <limits>

namespace dbms {

static_assert(BP_KEY_LEN >= 4, "BP_KEY_LEN too small");

namespace {
constexpr size_t kMaxLeafKeys = bptree_format::kMaxLeafKeys;
constexpr size_t kMaxInternalKeys = bptree_format::kMaxInternalKeys;
constexpr size_t kMaxNodeOrder =
    kMaxLeafKeys < kMaxInternalKeys ? kMaxLeafKeys : kMaxInternalKeys;
constexpr uint16_t kLegacyChecksummedIndexFormat =
    bptree_format::kContentChecksumFormat;
constexpr size_t kPageChecksumOffset = bptree_format::kChecksumOffset;
constexpr size_t kIntegerKeyBytes = sizeof(uint64_t);
constexpr uint64_t kIntegerSignBias = uint64_t{1} << 63;
static_assert(BP_PAGE_SIZE == bptree_format::kPageSize);
static_assert(BP_KEY_LEN == bptree_format::kKeySize);
static_assert(kMaxNodeOrder >= 2 && kMaxNodeOrder <= UINT16_MAX,
              "invalid B+ tree page layout");
static_assert(3 + kMaxLeafKeys * (BP_KEY_LEN + sizeof(int64_t)) +
                      sizeof(uint32_t) <= kPageChecksumOffset,
              "B+ tree leaf payload overlaps the page checksum");

void writeIndexPageChecksum(char* page, uint32_t pageId) {
    bptree_format::writePageChecksum(page, pageId);
}

bool verifyIndexPageChecksum(const char* page, uint32_t pageId, bool bindPageId) {
    return bptree_format::verifyPageChecksum(page, pageId, bindPageId);
}

bool validIntegerStoredKey(const std::string& key) {
    return key.size() == BP_KEY_LEN &&
           std::all_of(key.begin() + kIntegerKeyBytes, key.end(),
                       [](char byte) { return byte == '\0'; });
}

int64_t readIntegerStoredKey(const std::string& key, size_t offset = 0) {
    uint64_t biased = 0;
    for (size_t i = 0; i < kIntegerKeyBytes; ++i) {
        biased = (biased << 8) | static_cast<unsigned char>(key[offset + i]);
    }
    if (biased < kIntegerSignBias) {
        return std::numeric_limits<int64_t>::min() + static_cast<int64_t>(biased);
    }
    return static_cast<int64_t>(biased - kIntegerSignBias);
}

std::string storeIntegerKey(int64_t value) {
    uint64_t biased = static_cast<uint64_t>(value) ^ kIntegerSignBias;
    std::string stored(BP_KEY_LEN, '\0');
    auto* bytes = reinterpret_cast<unsigned char*>(stored.data());
    for (size_t i = kIntegerKeyBytes; i > 0; --i) {
        bytes[i - 1] = static_cast<unsigned char>(biased);
        biased >>= 8;
    }
    return stored;
}
}  // namespace

// Index buffer-pool frame count. Overridable with DBMS_INDEX_BUFFER_FRAMES.
static size_t indexBufferFrameCount() {
    static const size_t frames = [] {
        const char* env = std::getenv("DBMS_INDEX_BUFFER_FRAMES");
        if (env && *env) {
            char* end = nullptr;
            const long value = std::strtol(env, &end, 10);
            if (end && *end == '\0' && value >= 16 && value <= 4096) {
                return static_cast<size_t>(value);
            }
        }
        return static_cast<size_t>(128);
    }();
    return frames;
}

// ========================================================================
// Key normalization: pad with '\0' to fixed length
// ========================================================================
std::string BPTree::normalizeKey(const std::string& s) {
    std::string k = s;
    if (k.size() > BP_KEY_LEN) k.resize(BP_KEY_LEN);
    else if (k.size() < BP_KEY_LEN) k.append(BP_KEY_LEN - k.size(), '\0');
    return k;
}

std::string BPTree::normalizeKeyForComparison(const std::string& key) {
    return normalizeKey(key);
}

std::optional<std::string> BPTree::keyForStorage(const std::string& key) const {
    if (header_.reserved == bptree_format::kCompositePrimaryFingerprintFormat ||
        header_.reserved == bptree_format::kCompositePrimaryRangeFormat) {
        if (key.size() != BP_KEY_LEN) return std::nullopt;
        return key;
    }
    if (header_.reserved != bptree_format::kSignedIntegerKeyFormat) {
        return normalizeKey(key);
    }
    const char* first = key.data();
    const char* last = first + key.size();
    // TableSchema appends one separator after an explicit primary key.
    if (first != last && last[-1] == '\x01') --last;
    if (first == last) return std::nullopt;
    if (*first == '+') {
        ++first;
        if (first == last || *first < '0' || *first > '9') return std::nullopt;
    }
    int64_t value = 0;
    const auto parsed = std::from_chars(first, last, value);
    if (parsed.ec != std::errc{} || parsed.ptr != last) return std::nullopt;
    return storeIntegerKey(value);
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
        // The node already contains encoded bytes, including separators
        // copied from child nodes during splitting.
        std::memcpy(buf + pos, node.keys[i].data(), BP_KEY_LEN);
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

bool BPTree::deserializeNode(const char* buf, Node& node, uint16_t order) {
    node.isLeaf = static_cast<uint8_t>(buf[0]);
    std::memcpy(&node.numKeys, buf + 1, sizeof(uint16_t));
    if (node.isLeaf > 1 || node.numKeys > order ||
        node.numKeys > (node.isLeaf ? kMaxLeafKeys : kMaxInternalKeys)) {
        return false;
    }
    node.keys.clear();
    node.values.clear();
    node.children.clear();
    size_t pos = 3;
    for (size_t i = 0; i < node.numKeys; ++i) {
        // Keep keys fixed-width in memory as well as on disk.  Public
        // operations normalize their input before descending the tree;
        // retaining the padding here makes comparisons, separators, and
        // range boundaries use one ordering after a reopen.
        node.keys.emplace_back(buf + pos, BP_KEY_LEN);
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
    return std::is_sorted(node.keys.begin(), node.keys.end());
}

// ========================================================================
// File I/O
// ========================================================================
BPTree::BPTree(const std::filesystem::path& indexFile)
    : filePath_(indexFile),
      bp_(std::make_unique<BufferPool>(
          indexFile.string(), indexBufferFrameCount(), BP_PAGE_SIZE)) {
    bp_->setPageValidator([this](uint32_t pageId, const char* data) {
        if (pageId == 0) return true;
        const bool pageIdBound = bptree_format::usesPageBoundChecksum(header_.reserved);
        if (!pageIdBound && header_.reserved != kLegacyChecksummedIndexFormat) {
            return true;
        }
        if (verifyIndexPageChecksum(data, pageId, pageIdBound)) return true;
        std::cerr << "[index] invalid B+ tree page checksum in " << filePath_
                  << " at block " << pageId << std::endl;
        return false;
    });
}

BPTree::~BPTree() {
    close();
    if (generationFd_ >= 0) ::close(generationFd_);
}

bool BPTree::hasStaleFileGeneration() const {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    if (bp_ && bp_->isOpen() && !bp_->refersToCurrentFiles()) return true;
    if (generationFd_ < 0) return false;
    struct stat opened{}, current{};
    return ::fstat(generationFd_, &opened) != 0 ||
           ::stat(filePath_.c_str(), &current) != 0 ||
           !S_ISREG(current.st_mode) || opened.st_dev != current.st_dev ||
           opened.st_ino != current.st_ino;
}

bool BPTree::open() {
    return openInternal(true);
}

bool BPTree::openWithSignedIntegerKeys() {
    return openInternal(true, bptree_format::kSignedIntegerKeyFormat);
}

bool BPTree::openWithCompletePrimaryPrefixes() {
    return openInternal(true, bptree_format::kCompletePrimaryPrefixesFormat);
}

bool BPTree::openWithCompositePrimaryFingerprints() {
    return openInternal(true, bptree_format::kCompositePrimaryFingerprintFormat);
}

bool BPTree::openWithCompositePrimaryRanges() {
    return openInternal(true, bptree_format::kCompositePrimaryRangeFormat);
}

bool BPTree::openExisting() {
    return openInternal(false);
}

bool BPTree::usesSignedIntegerKeys() const {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    return bp_ && bp_->isOpen() &&
           header_.reserved == bptree_format::kSignedIntegerKeyFormat;
}

bool BPTree::hasCompletePrimaryKeys() const {
    std::shared_lock<std::shared_mutex> lock(treeMutex_);
    return bp_ && bp_->isOpen() &&
        (header_.reserved == bptree_format::kCompletePrimaryPrefixesFormat ||
         header_.reserved == bptree_format::kCompositePrimaryFingerprintFormat ||
         header_.reserved == bptree_format::kCompositePrimaryRangeFormat);
}

bool BPTree::usesCompositePrimaryFingerprints() const {
    std::shared_lock<std::shared_mutex> lock(treeMutex_);
    return bp_ && bp_->isOpen() &&
        (header_.reserved == bptree_format::kCompositePrimaryFingerprintFormat ||
         header_.reserved == bptree_format::kCompositePrimaryRangeFormat);
}

bool BPTree::usesCompositePrimaryRanges() const {
    std::shared_lock<std::shared_mutex> lock(treeMutex_);
    return bp_ && bp_->isOpen() && header_.reserved == bptree_format::kCompositePrimaryRangeFormat;
}

bool BPTree::openInternal(bool createIfMissing, uint16_t creationFormat) {
    std::unique_lock<std::shared_mutex> treeLock(treeMutex_);
    if (bp_ && bp_->isOpen()) return true;
    if (!bp_) bp_ = std::make_unique<BufferPool>(filePath_.string(), indexBufferFrameCount());
    // A (re)open may see a different on-disk tree (e.g. after clear or
    // external rebuild); never serve nodes cached from the previous open.
    {
        std::unique_lock<std::shared_mutex> lock(nodeCacheMutex_);
        nodeCache_.clear();
        nodeCacheOrder_.clear();
    }

    // Check existence BEFORE opening (O_CREAT would create the file)
    bool exists = std::filesystem::exists(filePath_);
    if (!createIfMissing && !exists) return false;
    if (!(createIfMissing ? bp_->open() : bp_->openExisting())) return false;

    if (exists) {
        if (!readHeader()) {
            bp_->close();
            return false;
        }
        if (header_.rootPage != 0 && !readNode(header_.rootPage)) {
            bp_->close();
            return false;
        }
    } else {
        header_.rootPage = 0;
        header_.nextFreePage = 1;
        header_.order = static_cast<uint16_t>(std::min(size_t(100), kMaxNodeOrder));
        if (header_.order < 2) header_.order = 2;
        header_.reserved = creationFormat;
        if (!writeHeader()) {
            bp_->close();
            return false;
        }
    }
    const int generation = ::open(filePath_.c_str(), O_RDONLY | O_CLOEXEC);
    struct stat opened{}, current{};
    if (generation < 0 || ::fstat(generation, &opened) != 0 ||
        ::stat(filePath_.c_str(), &current) != 0 ||
        !S_ISREG(current.st_mode) || opened.st_dev != current.st_dev ||
        opened.st_ino != current.st_ino || !bp_->refersToCurrentFiles()) {
        if (generation >= 0) ::close(generation);
        bp_->close();
        return false;
    }
    if (generationFd_ >= 0) ::close(generationFd_);
    generationFd_ = generation;
    return true;
}

void BPTree::close() {
    std::unique_lock<std::shared_mutex> treeLock(treeMutex_);
    {
        std::unique_lock<std::shared_mutex> lock(nodeCacheMutex_);
        nodeCache_.clear();
        nodeCacheOrder_.clear();
    }
    if (bp_) {
        bp_->close();
    }
}

bool BPTree::flush() {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    return bp_ && bp_->isOpen() && bp_->flush();
}

bool BPTree::writeHeader() {
    if (!bp_ || !bp_->isOpen()) return false;
    char* buf = bp_->fetchPage(0);
    if (!buf) return false;
    std::memcpy(buf, &header_, sizeof(FileHeader));
    if (bptree_format::usesPageBoundChecksum(header_.reserved)) {
        writeIndexPageChecksum(buf, 0);
    }
    bp_->markDirty(0);
    bp_->unpinPage(0);
    return true;
}

bool BPTree::readHeader() {
    if (!bp_ || !bp_->isOpen()) return false;
    std::error_code ec;
    const uintmax_t fileSize = std::filesystem::file_size(filePath_, ec);
    if (ec || fileSize < BP_PAGE_SIZE || fileSize % BP_PAGE_SIZE != 0) return false;
    const uintmax_t pageCount = fileSize / BP_PAGE_SIZE;
    char* buf = bp_->fetchPage(0);
    if (!buf) return false;
    std::memcpy(&header_, buf, sizeof(FileHeader));
    bool headerChecksumValid = false;
    if (bptree_format::usesPageBoundChecksum(header_.reserved)) {
        headerChecksumValid = verifyIndexPageChecksum(buf, 0, true);
    } else if (header_.reserved == kLegacyChecksummedIndexFormat) {
        headerChecksumValid = verifyIndexPageChecksum(buf, 0, false);
    } else if (header_.reserved == 0) {
        uint32_t legacyTrailer = 0;
        std::memcpy(&legacyTrailer, buf + kPageChecksumOffset,
                    sizeof(legacyTrailer));
        headerChecksumValid = legacyTrailer == 0;
    }
    bp_->unpinPage(0);
    if (!headerChecksumValid || header_.order < 2 ||
        header_.order > kMaxNodeOrder ||
        header_.nextFreePage < 1 || header_.nextFreePage > pageCount ||
        (header_.rootPage != 0 &&
         (header_.rootPage >= header_.nextFreePage || header_.rootPage >= pageCount))) {
        return false;
    }
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
    if (bptree_format::usesPageBoundChecksum(header_.reserved)) {
        writeIndexPageChecksum(buf, page);
    }
    bp_->markDirty(page);
    bp_->unpinPage(page);
    return page;
}

bool BPTree::writeNode(uint32_t pageNum, const Node& node) {
    if (!bp_ || !bp_->isOpen() || pageNum == 0 ||
        pageNum >= header_.nextFreePage || node.isLeaf > 1 ||
        node.numKeys != node.keys.size() || node.numKeys > header_.order ||
        !std::is_sorted(node.keys.begin(), node.keys.end()) ||
        !std::all_of(node.keys.begin(), node.keys.end(),
                     [](const std::string& key) { return key.size() == BP_KEY_LEN; })) {
        return false;
    }
    if ((node.isLeaf &&
         (node.numKeys > kMaxLeafKeys || node.values.size() != node.numKeys ||
          (node.nextLeaf != 0 && node.nextLeaf >= header_.nextFreePage))) ||
        (!node.isLeaf &&
         (node.numKeys > kMaxInternalKeys ||
          node.children.size() != static_cast<size_t>(node.numKeys) + 1))) {
        return false;
    }
    char* buf = bp_->fetchPage(pageNum);
    if (!buf) return false;
    serializeNode(buf, node, header_.order);
    if (bptree_format::usesPageBoundChecksum(header_.reserved)) {
        writeIndexPageChecksum(buf, pageNum);
    }
    bp_->markDirty(pageNum);
    bp_->unpinPage(pageNum);
    cacheNode(pageNum, node);
    return true;
}

// ---- Node cache --------------------------------------------------------
std::shared_ptr<const BPTree::Node> BPTree::cachedNode(uint32_t pageNum) const {
    {
        std::shared_lock<std::shared_mutex> lock(nodeCacheMutex_);
        auto it = nodeCache_.find(pageNum);
        if (it == nodeCache_.end()) return nullptr;
        // Promote recency under the exclusive lock (small critical section;
        // the find stays shared).
        lock.unlock();
        std::unique_lock<std::shared_mutex> ulock(nodeCacheMutex_);
        auto it2 = nodeCache_.find(pageNum);
        if (it2 == nodeCache_.end()) return nullptr;
        for (auto q = nodeCacheOrder_.begin(); q != nodeCacheOrder_.end(); ++q) {
            if (*q == pageNum) {
                nodeCacheOrder_.erase(q);
                break;
            }
        }
        nodeCacheOrder_.push_front(pageNum);
        return it2->second;
    }
}

void BPTree::cacheNode(uint32_t pageNum, const Node& node) const {
    std::unique_lock<std::shared_mutex> lock(nodeCacheMutex_);
    auto it = nodeCache_.find(pageNum);
    if (it != nodeCache_.end()) {
        it->second = std::make_shared<const Node>(node);
        for (auto q = nodeCacheOrder_.begin(); q != nodeCacheOrder_.end(); ++q) {
            if (*q == pageNum) {
                nodeCacheOrder_.erase(q);
                break;
            }
        }
        nodeCacheOrder_.push_front(pageNum);
        return;
    }
    if (nodeCache_.size() >= kNodeCacheCapacity) {
        const uint32_t victim = nodeCacheOrder_.back();
        nodeCacheOrder_.pop_back();
        nodeCache_.erase(victim);
    }
    nodeCache_.emplace(pageNum, std::make_shared<const Node>(node));
    nodeCacheOrder_.push_front(pageNum);
}

void BPTree::dropNodeFromCache(uint32_t pageNum) {
    std::unique_lock<std::shared_mutex> lock(nodeCacheMutex_);
    nodeCache_.erase(pageNum);
    for (auto q = nodeCacheOrder_.begin(); q != nodeCacheOrder_.end(); ++q) {
        if (*q == pageNum) {
            nodeCacheOrder_.erase(q);
            break;
        }
    }
}

std::optional<BPTree::Node> BPTree::readNode(uint32_t pageNum) const {
    if (!bp_ || !bp_->isOpen() || pageNum == 0 ||
        pageNum >= header_.nextFreePage) {
        return std::nullopt;
    }
    // Hot nodes (root, upper internal levels) are hit by nearly every
    // descent; serving them from the node cache skips the whole-page
    // deserialize (order-many heap allocations) per level.
    if (auto cached = cachedNode(pageNum)) return *cached;
    Node node;
    char* buf = const_cast<BufferPool*>(bp_.get())->fetchPage(pageNum);
    if (!buf) return std::nullopt;
    const bool valid = deserializeNode(buf, node, header_.order);
    bp_->unpinPage(pageNum);
    if (!valid ||
        (header_.reserved == bptree_format::kSignedIntegerKeyFormat &&
         !std::all_of(node.keys.begin(), node.keys.end(), validIntegerStoredKey))) {
        return std::nullopt;
    }
    if (node.isLeaf) {
        if (node.nextLeaf != 0 &&
            (node.nextLeaf >= header_.nextFreePage || node.nextLeaf == pageNum)) {
            return std::nullopt;
        }
    } else {
        for (const uint32_t child : node.children) {
            if (child == 0 || child >= header_.nextFreePage || child == pageNum) {
                return std::nullopt;
            }
        }
    }
    cacheNode(pageNum, node);
    return node;
}

// ========================================================================
// Search
// ========================================================================
namespace {
// Node key arrays are sorted; every descent previously walked them linearly
// (up to `order` comparisons per level). Fixed-width normalized keys compare
// directly as byte strings, so binary search applies.
// lowerBound returns the first index whose key is >= `key`.
size_t lowerBoundIdx(const std::vector<std::string>& keys, size_t count,
                     const std::string& key) {
    size_t lo = 0;
    size_t hi = count;
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (keys[mid] < key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}
// upperBound returns the first index whose key is > `key`.
size_t upperBoundIdx(const std::vector<std::string>& keys, size_t count,
                     const std::string& key) {
    size_t lo = 0;
    size_t hi = count;
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (keys[mid] <= key) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}
} // namespace

bool BPTree::search(const std::string& key, int64_t& value) const {
    return searchChecked(key, value) == SearchResult::Found;
}

BPTree::SearchResult BPTree::searchChecked(
        const std::string& key, int64_t& value) const {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    if (!bp_ || !bp_->isOpen()) return SearchResult::Error;
    const auto storedKey = keyForStorage(key);
    // A fractional or out-of-range equality probe cannot match an integer.
    if (!storedKey) return SearchResult::NotFound;
    if (header_.rootPage == 0) return SearchResult::NotFound;
    TraversalPath visited;
    return searchNode(header_.rootPage, *storedKey, value, visited);
}

BPTree::SearchResult BPTree::searchNode(
        uint32_t pageNum, const std::string& key, int64_t& value,
        TraversalPath& visited) const {
    if (!visited.visit(pageNum)) return SearchResult::Error;
    auto nodeOpt = readNode(pageNum);
    if (!nodeOpt) return SearchResult::Error;
    Node node = std::move(*nodeOpt);
    if (node.isLeaf) {
        const size_t i = lowerBoundIdx(node.keys, node.numKeys, key);
        if (i < node.numKeys && node.keys[i] == key) {
            value = node.values[i];
            return SearchResult::Found;
        }
        return SearchResult::NotFound;
    }
    // Internal node: separators are the smallest key of the right subtree,
    // so an equal key lives to the right — descend past every separator
    // that is <= key (upper bound).
    const size_t i = upperBoundIdx(node.keys, node.numKeys, key);
    if (i < node.children.size()) {
        return searchNode(node.children[i], key, value, visited);
    }
    return SearchResult::Error;
}

// ========================================================================
// Insert
// ========================================================================
bool BPTree::insert(const std::string& key, int64_t value) {
    std::unique_lock<std::shared_mutex> treeLock(treeMutex_);
    if (!bp_ || !bp_->isOpen()) return false;
    const auto storedKey = keyForStorage(key);
    if (!storedKey) return false;
    const std::string& normalizedKey = *storedKey;
    if (header_.rootPage == 0) {
        // Create root leaf
        uint32_t root = allocPage();
        if (root == 0) return false;
        Node node;
        node.isLeaf = 1;
        node.numKeys = 1;
        node.keys.push_back(normalizedKey);
        node.values.push_back(value);
        node.nextLeaf = 0;
        if (!writeNode(root, node)) return false;
        header_.rootPage = root;
        return writeHeader();
    }

    // Check if key already exists
    int64_t dummy;
    TraversalPath visited;
    const SearchResult existing = searchNode(header_.rootPage, normalizedKey,
                                             dummy, visited);
    if (existing != SearchResult::NotFound) return false;

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
    visited.clear();
    return insertNonFull(header_.rootPage, normalizedKey, value, visited);
}

// ========================================================================
// Multi-value search (for secondary indexes with duplicate keys)
// ========================================================================
std::vector<int64_t> BPTree::searchMulti(const std::string& key) const {
    std::vector<int64_t> results;
    if (!searchMultiChecked(key, results)) return {};
    return results;
}

bool BPTree::searchMultiChecked(const std::string& key,
                               std::vector<int64_t>& values) const {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    values.clear();
    std::vector<int64_t> results;
    if (!bp_ || !bp_->isOpen()) return false;
    const auto storedKey = keyForStorage(key);
    if (!storedKey || header_.rootPage == 0) return true;
    const std::string& normalizedKey = *storedKey;
    std::unordered_set<uint32_t> visited;
    auto nodeOpt = readNode(header_.rootPage);
    if (!nodeOpt) return false;
    visited.insert(header_.rootPage);
    Node node = std::move(*nodeOpt);
    while (!node.isLeaf) {
        // Descend to the leftmost possible leaf for this key.  Equal keys
        // may span multiple leaves after a split; using >= here would start
        // at the rightmost equal separator and silently miss earlier rows.
        const size_t i = lowerBoundIdx(node.keys, node.numKeys, normalizedKey);
        if (i >= node.children.size()) return false;
        const uint32_t nextPage = node.children[i];
        if (!visited.insert(nextPage).second) return false;
        auto next = readNode(nextPage);
        if (!next) return false;
        node = std::move(*next);
    }
    // Equal keys can continue through empty leaves or across a split, but
    // once a greater key is reached the rest of the tree cannot match.
    while (true) {
        const size_t first = lowerBoundIdx(node.keys, node.numKeys, normalizedKey);
        const size_t last = upperBoundIdx(node.keys, node.numKeys, normalizedKey);
        results.insert(results.end(), node.values.begin() + first,
                        node.values.begin() + last);
        if (last < node.numKeys || node.nextLeaf == 0) break;
        if (!visited.insert(node.nextLeaf).second) return false;
        auto next = readNode(node.nextLeaf);
        if (!next || !next->isLeaf) return false;
        if (!node.keys.empty() && !next->keys.empty() &&
            node.keys.back() > next->keys.front()) return false;
        node = std::move(*next);
    }
    values = std::move(results);
    return true;
}

bool BPTree::insertMulti(const std::string& key, int64_t value) {
    std::unique_lock<std::shared_mutex> treeLock(treeMutex_);
    if (!bp_ || !bp_->isOpen()) return false;
    const auto storedKey = keyForStorage(key);
    if (!storedKey) return false;
    const std::string& normalizedKey = *storedKey;
    if (header_.rootPage == 0) {
        uint32_t root = allocPage();
        if (root == 0) return false;
        Node node;
        node.isLeaf = 1;
        node.numKeys = 1;
        node.keys.push_back(normalizedKey);
        node.values.push_back(value);
        node.nextLeaf = 0;
        if (!writeNode(root, node)) return false;
        header_.rootPage = root;
        return writeHeader();
    }
    int64_t ignored = 0;
    TraversalPath visited;
    if (searchNode(header_.rootPage, normalizedKey, ignored, visited) ==
        SearchResult::Error) {
        return false;
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
    visited.clear();
    return insertNonFull(header_.rootPage, normalizedKey, value, visited);
}

bool BPTree::insertNonFull(uint32_t pageNum, const std::string& key, int64_t value,
                           TraversalPath& visited) {
    if (!visited.visit(pageNum)) return false;
    auto nodeOpt = readNode(pageNum);
    if (!nodeOpt) return false;
    Node node = std::move(*nodeOpt);
    if (node.isLeaf) {
        // Insert into leaf in sorted order: first key >= `key`.
        const size_t i = lowerBoundIdx(node.keys, node.numKeys, key);
        node.keys.insert(node.keys.begin() + i, key);
        node.values.insert(node.values.begin() + i, value);
        node.numKeys++;
        return writeNode(pageNum, node);
    }
    // Find child: separators <= key point right (upper bound).
    const size_t i = upperBoundIdx(node.keys, node.numKeys, key);
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
        size_t j = i;
        if (j >= node.children.size()) return false;
        if (key >= node.keys[j]) ++j;
        if (j >= node.children.size()) return false;
        childPage = node.children[j];
    }
    return insertNonFull(childPage, key, value, visited);
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
    std::unique_lock<std::shared_mutex> treeLock(treeMutex_);
    if (!bp_ || !bp_->isOpen() || header_.rootPage == 0) return false;
    const auto storedKey = keyForStorage(key);
    if (!storedKey) return false;
    std::unordered_set<uint32_t> visited;
    return removeFromNode(header_.rootPage, *storedKey, std::nullopt,
                          visited) == RemoveResult::Removed;
}

bool BPTree::removeMulti(const std::string& key, int64_t value) {
    std::unique_lock<std::shared_mutex> treeLock(treeMutex_);
    if (!bp_ || !bp_->isOpen() || header_.rootPage == 0) return false;
    const auto storedKey = keyForStorage(key);
    if (!storedKey) return false;
    std::unordered_set<uint32_t> visited;
    return removeFromNode(header_.rootPage, *storedKey, value,
                          visited) == RemoveResult::Removed;
}

BPTree::RemoveResult BPTree::removeFromNode(
        uint32_t pageNum, const std::string& key,
        const std::optional<int64_t>& value,
        std::unordered_set<uint32_t>& visited) {
    if (!visited.insert(pageNum).second) return RemoveResult::Error;
    auto nodeOpt = readNode(pageNum);
    if (!nodeOpt) return RemoveResult::Error;
    Node node = std::move(*nodeOpt);
    if (node.isLeaf) {
        // Duplicate keys (multi-value indexes) can carry several RIDs; scan
        // forward through every equal key until the requested value matches.
        size_t i = lowerBoundIdx(node.keys, node.numKeys, key);
        while (i < node.numKeys && node.keys[i] == key) {
            if (!value || node.values[i] == *value) {
                node.keys.erase(node.keys.begin() + i);
                node.values.erase(node.values.begin() + i);
                node.numKeys--;
                return writeNode(pageNum, node) ? RemoveResult::Removed
                                                : RemoveResult::Error;
            }
            ++i;
        }
        return RemoveResult::NotFound;
    }
    // Equal separator keys can span multiple leaves after a split. Start at
    // the leftmost possible child and visit every child whose separator is
    // equal to the target, otherwise a duplicate RID could be missed.
    const size_t first = lowerBoundIdx(node.keys, node.numKeys, key);
    for (size_t i = first; i < node.children.size(); ++i) {
        if (i > first && node.keys[i - 1] != key) break;
        const RemoveResult result = removeFromNode(node.children[i], key, value,
                                                   visited);
        if (result != RemoveResult::NotFound) return result;
    }
    return RemoveResult::NotFound;
}

// ========================================================================
// Range scan
// ========================================================================
std::vector<int64_t> BPTree::rangeScan(const std::string& startKey, const std::string& endKey) const {
    std::vector<int64_t> result;
    if (!rangeScanChecked(startKey, endKey, result)) return {};
    return result;
}

bool BPTree::rangeScanChecked(const std::string& startKey,
                             const std::string& endKey,
                             std::vector<int64_t>& values) const {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    values.clear();
    std::vector<int64_t> result;
    if (!bp_ || !bp_->isOpen()) return false;
    auto lower = startKey.empty()
        ? std::optional(std::string(BP_KEY_LEN, '\0')) : keyForStorage(startKey);
    auto upper = endKey.size() == BP_KEY_LEN &&
                 std::all_of(endKey.begin(), endKey.end(),
                             [](char byte) { return byte == '\xFF'; })
        ? std::optional(endKey) : keyForStorage(endKey);
    if (!lower || !upper) return false;
    if (header_.rootPage == 0) return true;
    std::unordered_set<uint32_t> visited;
    if (!collectRange(header_.rootPage, *lower, *upper, result,
                      visited)) return false;
    values = std::move(result);
    return true;
}

bool BPTree::collectRange(uint32_t pageNum, const std::string& startKey,
                          const std::string& endKey, std::vector<int64_t>& out,
                          std::unordered_set<uint32_t>& visited) const {
    if (!visited.insert(pageNum).second) return false;
    auto nodeOpt = readNode(pageNum);
    if (!nodeOpt) return false;
    Node node = std::move(*nodeOpt);
    if (node.isLeaf) {
        // Keys are sorted: skip everything below startKey in one bound.
        const size_t first = lowerBoundIdx(node.keys, node.numKeys, startKey);
        for (size_t i = first; i < node.numKeys; ++i) {
            if (node.keys[i] > endKey) break;
            out.push_back(node.values[i]);
        }
        return true;
    }
    // Internal node: visit all children that might contain keys in range
    for (size_t i = 0; i < node.children.size(); ++i) {
        if (!collectRange(node.children[i], startKey, endKey, out, visited)) return false;
    }
    return true;
}

std::vector<int64_t> BPTree::allValues() const {
    std::vector<int64_t> result;
    if (!allValuesChecked(result)) return {};
    return result;
}

bool BPTree::allValuesChecked(std::vector<int64_t>& values) const {
    return rangeScanChecked("", std::string(BP_KEY_LEN, '\xFF'), values);
}

BPTree::LeafEntriesResult BPTree::readCompositePrimaryRange(
        const std::string& lower, const std::string& upper, size_t maxRows,
        size_t maxNodes, std::vector<int64_t>& values, std::vector<int64_t>* suffixes,
        CompositeRangeRead mode) const {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    values.clear();
    if (suffixes) suffixes->clear();
    if (!bp_ || !bp_->isOpen() ||
        header_.reserved != bptree_format::kCompositePrimaryRangeFormat ||
        lower.size() != BP_KEY_LEN || upper.size() != BP_KEY_LEN)
        return LeafEntriesResult::Error;
    const bool prefix = mode != CompositeRangeRead::Entire;
    const bool descending = mode == CompositeRangeRead::FirstDescending;
    if (!header_.rootPage || lower > upper || (prefix && maxRows == 0))
        return LeafEntriesResult::Complete;

    std::vector<int64_t> result;
    std::vector<int64_t> resultSuffixes;
    std::unordered_set<uint32_t> visited;
    std::optional<size_t> leafDepth;
    uint32_t lastLeaf = 0;
    uint32_t lastNextLeaf = 0;
    const auto collect = [&](const auto& self, uint32_t page, size_t depth,
                             const std::string* lowerFence, const std::string* upperFence,
                             bool rightEdge) -> LeafEntriesResult {
        if (depth >= kMaxTraversalDepth || visited.count(page))
            return LeafEntriesResult::Error;
        if (visited.size() >= maxNodes) return LeafEntriesResult::LimitExceeded;
        visited.insert(page);
        const auto node = readNode(page);
        if (!node) return LeafEntriesResult::Error;
        // Equal keys may span two children, so both fences are inclusive.
        if (!node->keys.empty() && ((lowerFence && node->keys.front() < *lowerFence) ||
                                   (upperFence && node->keys.back() > *upperFence)))
            return LeafEntriesResult::Error;
        const size_t first = lowerBoundIdx(node->keys, node->numKeys, lower);
        const size_t end = upperBoundIdx(node->keys, node->numKeys, upper);
        if (!node->isLeaf) {
            for (size_t offset = 0; offset <= end - first && (!prefix || result.size() < maxRows);
                 ++offset) {
                const size_t i = descending ? end - offset : first + offset;
                const auto status = self(self, node->children[i], depth + 1,
                    i ? &node->keys[i - 1] : lowerFence,
                    i < node->keys.size() ? &node->keys[i] : upperFence,
                    rightEdge && i + 1 == node->children.size());
                if (status != LeafEntriesResult::Complete) return status;
            }
            return LeafEntriesResult::Complete;
        }
        // In reverse order the current leaf must point to the one just read.
        const bool validLink = descending && lastLeaf
            ? node->nextLeaf == lastLeaf : !visited.count(node->nextLeaf);
        if ((leafDepth && *leafDepth != depth) || !validLink ||
            (!descending && lastLeaf && lastNextLeaf != page) ||
            rightEdge != (node->nextLeaf == 0))
            return LeafEntriesResult::Error;
        leafDepth = depth;
        lastLeaf = page;
        lastNextLeaf = node->nextLeaf;
        const size_t remaining = maxRows - result.size();
        if (!prefix && end - first > remaining) return LeafEntriesResult::LimitExceeded;
        const size_t take = std::min(end - first, remaining);
        for (size_t offset = 0; offset < take; ++offset) {
            const size_t i = descending ? end - 1 - offset : first + offset;
            result.push_back(node->values[i]);
            if (suffixes)
                resultSuffixes.push_back(readIntegerStoredKey(node->keys[i], BP_KEY_LEN - kIntegerKeyBytes));
        }
        return LeafEntriesResult::Complete;
    };
    const auto status = collect(collect, header_.rootPage, 0, nullptr, nullptr, true);
    if (status == LeafEntriesResult::Complete) {
        values = std::move(result);
        if (suffixes) *suffixes = std::move(resultSuffixes);
    }
    return status;
}

BPTree::LeafEntriesResult BPTree::readLeafEntriesChecked(
        size_t maxEntries, std::vector<LeafEntry>& entries) const {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    entries.clear();
    if (!bp_ || !bp_->isOpen()) return LeafEntriesResult::Error;
    if (!header_.rootPage) return LeafEntriesResult::Complete;

    std::vector<LeafEntry> result;
    // Deletion can leave a mostly empty tree. Bound its traversal even
    // when almost none of the visited pages contribute to the entry limit.
    const size_t maxNodes = std::max(maxEntries, kMaxTraversalDepth);
    std::unordered_set<uint32_t> visited;
    std::optional<size_t> leafDepth;
    uint32_t nextLeaf = 0;
    const auto collect = [&](const auto& self, uint32_t page, size_t depth,
                             const std::string* lower, const std::string* upper)
            -> LeafEntriesResult {
        if (visited.size() == maxNodes) return LeafEntriesResult::LimitExceeded;
        if (depth >= kMaxTraversalDepth || !visited.insert(page).second)
            return LeafEntriesResult::Error;
        const auto node = readNode(page);
        if (!node) return LeafEntriesResult::Error;
        // Deleted keys may leave a separator that no longer equals the first
        // key of its right child. Equal keys may also span two children.
        if (!node->keys.empty() && ((lower && node->keys.front() < *lower) ||
                                    (upper && node->keys.back() > *upper)))
            return LeafEntriesResult::Error;

        if (!node->isLeaf) {
            for (size_t i = 0; i < node->children.size(); ++i) {
                const auto status = self(self, node->children[i], depth + 1,
                    i ? &node->keys[i - 1] : lower,
                    i < node->keys.size() ? &node->keys[i] : upper);
                if (status != LeafEntriesResult::Complete) return status;
            }
            return LeafEntriesResult::Complete;
        }

        // Both the child graph and the independent leaf chain must name
        // every reached leaf in the same order, including empty leaves.
        if (leafDepth && (*leafDepth != depth || nextLeaf != page))
            return LeafEntriesResult::Error;
        leafDepth = depth;
        nextLeaf = node->nextLeaf;
        if (node->numKeys > maxEntries - result.size())
            return LeafEntriesResult::LimitExceeded;
        const size_t required = result.size() + node->numKeys;
        if (required > result.capacity()) {
            const size_t capacity = result.capacity();
            const size_t grown = capacity + std::min(capacity, maxEntries - capacity);
            result.reserve(std::max(required, grown));
        }
        for (size_t i = 0; i < node->numKeys; ++i) {
            LeafEntry entry;
            std::copy_n(node->keys[i].data(), BP_KEY_LEN, entry.key.begin());
            entry.value = node->values[i];
            result.push_back(entry);
        }
        return LeafEntriesResult::Complete;
    };

    const auto status = collect(collect, header_.rootPage, 0, nullptr, nullptr);
    if (status != LeafEntriesResult::Complete) return status;
    if (!leafDepth || nextLeaf) return LeafEntriesResult::Error;
    entries = std::move(result);
    return LeafEntriesResult::Complete;
}

BPTree::LeafEntriesResult BPTree::readSignedIntegerPrefix(
        size_t count, size_t maxNodes, bool descending,
        std::vector<IntegerEntry>& entries, size_t& visitedNodes) const {
    return readSignedIntegerRange(count, maxNodes, descending, std::nullopt,
                                  std::nullopt, entries, visitedNodes);
}

BPTree::LeafEntriesResult BPTree::readSignedIntegerRange(
        size_t count, size_t maxNodes, bool descending,
        std::optional<int64_t> lower, std::optional<int64_t> upper,
        std::vector<IntegerEntry>& entries, size_t& visitedNodes) const {
    std::shared_lock<std::shared_mutex> treeLock(treeMutex_);
    entries.clear();
    visitedNodes = 0;
    if (!bp_ || !bp_->isOpen() ||
        header_.reserved != bptree_format::kSignedIntegerKeyFormat) {
        return LeafEntriesResult::Error;
    }
    if (count == 0 || header_.rootPage == 0 || (lower && upper && *lower > *upper)) {
        return LeafEntriesResult::Complete;
    }

    const auto firstKey = lower ? std::optional(storeIntegerKey(*lower)) : std::nullopt;
    const auto lastKey = upper ? std::optional(storeIntegerKey(*upper)) : std::nullopt;
    std::vector<IntegerEntry> result;
    std::unordered_set<uint32_t> visited;
    std::optional<size_t> leafDepth;
    uint32_t lastLeaf = 0;
    uint32_t lastNextLeaf = 0;
    const auto collect = [&](const auto& self, uint32_t page, size_t depth,
                             const std::string* lowerFence, const std::string* upperFence,
                             bool rightEdge)
            -> LeafEntriesResult {
        if (depth >= kMaxTraversalDepth || visited.find(page) != visited.end())
            return LeafEntriesResult::Error;
        if (visited.size() == maxNodes) return LeafEntriesResult::LimitExceeded;
        visited.insert(page);
        visitedNodes = visited.size();
        const auto node = readNode(page);
        if (!node) return LeafEntriesResult::Error;
        // With unique keys, the left child excludes its upper separator.
        // Removal may leave the right child above its lower separator.
        if (!node->keys.empty() &&
            ((lowerFence && node->keys.front() < *lowerFence) ||
             (upperFence && node->keys.back() >= *upperFence))) {
            return LeafEntriesResult::Error;
        }
        if (!node->isLeaf) {
            if (std::adjacent_find(node->keys.begin(), node->keys.end()) !=
                node->keys.end()) return LeafEntriesResult::Error;
            // Equality to a separator belongs to the child on its right.
            // Include that child when either closed query bound equals it.
            const size_t first = firstKey
                ? upperBoundIdx(node->keys, node->numKeys, *firstKey) : 0;
            const size_t end = lastKey
                ? upperBoundIdx(node->keys, node->numKeys, *lastKey) + 1
                : node->children.size();
            for (size_t offset = 0; offset < end - first && result.size() != count; ++offset) {
                const size_t i = descending ? end - 1 - offset : first + offset;
                const auto status = self(self, node->children[i], depth + 1,
                    i ? &node->keys[i - 1] : lowerFence,
                    i < node->keys.size() ? &node->keys[i] : upperFence,
                    rightEdge && i + 1 == node->children.size());
                if (status != LeafEntriesResult::Complete) return status;
            }
            return LeafEntriesResult::Complete;
        }

        // The selected children form a contiguous traversal. Check the
        // links between reached leaves and the end of the tree when reached.
        const bool validNextLeaf = descending && lastLeaf
            ? node->nextLeaf == lastLeaf
            : rightEdge == (node->nextLeaf == 0) && visited.find(node->nextLeaf) == visited.end();
        if ((leafDepth && *leafDepth != depth) || !validNextLeaf ||
            (!descending && lastLeaf && lastNextLeaf != page)) {
            return LeafEntriesResult::Error;
        }
        leafDepth = depth;
        lastLeaf = page;
        lastNextLeaf = node->nextLeaf;
        const size_t first = firstKey ? lowerBoundIdx(node->keys, node->numKeys, *firstKey) : 0;
        const size_t end = lastKey
            ? upperBoundIdx(node->keys, node->numKeys, *lastKey) : node->keys.size();
        const size_t take = std::min(end - first, count - result.size());
        const size_t required = result.size() + take;
        if (required > result.capacity()) {
            const size_t capacity = result.capacity();
            const size_t grown = capacity + std::min(capacity, count - capacity);
            result.reserve(std::max(required, grown));
        }
        for (size_t position = 0; position < take; ++position) {
            const size_t i = descending ? end - 1 - position : first + position;
            const int64_t key = readIntegerStoredKey(node->keys[i]);
            if (!result.empty() && (descending ? key >= result.back().key
                                               : key <= result.back().key)) {
                return LeafEntriesResult::Error;
            }
            result.push_back({key, node->values[i]});
        }
        return LeafEntriesResult::Complete;
    };

    const auto status = collect(collect, header_.rootPage, 0, nullptr, nullptr, true);
    if (status != LeafEntriesResult::Complete) return status;
    if (!leafDepth) return LeafEntriesResult::Error;
    entries = std::move(result);
    return LeafEntriesResult::Complete;
}

} // namespace dbms
