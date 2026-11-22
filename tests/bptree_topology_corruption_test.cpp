// test_sources: src/access/BPTree.cpp src/storage/BufferPool.cpp

#include "access/BPTree.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

constexpr size_t kPageSize = dbms::BP_PAGE_SIZE;
constexpr size_t kChecksumOffset = kPageSize - sizeof(uint32_t);
constexpr uint16_t kLegacyChecksummedFormat = 0xC551;
constexpr uint16_t kPageBoundChecksummedFormat = 0xC552;
using LeafEntriesResult = dbms::BPTree::LeafEntriesResult;

uint32_t legacyChecksum(const char* page) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < kPageSize; ++i) {
        const uint8_t byte = i >= kChecksumOffset
            ? 0 : static_cast<uint8_t>(page[i]);
        crc ^= byte;
        for (unsigned bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^ ((crc & 1u) ? 0x82F63B78u : 0u);
        }
    }
    crc ^= 0xFFFFFFFFu;
    return crc == 0 ? 0xFFFFFFFFu : crc;
}

void stampLegacyChecksum(std::vector<char>& file, size_t pageId) {
    char* page = file.data() + pageId * kPageSize;
    uint32_t zero = 0;
    std::memcpy(page + kChecksumOffset, &zero, sizeof(zero));
    const uint32_t checksum = legacyChecksum(page);
    std::memcpy(page + kChecksumOffset, &checksum, sizeof(checksum));
}

void putU16(std::vector<char>& file, size_t offset, uint16_t value) {
    std::memcpy(file.data() + offset, &value, sizeof(value));
}

void putU32(std::vector<char>& file, size_t offset, uint32_t value) {
    std::memcpy(file.data() + offset, &value, sizeof(value));
}

void putI64(std::vector<char>& file, size_t offset, int64_t value) {
    std::memcpy(file.data() + offset, &value, sizeof(value));
}

void writeHeader(std::vector<char>& file, uint32_t root, uint32_t nextFree) {
    putU32(file, 0, root);
    putU32(file, sizeof(uint32_t), nextFree);
    putU16(file, sizeof(uint32_t) * 2, 2);
}

void writeInternal(std::vector<char>& file, uint32_t page, uint32_t child) {
    const size_t base = static_cast<size_t>(page) * kPageSize;
    file[base] = 0;
    putU16(file, base + 1, 0);
    putU32(file, base + 3, child);
}

void writeLeaf(std::vector<char>& file, uint32_t page, const std::string& key,
               int64_t value, uint32_t nextLeaf) {
    const size_t base = static_cast<size_t>(page) * kPageSize;
    file[base] = 1;
    putU16(file, base + 1, 1);
    std::memcpy(file.data() + base + 3, key.data(),
                std::min(key.size(), dbms::BP_KEY_LEN));
    putI64(file, base + 3 + dbms::BP_KEY_LEN, value);
    putU32(file, base + 3 + dbms::BP_KEY_LEN + sizeof(int64_t), nextLeaf);
}

void publish(const std::string& path, const std::vector<char>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out);
    out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    out.flush();
    assert(out);
}

void cleanup(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    ec.clear();
    std::filesystem::remove(path + ".tde", ec);
}

std::filesystem::path createTempDir() {
    const std::string pattern =
        (std::filesystem::temp_directory_path() /
         "dbms_bptree_page_swap_XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    char* created = ::mkdtemp(writable.data());
    if (created == nullptr) {
        throw std::runtime_error("mkdtemp for B+ tree corruption test failed");
    }
    return created;
}

void test_checked_lookup_distinguishes_absence() {
    const std::string path = "bptree_checked_empty.idx";
    cleanup(path);
    dbms::BPTree tree(path);
    assert(tree.open());
    int64_t value = 0;
    std::vector<int64_t> values{99};
    assert(tree.searchChecked("missing", value) ==
           dbms::BPTree::SearchResult::NotFound);
    assert(tree.searchMultiChecked("missing", values) && values.empty());
    assert(tree.rangeScanChecked("a", "z", values) && values.empty());
    assert(tree.allValuesChecked(values) && values.empty());
    std::vector<dbms::BPTree::LeafEntry> entries(1);
    assert(tree.readLeafEntriesChecked(0, entries) == LeafEntriesResult::Complete && entries.empty());
    assert(tree.insert("key", 11));
    assert(tree.searchChecked("key", value) == dbms::BPTree::SearchResult::Found);
    assert(value == 11);
    assert(tree.searchChecked("missing", value) ==
           dbms::BPTree::SearchResult::NotFound);
    assert(tree.insert("2", 22));
    assert(tree.insert("10", 33));
    assert(tree.readLeafEntriesChecked(3, entries) == LeafEntriesResult::Complete);
    assert(entries.size() == 3);
    assert(std::string(entries.front().key.data(), dbms::BP_KEY_LEN) ==
           dbms::BPTree::normalizeKeyForComparison("10"));
    assert(entries[0].value == 33 && entries[1].value == 22 && entries[2].value == 11);
    assert(tree.readLeafEntriesChecked(2, entries) == LeafEntriesResult::LimitExceeded && entries.empty());
    tree.close();
    assert(tree.searchChecked("key", value) == dbms::BPTree::SearchResult::Error);
    assert(!tree.searchMultiChecked("key", values));
    assert(tree.readLeafEntriesChecked(3, entries) == LeafEntriesResult::Error && entries.empty());
    cleanup(path);
}

void test_leaf_entry_scan_bounds_sparse_tree() {
    const auto directory = createTempDir();
    dbms::BPTree tree(directory / "sparse.idx");
    assert(tree.open());
    constexpr int rowCount = 4096;
    for (int i = 0; i < rowCount; ++i)
        assert(tree.insert(std::to_string(10000 + i), 1 + i));
    for (int i = 1; i < rowCount - 1; ++i)
        assert(tree.remove(std::to_string(10000 + i)));

    std::vector<dbms::BPTree::LeafEntry> entries(1);
    assert(tree.readLeafEntriesChecked(2, entries) == LeafEntriesResult::LimitExceeded);
    assert(entries.empty());
    assert(tree.readLeafEntriesChecked(rowCount, entries) == LeafEntriesResult::Complete);
    assert(entries.size() == 2 && entries.front().value == 1 && entries.back().value == rowCount);
    tree.close();
    std::filesystem::remove_all(directory);
}

void test_multi_lookup_reads_only_the_matching_key_range() {
    const auto directory = createTempDir();
    const auto path = directory / "prefix.idx";
    const std::string prefix = "prefixabcdefghijklmnopqrst";
    const auto laterKey = [](int n) { return "z" + std::to_string(10000 + n); };
    {
        dbms::BPTree tree(path);
        assert(tree.open());
        for (int64_t rid = 1; rid <= 275; ++rid)
            assert(tree.insertMulti(prefix + std::to_string(rid), rid));
        for (int n = 0; n < 2000; ++n)
            assert(tree.insertMulti(laterKey(n), 10000 + n));
        // Splits and removals leave both empty leaves and equal keys across
        // the remaining leaf boundaries.
        for (int64_t rid = 50; rid <= 150; ++rid)
            assert(tree.removeMulti(prefix, rid));
        assert(tree.flush());
    }
    {
        dbms::BPTree tree(path);
        assert(tree.openExisting());
        auto before = tree.cacheStatistics();
        std::vector<int64_t> values;
        assert(tree.searchMultiChecked(prefix + "-probe", values));
        std::sort(values.begin(), values.end());
        std::vector<int64_t> expected;
        for (int64_t rid = 1; rid <= 275; ++rid)
            if (rid < 50 || rid > 150) expected.push_back(rid);
        assert(values == expected);
        auto after = tree.cacheStatistics();
        assert(after.misses - before.misses <= 9);
        assert(tree.searchMultiChecked("a-before", values) && values.empty());
        assert(tree.searchMultiChecked("y-gap", values) && values.empty());
        assert(tree.searchMultiChecked(laterKey(1999), values));
        assert(values == std::vector<int64_t>{11999});
    }
    std::filesystem::remove_all(directory);
}

void test_signed_integer_prefix_and_legacy_reopen() {
    const auto directory = createTempDir();
    const auto numericPath = directory / "numeric.idx";
    const auto legacyPath = directory / "legacy.idx";
    constexpr int64_t low = std::numeric_limits<int64_t>::min();
    constexpr int64_t high = std::numeric_limits<int64_t>::max();
    {
        dbms::BPTree tree(numericPath);
        assert(tree.openWithSignedIntegerKeys() && tree.usesSignedIntegerKeys());
        for (int i = 0; i < 420; ++i) {
            const int key = ((i * 73) % 420) - 210;
            assert(tree.insert(std::to_string(key) + '\x01', key + 1000));
        }
        assert(tree.insert(std::to_string(low), 1));
        assert(tree.insert(std::to_string(high) + '\x01', 2));
        assert(!tree.insert("+0", 9999));
        assert(!tree.insert("1\x01" "2", 3));
        assert(tree.remove("-208"));
        assert(tree.insert("-208\x01", 792));
        assert(tree.flush());
    }
    {
        dbms::BPTree tree(numericPath);
        assert(tree.openExisting() && tree.usesSignedIntegerKeys());
        int64_t value = 0;
        assert(tree.search("-208", value) && value == 792);
        assert(tree.search(std::to_string(low) + '\x01', value) && value == 1);
        std::vector<int64_t> values;
        assert(tree.rangeScanChecked("-2", "+2", values));
        assert((values == std::vector<int64_t>{998, 999, 1000, 1001, 1002}));
        assert(tree.allValuesChecked(values) && values.size() == 422);
        assert(values.front() == 1 && values.back() == 2);

        std::vector<dbms::BPTree::IntegerEntry> entries;
        size_t visits = 0;
        assert(tree.readSignedIntegerPrefix(5, 64, false, entries, visits) == LeafEntriesResult::Complete);
        assert(entries.size() == 5 && entries.front().key == low && entries.back().key == -207);
        const size_t firstPageVisits = visits;
        assert(tree.readSignedIntegerPrefix(5, 64, true, entries, visits) == LeafEntriesResult::Complete);
        assert(entries.size() == 5 && entries.front().key == high && entries.back().key == 206);
        const size_t lastPageVisits = visits;
        assert(tree.readSignedIntegerPrefix(160, 64, true, entries, visits) == LeafEntriesResult::Complete);
        assert(entries.size() == 160 && entries.back().key == 51 && visits > lastPageVisits);
        assert(tree.readSignedIntegerPrefix(500, 64, false, entries, visits) == LeafEntriesResult::Complete);
        assert(entries.size() == 422 && visits > firstPageVisits && visits > lastPageVisits);
        const size_t fullVisits = visits;
        for (const bool descending : {false, true}) {
            assert(tree.readSignedIntegerRange(200, 64, descending, -99, 12, entries, visits) ==
                   LeafEntriesResult::Complete);
            assert(entries.size() == 112 && visits < fullVisits);
            for (size_t i = 0; i < entries.size(); ++i) {
                const auto key = descending ? 12 - static_cast<int64_t>(i) : -99 + static_cast<int64_t>(i);
                assert(entries[i].key == key && entries[i].value == key + 1000);
            }
            assert(tree.readSignedIntegerRange(1, 64, descending, low, low, entries, visits) ==
                   LeafEntriesResult::Complete);
            assert(entries.size() == 1 && entries.front().key == low);
            assert(tree.readSignedIntegerRange(1, 64, descending, high, high, entries, visits) ==
                   LeafEntriesResult::Complete);
            assert(entries.size() == 1 && entries.front().key == high);
        }
        assert(tree.readSignedIntegerRange(10, 64, false, 12, -99, entries, visits) == LeafEntriesResult::Complete);
        assert(entries.empty() && visits == 0);
        assert(tree.readSignedIntegerPrefix(5, 1, false, entries, visits) == LeafEntriesResult::LimitExceeded);
        assert(entries.empty());
    }
    {
        dbms::BPTree tree(legacyPath);
        assert(tree.open() && tree.insert("-2", 11) && tree.insert("2", 22) && tree.insert("10", 33));
        assert(tree.flush());
    }
    {
        dbms::BPTree tree(legacyPath);
        assert(tree.openWithSignedIntegerKeys() && !tree.usesSignedIntegerKeys());
        std::vector<int64_t> values;
        assert(tree.allValuesChecked(values));
        assert((values == std::vector<int64_t>{11, 33, 22}));
    }
    std::filesystem::remove_all(directory);
}

void test_checksummed_index_page_rejects_corruption() {
    const std::string path = "bptree_page_checksum.idx";
    cleanup(path);
    {
        dbms::BPTree tree(path);
        assert(tree.open());
        assert(tree.insert("alpha", 42));
        assert(tree.flush());
        tree.close();
    }

    const auto fileSize = std::filesystem::file_size(path);
    assert(fileSize >= 2 * kPageSize);
    std::vector<char> bytes(static_cast<size_t>(fileSize));
    {
        std::ifstream input(path, std::ios::binary);
        assert(input);
        input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        assert(input);
    }
    uint16_t format = 0;
    std::memcpy(&format, bytes.data() + sizeof(uint32_t) * 2 + sizeof(uint16_t),
                sizeof(format));
    assert(format != 0);

    // Flip a key byte without changing the node's shape.  Structural
    // validation alone would accept the page and silently return a miss.
    bytes[kPageSize + 3] ^= 0x01;
    publish(path, bytes);
    {
        dbms::BPTree tree(path);
        assert(!tree.openExisting());
    }
    cleanup(path);
}

void test_checksummed_index_rejects_swapped_pages() {
    const std::filesystem::path tempDir = createTempDir();
    const std::string path = (tempDir / "page_swap.idx").string();
    {
        dbms::BPTree tree(path);
        assert(tree.open());
        for (int i = 0; i < 200; ++i) {
            std::string key = std::to_string(i);
            key.insert(0, 4 - key.size(), '0');
            assert(tree.insert("key-" + key, i));
        }
        std::vector<dbms::BPTree::LeafEntry> entries;
        assert(tree.readLeafEntriesChecked(200, entries) == LeafEntriesResult::Complete);
        assert(entries.size() == 200);
        for (int i = 0; i < 200; ++i) assert(entries[i].value == i);
        assert(tree.readLeafEntriesChecked(199, entries) == LeafEntriesResult::LimitExceeded && entries.empty());
        assert(tree.flush());
        tree.close();
    }

    const auto fileSize = std::filesystem::file_size(path);
    assert(fileSize >= 4 * kPageSize);
    std::vector<char> bytes(static_cast<size_t>(fileSize));
    {
        std::ifstream input(path, std::ios::binary);
        assert(input);
        input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        assert(input);
    }
    // The first two data pages are sibling leaves after this insertion set.
    // Swapping whole, individually valid pages preserves each page's CRC but
    // changes which keys the root's child pointers name.
    std::swap_ranges(bytes.begin() + kPageSize,
                     bytes.begin() + 2 * kPageSize,
                     bytes.begin() + 2 * kPageSize);
    publish(path, bytes);

    {
        dbms::BPTree tree(path);
        const bool opened = tree.openExisting();
        if (opened) {
            int64_t value = 0;
            // A page-number-bound checksum must report corruption, not a
            // clean NotFound caused by looking in the swapped sibling page.
            assert(tree.searchChecked("key-0000", value) ==
                   dbms::BPTree::SearchResult::Error);
            std::vector<dbms::BPTree::LeafEntry> entries(1);
            assert(tree.readLeafEntriesChecked(200, entries) == LeafEntriesResult::Error && entries.empty());
        }
        tree.close();
    }
    std::filesystem::remove_all(tempDir);
}

void test_previous_checksummed_format_remains_readable() {
    const std::filesystem::path tempDir = createTempDir();
    const std::string path = (tempDir / "legacy_checksum.idx").string();
    {
        dbms::BPTree tree(path);
        assert(tree.open());
        assert(tree.insert("alpha", 42));
        assert(tree.flush());
        tree.close();
    }

    const auto fileSize = std::filesystem::file_size(path);
    assert(fileSize >= 2 * kPageSize && fileSize % kPageSize == 0);
    std::vector<char> bytes(static_cast<size_t>(fileSize));
    {
        std::ifstream input(path, std::ios::binary);
        assert(input);
        input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        assert(input);
    }
    const size_t formatOffset = sizeof(uint32_t) * 2 + sizeof(uint16_t);
    uint16_t format = 0;
    std::memcpy(&format, bytes.data() + formatOffset, sizeof(format));
    assert(format == kPageBoundChecksummedFormat);
    putU16(bytes, formatOffset, kLegacyChecksummedFormat);
    for (size_t pageId = 0; pageId < bytes.size() / kPageSize; ++pageId) {
        stampLegacyChecksum(bytes, pageId);
    }
    publish(path, bytes);

    {
        dbms::BPTree tree(path);
        assert(tree.openExisting());
        int64_t value = 0;
        assert(tree.searchChecked("alpha", value) ==
               dbms::BPTree::SearchResult::Found);
        assert(value == 42);
        tree.close();
    }
    std::filesystem::remove_all(tempDir);
}

void test_internal_page_cycle_fails_closed() {
    const std::string path = "/tmp/bptree_internal_cycle.idx";
    cleanup(path);
    std::vector<char> bytes(kPageSize * 3, 0);
    writeHeader(bytes, 1, 3);
    writeInternal(bytes, 1, 2);
    writeInternal(bytes, 2, 1);
    publish(path, bytes);

    dbms::BPTree tree(path);
    assert(tree.open());
    int64_t value = 0;
    assert(!tree.search("key", value));
    assert(tree.searchMulti("key").empty());
    assert(tree.rangeScan("a", "z").empty());
    assert(tree.allValues().empty());
    assert(tree.searchChecked("key", value) == dbms::BPTree::SearchResult::Error);
    std::vector<int64_t> values{99};
    assert(!tree.searchMultiChecked("key", values) && values.empty());
    values = {99};
    assert(!tree.rangeScanChecked("a", "z", values) && values.empty());
    values = {99};
    assert(!tree.allValuesChecked(values) && values.empty());
    std::vector<dbms::BPTree::LeafEntry> entries(1);
    assert(tree.readLeafEntriesChecked(4, entries) == LeafEntriesResult::Error && entries.empty());
    assert(!tree.insert("key", 1));
    assert(!tree.insertMulti("key", 1));
    assert(!tree.remove("key"));
    assert(!tree.removeMulti("key", 1));
    tree.close();
    cleanup(path);
}

void test_leaf_chain_cycle_and_type_confusion_fail_closed() {
    const std::string path = "/tmp/bptree_leaf_cycle.idx";
    cleanup(path);
    std::vector<char> bytes(kPageSize * 3, 0);
    writeHeader(bytes, 1, 3);
    writeLeaf(bytes, 1, "key", 11, 2);
    writeLeaf(bytes, 2, "key", 22, 1);
    publish(path, bytes);
    {
        dbms::BPTree tree(path);
        assert(tree.open());
        assert(tree.searchMulti("key").empty());
        std::vector<int64_t> values{99};
        assert(!tree.searchMultiChecked("key", values) && values.empty());
        std::vector<dbms::BPTree::LeafEntry> entries(1);
        assert(tree.readLeafEntriesChecked(4, entries) == LeafEntriesResult::Error && entries.empty());
        tree.close();
    }

    std::fill(bytes.begin(), bytes.end(), 0);
    writeHeader(bytes, 1, 3);
    writeLeaf(bytes, 1, "key", 11, 2);
    writeInternal(bytes, 2, 1);
    publish(path, bytes);
    {
        dbms::BPTree tree(path);
        assert(tree.open());
        assert(tree.searchMulti("key").empty());
        std::vector<int64_t> values{99};
        assert(!tree.searchMultiChecked("key", values) && values.empty());
        tree.close();
    }
    cleanup(path);
}

void test_leaf_entry_scan_validates_sibling_links_and_separators() {
    const auto tempDir = createTempDir();
    const auto path = tempDir / "leaf_entries.idx";
    std::vector<char> bytes(4 * kPageSize, 0);
    writeHeader(bytes, 1, 4);
    putU16(bytes, kPageSize + 1, 1);
    bytes[kPageSize + 3] = 'b';
    putU32(bytes, kPageSize + 3 + dbms::BP_KEY_LEN, 2);
    putU32(bytes, kPageSize + 3 + dbms::BP_KEY_LEN + sizeof(uint32_t), 3);
    writeLeaf(bytes, 2, "a", 11, 3);
    writeLeaf(bytes, 3, "c", 22, 0);

    // A separator can still divide its children after the original key
    // has been deleted from the right-hand leaf.
    std::vector<dbms::BPTree::LeafEntry> entries;
    publish(path.string(), bytes);
    {
        dbms::BPTree tree(path);
        assert(tree.openExisting());
        assert(tree.readLeafEntriesChecked(2, entries) == LeafEntriesResult::Complete);
        assert(entries.size() == 2 && entries[0].value == 11 && entries[1].value == 22);
    }

    // The child graph still lists both leaves, but the leaf chain drops one.
    writeLeaf(bytes, 2, "a", 11, 0);
    publish(path.string(), bytes);
    {
        dbms::BPTree tree(path);
        assert(tree.openExisting());
        assert(tree.readLeafEntriesChecked(2, entries) == LeafEntriesResult::Error && entries.empty());
    }

    // Restore the leaf links and break the boundary between the children.
    writeLeaf(bytes, 2, "a", 11, 3);
    bytes[kPageSize + 3] = 'd';
    publish(path.string(), bytes);
    {
        dbms::BPTree tree(path);
        assert(tree.openExisting());
        assert(tree.readLeafEntriesChecked(2, entries) == LeafEntriesResult::Error && entries.empty());
    }
    std::filesystem::remove_all(tempDir);
}

}  // namespace

int main() {
    test_checked_lookup_distinguishes_absence();
    test_leaf_entry_scan_bounds_sparse_tree();
    test_multi_lookup_reads_only_the_matching_key_range();
    test_signed_integer_prefix_and_legacy_reopen();
    test_checksummed_index_page_rejects_corruption();
    test_checksummed_index_rejects_swapped_pages();
    test_previous_checksummed_format_remains_readable();
    test_internal_page_cycle_fails_closed();
    test_leaf_chain_cycle_and_type_confusion_fail_closed();
    test_leaf_entry_scan_validates_sibling_links_and_separators();
    std::cout << "[BPTREE TOPOLOGY] cycles and invalid leaf links rejected OK\n";
    return 0;
}
