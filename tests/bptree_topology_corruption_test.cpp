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
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

constexpr size_t kPageSize = dbms::BP_PAGE_SIZE;
constexpr size_t kChecksumOffset = kPageSize - sizeof(uint32_t);
constexpr uint16_t kLegacyChecksummedFormat = 0xC551;
constexpr uint16_t kPageBoundChecksummedFormat = 0xC552;

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
    assert(tree.insert("key", 11));
    assert(tree.searchChecked("key", value) == dbms::BPTree::SearchResult::Found);
    assert(value == 11);
    assert(tree.searchChecked("missing", value) ==
           dbms::BPTree::SearchResult::NotFound);
    tree.close();
    assert(tree.searchChecked("key", value) == dbms::BPTree::SearchResult::Error);
    assert(!tree.searchMultiChecked("key", values));
    cleanup(path);
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

}  // namespace

int main() {
    test_checked_lookup_distinguishes_absence();
    test_checksummed_index_page_rejects_corruption();
    test_checksummed_index_rejects_swapped_pages();
    test_previous_checksummed_format_remains_readable();
    test_internal_page_cycle_fails_closed();
    test_leaf_chain_cycle_and_type_confusion_fail_closed();
    std::cout << "[BPTREE TOPOLOGY] cycles and invalid leaf links rejected OK\n";
    return 0;
}
