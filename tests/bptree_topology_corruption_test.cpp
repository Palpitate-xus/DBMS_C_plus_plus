// test_sources: src/access/BPTree.cpp src/storage/BufferPool.cpp

#include "access/BPTree.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace {

constexpr size_t kPageSize = dbms::BP_PAGE_SIZE;

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
    test_internal_page_cycle_fails_closed();
    test_leaf_chain_cycle_and_type_confusion_fail_closed();
    std::cout << "[BPTREE TOPOLOGY] cycles and invalid leaf links rejected OK\n";
    return 0;
}
