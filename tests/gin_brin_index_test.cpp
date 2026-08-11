#include "access/GinIndex.h"
#include "access/BrinIndex.h"
#include "access/BPTree.h"
#include "access/HashIndex.h"
#include <cassert>
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

namespace fs = std::filesystem;

static void cleanup(const std::string& p) {
    if (fs::exists(p)) fs::remove(p);
}

static void test_gin_basic() {
    std::string idx = "/tmp/gin_test.idx";
    cleanup(idx);
    dbms::GinIndex gin(idx);
    gin.open();
    gin.insert("hello world", 1);
    gin.insert("world peace", 2);
    gin.insert("hello dbms", 3);

    auto r1 = gin.searchContains("hello");
    assert(r1.size() == 2);  // rows 1,3

    auto r2 = gin.searchContains("world");
    assert(r2.size() == 2);  // rows 1,2

    auto r3 = gin.searchContains("hello world");
    assert(r3.size() == 1);  // row 1 (AND semantics)

    auto r4 = gin.searchContains("nonexistent");
    assert(r4.empty());

    gin.close();
    cleanup(idx);
    std::cout << "[GIN] basic OK" << std::endl;
}

static void test_gin_remove() {
    std::string idx = "/tmp/gin_test2.idx";
    cleanup(idx);
    dbms::GinIndex gin(idx);
    gin.open();
    gin.insert("apple banana", 1);
    gin.insert("apple cherry", 2);
    gin.remove("apple banana", 1);

    auto r = gin.searchContains("apple");
    assert(r.size() == 1);  // only row 2

    gin.close();
    cleanup(idx);
    std::cout << "[GIN] remove OK" << std::endl;
}

static void test_gin_persistence() {
    std::string idx = "/tmp/gin_test3.idx";
    cleanup(idx);
    {
        dbms::GinIndex gin(idx);
        gin.open();
        gin.insert("persist test", 10);
        gin.insert("persist data", 20);
        gin.close();
    }
    {
        dbms::GinIndex gin(idx);
        gin.open();
        auto r = gin.searchContains("persist");
        assert(r.size() == 2);
        gin.close();
    }
    {
        std::ofstream out(idx, std::ios::trunc);
        out << "broken posting not-a-row-id\n";
    }
    {
        dbms::GinIndex broken(idx);
        assert(!broken.open());
    }
    cleanup(idx);
    std::cout << "[GIN] persistence OK" << std::endl;
}

static void test_hash_persistence_and_corruption() {
    std::string idx = "/tmp/hash_index_test.idx";
    cleanup(idx);
    {
        dbms::HashIndex hash(idx);
        assert(hash.open());
        hash.insert("alpha", 10);
        hash.insert("alpha", 11);
        assert(hash.close());
    }
    {
        dbms::HashIndex hash(idx);
        assert(hash.open());
        assert((hash.search("alpha") == std::vector<int64_t>{10, 11}));
        assert(hash.close());
    }
    {
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        out << "truncated index";
    }
    dbms::HashIndex broken(idx);
    assert(!broken.open());
    cleanup(idx);
    std::cout << "[HASH] durable persistence/corruption rejection OK" << std::endl;
}

static void test_brin_basic() {
    std::string idx = "/tmp/brin_test.idx";
    cleanup(idx);
    dbms::BrinIndex brin(idx);
    brin.open();

    for (int i = 0; i < 300; ++i) {
        brin.addValue(std::to_string(1000 + i), i);
    }

    auto r = brin.searchRange("1050", "1070");
    // Should return rowIds in the overlapping blocks.
    assert(!r.empty());

    auto r2 = brin.searchRange("2000", "3000");
    assert(r2.empty());

    assert(brin.close());
    {
        dbms::BrinIndex reopened(idx);
        assert(reopened.open());
        assert(!reopened.searchRange("1050", "1070").empty());
        assert(reopened.close());
    }
    {
        std::ofstream out(idx, std::ios::binary | std::ios::trunc);
        out << "truncated brin";
    }
    {
        dbms::BrinIndex broken(idx);
        assert(!broken.open());
    }
    cleanup(idx);
    std::cout << "[BRIN] basic OK" << std::endl;
}

static void test_brin_json_contains() {
    std::string idx = "/tmp/gin_json.idx";
    cleanup(idx);
    dbms::GinIndex gin(idx);
    gin.open();
    gin.insert("{\"name\":\"alice\",\"age\":30}", 1);
    gin.insert("{\"name\":\"bob\",\"age\":40}", 2);

    auto r = gin.searchJsonContains("alice");
    assert(r.size() == 1);

    gin.close();
    cleanup(idx);
    std::cout << "[GIN] JSON contains OK" << std::endl;
}

static void test_btree_split_and_range() {
    std::string idx = "/tmp/btree_split_test.idx";
    cleanup(idx);
    {
        dbms::BPTree tree(idx);
        assert(tree.open());

        // BP_KEY_LEN is 20 and the default leaf order is 100.  This crosses
        // the root-leaf split boundary and keeps keys lexicographically
        // ordered without relying on numeric-string ordering.
        for (int i = 0; i < 250; ++i) {
            char key[16];
            std::snprintf(key, sizeof(key), "k%04d", i);
            assert(tree.insert(key, i));
        }

        for (int i = 0; i < 250; ++i) {
            char key[16];
            std::snprintf(key, sizeof(key), "k%04d", i);
            int64_t value = -1;
            assert(tree.search(key, value));
            assert(value == i);
        }

        auto range = tree.rangeScan("k0050", "k0100");
        assert(range.size() == 51);
        auto all = tree.allValues();
        assert(all.size() == 250);
        tree.close();
    }
    cleanup(idx);

    // Duplicate secondary-index keys must remain discoverable when equal
    // keys span more than one leaf page.
    {
        dbms::BPTree tree(idx);
        assert(tree.open());
        // Enough rows to force both leaf and internal-node splits.
        for (int i = 0; i < 6000; ++i) assert(tree.insertMulti("same", i));
        auto matches = tree.searchMulti("same");
        assert(matches.size() == 6000);
        assert(tree.removeMulti("same", 3000));
        assert(!tree.removeMulti("same", 3000));
        matches = tree.searchMulti("same");
        assert(matches.size() == 5999);
        assert(std::find(matches.begin(), matches.end(), 2999) != matches.end());
        assert(std::find(matches.begin(), matches.end(), 3001) != matches.end());
        tree.close();
        dbms::BPTree reopened(idx);
        assert(reopened.open());
        assert(reopened.searchMulti("same").size() == 5999);
        reopened.close();
    }
    cleanup(idx);
    std::cout << "[BPTREE] root split/search/range OK" << std::endl;
}

int main() {
    test_gin_basic();
    test_gin_remove();
    test_gin_persistence();
    test_hash_persistence_and_corruption();
    test_brin_basic();
    test_brin_json_contains();
    test_btree_split_and_range();
    std::cout << "[GIN_BRIN] all passed" << std::endl;
    return 0;
}
