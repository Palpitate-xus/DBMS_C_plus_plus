#include "access/GinIndex.h"
#include "access/BrinIndex.h"
#include <cassert>
#include <filesystem>
#include <iostream>

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
    cleanup(idx);
    std::cout << "[GIN] persistence OK" << std::endl;
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

    brin.close();
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

int main() {
    test_gin_basic();
    test_gin_remove();
    test_gin_persistence();
    test_brin_basic();
    test_brin_json_contains();
    std::cout << "[GIN_BRIN] all passed" << std::endl;
    return 0;
}
