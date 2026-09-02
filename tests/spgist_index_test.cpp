// test_sources: src/access/SPGiSTIndex.cpp

#include "access/SPGiSTIndex.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iostream>

using dbms::SPGiSTIndex;

static void test_missing_remove_preserves_size() {
    SPGiSTIndex index(-100.0, -100.0, 100.0, 100.0);
    index.insert(1.0, 2.0, 42);
    assert(index.size() == 1);

    index.remove(1.0, 2.0, 99);
    assert(index.size() == 1);
    assert(index.searchEquals(1.0, 2.0) == std::vector<int64_t>{42});

    index.remove(9.0, 9.0, 42);
    assert(index.size() == 1);

    index.remove(1.0, 2.0, 42);
    assert(index.size() == 0);
    assert(index.searchEquals(1.0, 2.0).empty());

    // Repeating the delete must not underflow the unsigned size counter.
    index.remove(1.0, 2.0, 42);
    assert(index.size() == 0);
}

static void test_within_uses_exact_distance() {
    SPGiSTIndex index(-100.0, -100.0, 100.0, 100.0);
    index.insert(0.0, 0.0, 1);
    index.insert(3.0, 4.0, 2);  // exactly on a radius-five boundary
    index.insert(4.0, 4.0, 3);  // inside the bounding box, outside the circle
    index.insert(-6.0, 0.0, 4);

    auto matches = index.searchWithin(0.0, 0.0, 5.0);
    std::sort(matches.begin(), matches.end());
    assert((matches == std::vector<int64_t>{1, 2}));
    assert(index.searchWithin(0.0, 0.0, -1.0).empty());
}

static void test_duplicate_points_do_not_create_unbounded_depth() {
    SPGiSTIndex index(-1.0, -1.0, 1.0, 1.0);
    constexpr int duplicateCount = 20000;
    for (int rid = 0; rid < duplicateCount; ++rid) {
        index.insert(0.25, -0.25, rid);
    }

    assert(index.size() == duplicateCount);
    const auto matches = index.searchEquals(0.25, -0.25);
    assert(matches.size() == duplicateCount);
}

int main() {
    test_missing_remove_preserves_size();
    test_within_uses_exact_distance();
    test_duplicate_points_do_not_create_unbounded_depth();
    std::cout << "[SPGIST] missing remove accounting OK\n";
    std::cout << "[SPGIST] exact radius filtering OK\n";
    std::cout << "[SPGIST] duplicate-point depth guard OK\n";
    return 0;
}
