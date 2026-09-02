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

static void test_nearby_coordinates_remain_distinct() {
    SPGiSTIndex index(-10.0, -10.0, 10.0, 10.0);
    constexpr double x1 = 1.0000001;
    constexpr double y1 = 2.0000001;
    constexpr double x2 = 1.0000002;
    constexpr double y2 = 2.0000002;

    index.insert(x1, y1, 101);
    index.insert(x2, y2, 202);
    assert(index.searchEquals(x1, y1) == std::vector<int64_t>{101});
    assert(index.searchEquals(x2, y2) == std::vector<int64_t>{202});
    assert(index.searchWithin(x1, y1, 0.0) == std::vector<int64_t>{101});

    // Removing a RID using a merely nearby coordinate must not remove it.
    index.remove(x2, y2, 101);
    assert(index.size() == 2);
    assert(index.searchEquals(x1, y1) == std::vector<int64_t>{101});
}

static void test_world_bounds_expand_without_false_negatives() {
    SPGiSTIndex index(-10.0, -10.0, 10.0, 10.0);
    index.insert(0.0, 0.0, 1);
    index.insert(100.0, 5.0, 2);
    index.insert(-250.0, -20.0, 3);
    index.insert(4.0, 300.0, 4);

    assert(index.size() == 4);
    assert(index.searchWithin(100.0, 5.0, 0.0) == std::vector<int64_t>{2});
    assert(index.searchWithin(-250.0, -20.0, 0.0) == std::vector<int64_t>{3});
    assert(index.searchWithin(4.0, 300.0, 0.0) == std::vector<int64_t>{4});
    assert(index.searchRightOf(50.0) == std::vector<int64_t>{2});
    assert(index.searchLeftOf(-100.0) == std::vector<int64_t>{3});
    assert(index.searchAbove(200.0) == std::vector<int64_t>{4});
}

static void test_directional_searches_are_strict() {
    SPGiSTIndex index(-10.0, -10.0, 10.0, 10.0);
    index.insert(5.0, 5.0, 1);  // exactly on both query thresholds
    index.insert(4.0, 5.0, 2);
    index.insert(6.0, 5.0, 3);
    index.insert(5.0, 4.0, 4);
    index.insert(5.0, 6.0, 5);

    assert(index.searchLeftOf(5.0) == std::vector<int64_t>{2});
    assert(index.searchRightOf(5.0) == std::vector<int64_t>{3});
    assert(index.searchBelow(5.0) == std::vector<int64_t>{4});
    assert(index.searchAbove(5.0) == std::vector<int64_t>{5});
}

int main() {
    test_missing_remove_preserves_size();
    test_within_uses_exact_distance();
    test_duplicate_points_do_not_create_unbounded_depth();
    test_nearby_coordinates_remain_distinct();
    test_world_bounds_expand_without_false_negatives();
    test_directional_searches_are_strict();
    std::cout << "[SPGIST] missing remove accounting OK\n";
    std::cout << "[SPGIST] exact radius filtering OK\n";
    std::cout << "[SPGIST] duplicate-point depth guard OK\n";
    std::cout << "[SPGIST] full-precision coordinates OK\n";
    std::cout << "[SPGIST] dynamic world bounds OK\n";
    std::cout << "[SPGIST] strict directional predicates OK\n";
    return 0;
}
