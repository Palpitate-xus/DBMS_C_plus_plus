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

int main() {
    test_missing_remove_preserves_size();
    test_within_uses_exact_distance();
    std::cout << "[SPGIST] missing remove accounting OK\n";
    std::cout << "[SPGIST] exact radius filtering OK\n";
    return 0;
}
