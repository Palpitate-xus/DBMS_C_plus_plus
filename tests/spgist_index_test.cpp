// test_sources: src/access/SPGiSTIndex.cpp

#include "access/SPGiSTIndex.h"

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

int main() {
    test_missing_remove_preserves_size();
    std::cout << "[SPGIST] missing remove accounting OK\n";
    return 0;
}
