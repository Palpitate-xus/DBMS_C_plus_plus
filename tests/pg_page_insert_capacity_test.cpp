// test_sources: src/storage/PgPage.cpp
#include "storage/PgPage.h"

#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <limits>
#include <vector>

using namespace dbms;

static void boundary(size_t available, size_t length, bool reusable) {
    alignas(8) std::array<char, PgPage::PAGE_SIZE> buffer{};
    PgPage page(buffer.data());
    page.init(29);
    page.setNextPage(42);
    const size_t initialFree = page.freeSpace();
    std::vector<char> original(initialFree - sizeof(PgPage::ItemIdData) - available, 'a');
    OffsetNumber first = 0;
    assert(page.insert(original.data(), original.size(), first));
    assert(first == 1 && page.freeSpace() == available);
    if (reusable) assert(page.remove(first));
    const auto before = buffer;
    const size_t slotCost = reusable ? 0 : sizeof(PgPage::ItemIdData);
    const bool expected = length > 0 && length <= available &&
                          slotCost <= available - length;
    std::vector<char> value(length, 'b');
    OffsetNumber inserted = 123;
    assert(page.canFit(length) == expected);
    assert(page.insert(value.data(), value.size(), inserted) == expected);
    if (!expected) {
        assert(inserted == 123 && buffer == before);
        if (reusable) assert(page.restore(first));
    } else {
        assert(inserted == (reusable ? 1 : 2));
        assert(page.freeSpace() == available - length - slotCost);
        const char* bytes = nullptr;
        size_t size = 0;
        assert(page.get(inserted, bytes, size) && size == length);
        assert(std::memcmp(bytes, value.data(), size) == 0);
    }
    assert(page.isValid(29) && page.verifyChecksum() && page.nextPage() == 42);
    if (!expected || !reusable) {
        const char* bytes = nullptr;
        size_t size = 0;
        assert(page.get(first, bytes, size) && size == original.size());
        assert(std::memcmp(bytes, original.data(), size) == 0);
    }
}

int main() {
    // Test failed insert first, independently of canFit, to reproduce the
    // old unsigned subtraction when a normal removed slot is reused.
    alignas(8) std::array<char, PgPage::PAGE_SIZE> buffer{};
    PgPage page(buffer.data());
    page.init(29);
    std::vector<char> full(page.freeSpace() - sizeof(PgPage::ItemIdData), 'x');
    OffsetNumber slot = 0;
    assert(page.insert(full.data(), full.size(), slot));
    assert(page.freeSpace() == 0 && page.remove(slot));
    const auto before = buffer;
    const char value = 'y';
    OffsetNumber rejected = 123;
    assert(!page.insert(&value, 1, rejected));
    assert(rejected == 123 && buffer == before && page.isValid(29));

    int controls = 0;
    for (bool reusable : {false, true})
        for (size_t free = 0; free <= 10; ++free)
            for (size_t length = 0; length <= 8; ++length) {
                boundary(free, length, reusable);
                ++controls;
            }
    assert(!page.canFit(0));
    assert(!page.canFit(std::numeric_limits<size_t>::max()));
    assert(!page.insert(&value, std::numeric_limits<size_t>::max(), rejected));
    assert(buffer == before);
    std::cout << "[PAGE INSERT CAPACITY] " << controls
              << " boundaries and rejected full-page insert passed\n";
}
