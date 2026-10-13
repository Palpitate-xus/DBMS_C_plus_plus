// test_sources: src/storage/PgPage.cpp
#include "storage/PgPage.h"
#include "storage/HeapTupleHeader.h"

#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <vector>

using namespace dbms;

static std::vector<char> tuple(int16_t value) {
    const size_t headerSize = computeHeapHeaderSize(1);
    std::vector<char> bytes(headerSize + sizeof(value), 0);
    auto* header = reinterpret_cast<HeapTupleHeaderData*>(bytes.data());
    initHeapTupleHeader(header, 1, 1);
    setNotNull(header, 0);
    setXminCommitted(header);
    std::memcpy(bytes.data() + headerSize, &value, sizeof(value));
    return bytes;
}

static void verifyRows(const PgPage& page,
                       const std::map<OffsetNumber, std::vector<char>>& rows) {
    assert(page.isValid(41));
    assert(page.verifyChecksum());
    assert(page.nextPage() == 73);
    assert(page.liveCount() == rows.size());
    for (const auto& row : rows) {
        const char* bytes = nullptr;
        size_t length = 0;
        assert(page.get(row.first, bytes, length));
        assert(length == row.second.size());
        assert(std::memcmp(bytes, row.second.data(), length) == 0);
    }
}

static void exercise(int initial, int deletion) {
    alignas(8) std::array<char, PgPage::PAGE_SIZE> buffer{};
    PgPage page(buffer.data());
    page.init(41);
    page.setNextPage(73);
    std::map<OffsetNumber, std::vector<char>> rows;
    std::vector<OffsetNumber> slots;
    for (int i = 0; i < initial; ++i) {
        auto bytes = tuple(static_cast<int16_t>(i + 1));
        OffsetNumber slot = 0;
        assert(page.insert(bytes.data(), bytes.size(), slot));
        slots.push_back(slot);
        rows.emplace(slot, std::move(bytes));
    }
    verifyRows(page, rows);
    std::vector<OffsetNumber> removed;
    for (int i = 0; i < initial; ++i) {
        const bool erase = deletion == 0 ? i == 0 :
                           deletion == 1 ? i == initial / 2 :
                           deletion == 2 ? i == initial - 1 :
                           deletion == 3 ? i % 3 == 0 : true;
        if (erase) {
            assert(page.remove(slots[i]));
            rows.erase(slots[i]);
            removed.push_back(slots[i]);
        }
    }
    verifyRows(page, rows);
    page.compact();
    verifyRows(page, rows);
    const size_t expectedFree = page.header()->pd_special -
        sizeof(PgPage::PageHeaderData) -
        page.numLinePointers() * sizeof(PgPage::ItemIdData) -
        rows.size() * tuple(0).size();
    assert(page.freeSpace() == expectedFree);
    for (auto slot : removed) {
        const char* bytes = nullptr;
        size_t length = 0;
        assert(!page.get(slot, bytes, length));
        assert(!page.restore(slot)); // VACUUM has reclaimed this tuple.
    }
    page.compact();
    verifyRows(page, rows);
    assert(page.freeSpace() == expectedFree);
    for (int i = 0; i < 4; ++i) {
        auto bytes = tuple(static_cast<int16_t>(1000 + i));
        if (!page.canFit(bytes.size())) break;
        OffsetNumber slot = 0;
        assert(page.insert(bytes.data(), bytes.size(), slot));
        assert(rows.emplace(slot, std::move(bytes)).second);
        verifyRows(page, rows);
    }
    std::cout << "[PAGE COMPACTION] initial=" << initial
              << " deletion=" << deletion << " surviving=" << rows.size()
              << " free=" << page.freeSpace() << '\n';
}

static void redirectIdentity() {
    alignas(8) std::array<char, PgPage::PAGE_SIZE> buffer{};
    PgPage page(buffer.data());
    page.init(41);
    page.setNextPage(73);
    auto oldValue = tuple(1);
    auto currentValue = tuple(2);
    OffsetNumber oldSlot = 0, newSlot = 0;
    assert(page.insert(oldValue.data(), oldValue.size(), oldSlot));
    assert(page.insert(currentValue.data(), currentValue.size(), newSlot));
    assert(page.redirect(oldSlot, newSlot));
    page.compact();
    const char* bytes = nullptr;
    size_t length = 0;
    assert(page.get(oldSlot, bytes, length));
    assert(length == currentValue.size());
    assert(std::memcmp(bytes, currentValue.data(), length) == 0);
    assert(page.isValid(41) && page.nextPage() == 73);
    std::cout << "[PAGE COMPACTION] HOT redirect preserved\n";
}

int main() {
    for (int initial : {258, 270, 272, 257, 1})
        for (int deletion = 0; deletion < 5; ++deletion)
            exercise(initial, deletion);
    redirectIdentity();
    std::cout << "[PAGE COMPACTION] all controls passed\n";
}
