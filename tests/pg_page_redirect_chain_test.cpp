// test_sources: src/storage/PgPage.cpp src/storage/PageWrapper.cpp
#include "storage/PgPage.h"
#include "storage/PageWrapper.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace dbms;

namespace {
int controls = 0;
int failures = 0;

void check(bool ok, const std::string& label) {
    ++controls;
    if (!ok) {
        ++failures;
        if (failures <= 20) std::cerr << "FAIL " << label << '\n';
    }
}

void require(bool ok) {
    if (!ok) throw std::runtime_error("CONTROL_ERROR: fixture setup failed");
}

std::vector<char> value(size_t length, unsigned seed) {
    std::vector<char> bytes(length);
    for (size_t i = 0; i < length; ++i)
        bytes[i] = static_cast<char>((seed * 31 + i * 73) & 255);
    return bytes;
}

bool read(PgPage& page, PageWrapper& wrapper, bool wrapped,
          OffsetNumber slot, const char*& data, size_t& length) {
    return wrapped ? wrapper.read(slot - 1, data, length)
                   : page.get(slot, data, length);
}

void verify(char* bytes, bool wrapped, const std::vector<OffsetNumber>& slots,
            const std::vector<char>& expected, const std::string& label) {
    PgPage page(bytes);
    PageWrapper wrapper(bytes, PgPage::PAGE_SIZE, DATA_FILE_FORMAT_VERSION);
    check(page.isValid(41) && page.verifyChecksum() && page.nextPage() == 73 &&
          page.header()->pd_lsn == 1234567 && page.liveCount() == 1 &&
          page.numLinePointers() == slots.size(), label + "/metadata");
    std::array<char, PgPage::PAGE_SIZE> before{};
    std::memcpy(before.data(), bytes, before.size());
    for (auto slot : slots) {
        const char* data = nullptr;
        size_t length = 77777;
        bool ok = read(page, wrapper, wrapped, slot, data, length);
        check(ok && data && length == expected.size() &&
              std::memcmp(data, expected.data(), expected.size()) == 0,
              label + "/slot=" + std::to_string(slot));
    }
    const char sentinel = 'x';
    const char* data = &sentinel;
    size_t length = 77777;
    check(!read(page, wrapper, wrapped, slots.size() + 1, data, length) &&
          data == &sentinel && length == 77777, label + "/missing-slot");
    check(std::memcmp(before.data(), bytes, before.size()) == 0,
          label + "/reads-preserve-entire-page");
}

void exercise(size_t count, size_t length, bool wrapped, bool reverse) {
    alignas(8) std::array<char, PgPage::PAGE_SIZE> bytes{};
    PgPage page(bytes.data());
    PageWrapper wrapper(bytes.data(), PgPage::PAGE_SIZE, DATA_FILE_FORMAT_VERSION);
    page.init(41);
    page.header()->pd_lsn = 1234567;
    page.setNextPage(73);
    std::vector<OffsetNumber> slots;
    for (size_t i = 0; i < count; ++i) {
        auto payload = value(length, i + 1);
        OffsetNumber slot = 0;
        if (wrapped) {
            uint16_t external = 60000;
            require(wrapper.insert(payload.data(), payload.size(), external));
            slot = external + 1;
        } else {
            require(page.insert(payload.data(), payload.size(), slot));
        }
        slots.push_back(slot);
    }
    if (reverse) std::reverse(slots.begin(), slots.end());
    for (size_t i = 0; i + 1 < slots.size(); ++i) {
        require(wrapped ? wrapper.redirect(slots[i] - 1, slots[i + 1] - 1)
                        : page.redirect(slots[i], slots[i + 1]));
    }
    const auto terminal = value(length, slots.back());
    const std::string label = std::string(wrapped ? "wrapper" : "page") +
        (reverse ? "/reverse" : "/forward") + "/len=" +
        std::to_string(length) + "/nodes=" + std::to_string(count);
    verify(bytes.data(), wrapped, slots, terminal, label + "/original");
    if (wrapped) wrapper.compact(); else page.compact();
    verify(bytes.data(), wrapped, slots, terminal, label + "/compacted");
    alignas(8) std::array<char, PgPage::PAGE_SIZE> reopened{};
    std::memcpy(reopened.data(), bytes.data(), bytes.size());
    verify(reopened.data(), wrapped, slots, terminal, label + "/copied-buffer");

    PgPage copy(reopened.data());
    PageWrapper copyWrapper(reopened.data(), PgPage::PAGE_SIZE, DATA_FILE_FORMAT_VERSION);
    auto next = value(length, 97);
    OffsetNumber nextSlot = 0;
    require(copy.insert(next.data(), next.size(), nextSlot));
    require(copy.redirect(slots.back(), nextSlot));
    slots.push_back(nextSlot);
    verify(reopened.data(), wrapped, slots, next, label + "/extended");
    copyWrapper.compact();
    verify(reopened.data(), wrapped, slots, next, label + "/extended-compacted");
}
}

int main() {
    try {
        for (size_t length : {size_t{1}, size_t{23}}) {
            const size_t capacity = (PgPage::PAGE_SIZE - sizeof(uint32_t) -
                sizeof(PgPage::PageHeaderData)) /
                (sizeof(PgPage::ItemIdData) + length);
            std::vector<size_t> counts{1, 2, 3, 15, 16, 17, 18, 32, 64, 128, 256, capacity};
            for (bool wrapped : {false, true})
                for (bool reverse : {false, true})
                    for (size_t count : counts)
                        exercise(count, length, wrapped, reverse);
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
    std::cout << "PG_PAGE_REDIRECT_CHAIN_COMPLETE " << controls
              << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
