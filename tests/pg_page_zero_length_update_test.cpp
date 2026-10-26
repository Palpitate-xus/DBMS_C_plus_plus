// test_sources: src/storage/PgPage.cpp src/storage/PageWrapper.cpp
#include "storage/PgPage.h"
#include "storage/PageWrapper.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

using namespace dbms;
using Buffer = std::array<char, PgPage::PAGE_SIZE>;
using Rows = std::map<OffsetNumber, std::vector<char>>;

static void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

static std::vector<char> payload(size_t size, unsigned seed) {
    std::vector<char> value(size);
    for (size_t i = 0; i < size; ++i)
        value[i] = static_cast<char>((seed + i * 19) & 0xff);
    return value;
}

static bool rowsEqual(const PgPage& page, const Rows& rows) {
    if (!page.isValid(51) || !page.verifyChecksum() || page.nextPage() != 87 ||
        page.header()->pd_lsn != 123456 || page.liveCount() != rows.size()) return false;
    for (const auto& entry : rows) {
        const char* value = nullptr;
        size_t length = 99999;
        if (!page.get(entry.first, value, length) || length != entry.second.size() ||
            std::memcmp(value, entry.second.data(), length) != 0) return false;
    }
    return true;
}

static OffsetNumber insert(PgPage& page, Rows& rows, std::vector<char> value) {
    OffsetNumber slot = 0;
    require(page.insert(value.data(), value.size(), slot), "setup insert failed");
    require(rows.emplace(slot, std::move(value)).second, "setup reused a live slot");
    return slot;
}

static bool update(unsigned api, PgPage& page, PageWrapper& wrapper,
                   OffsetNumber slot, const std::vector<char>& value,
                   size_t length, uint16_t& output) {
    if (api == 0) return page.update(slot, value.data(), length, &output);
    if (api == 1) return page.update(slot, value.data(), length);
    if (api == 2) return wrapper.update(slot - 1, value.data(), length, output);
    return wrapper.update(slot - 1, value.data(), length);
}

static bool emptyUpdate(unsigned api, size_t oldLength, int room, bool prefix) {
    alignas(8) Buffer bytes{};
    PgPage page(bytes.data());
    PageWrapper wrapper(bytes.data(), PgPage::PAGE_SIZE, DATA_FILE_FORMAT_VERSION);
    page.init(51);
    page.setNextPage(87);
    page.header()->pd_lsn = 123456;
    page.writeChecksum();
    Rows rows;
    if (prefix) insert(page, rows, payload(17, 71));
    const auto original = payload(oldLength, 31);
    const OffsetNumber slot = insert(page, rows, original);
    if (room >= 0) {
        require(page.freeSpace() > static_cast<size_t>(room) + sizeof(PgPage::ItemIdData), "setup capacity");
        insert(page, rows, payload(page.freeSpace() - room - sizeof(PgPage::ItemIdData), 171));
        require(page.freeSpace() == static_cast<size_t>(room), "setup free bytes");
    }
    require(rowsEqual(page, rows), "invalid setup page");
    const Buffer before = bytes;
    const size_t beforeFree = page.freeSpace();
    const uint16_t beforeSlots = page.numLinePointers();
    uint16_t output = 999;
    const bool accepted = update(api, page, wrapper, slot, original, 0, output);
    bool ok = !accepted && output == 999 && bytes == before &&
        page.freeSpace() == beforeFree && page.numLinePointers() == beforeSlots && rowsEqual(page, rows);
    if (!ok) return false; // Preserve the old corruption observation; do not repair the page.

    // A rejected empty update must leave the existing tuple available for a
    // subsequent valid update, including a page with zero spare bytes.
    const auto replacement = payload(oldLength, 97);
    output = 999;
    const bool retry = update(api, page, wrapper, slot, replacement, replacement.size(), output);
    if (api == 0) ok = ok && output == slot;
    if (api == 2) ok = ok && output == slot - 1;
    if (api == 1 || api == 3) ok = ok && output == 999;
    rows[slot] = replacement;
    return ok && retry && rowsEqual(page, rows) && page.freeSpace() == beforeFree &&
        page.numLinePointers() == beforeSlots;
}

static bool nonemptyUpdate(unsigned api, size_t oldLength, bool earlierHole, int shape) {
    alignas(8) Buffer bytes{};
    PgPage page(bytes.data());
    PageWrapper wrapper(bytes.data(), PgPage::PAGE_SIZE, DATA_FILE_FORMAT_VERSION);
    page.init(51);
    page.setNextPage(87);
    page.header()->pd_lsn = 123456;
    page.writeChecksum();
    Rows rows;
    OffsetNumber hole = 0;
    if (earlierHole) hole = insert(page, rows, payload(17, 71));
    const OffsetNumber slot = insert(page, rows, payload(oldLength, 31));
    insert(page, rows, payload(23, 171));
    // Delete the prefix only after all inserts so growing updates can reuse
    // an earlier slot while retaining a separate live neighbor.
    if (earlierHole) {
        require(page.remove(hole), "setup remove failed");
        rows.erase(hole);
    }
    require(rowsEqual(page, rows), "invalid nonempty setup");
    const size_t length = shape == 0 ? 1 : shape == 1 ? oldLength : oldLength + 1;
    const auto replacement = payload(length, 97);
    uint16_t output = 999;
    if (!update(api, page, wrapper, slot, replacement, length, output)) return false;
    const OffsetNumber expected = shape == 2 && earlierHole ? hole : slot;
    if (api == 0 && output != expected) return false;
    if (api == 2 && output != expected - 1) return false;
    if ((api == 1 || api == 3) && output != 999) return false;
    rows.erase(slot);
    rows[expected] = replacement;
    return rowsEqual(page, rows);
}

int main() {
    int controls = 0, failures = 0, emptyControls = 0, emptyFailures = 0;
    try {
        for (unsigned api = 0; api < 4; ++api)
            for (size_t size : {size_t(1), size_t(2), size_t(31), size_t(256), size_t(1024)})
                for (int room : {0, 1, 7, -1})
                    for (bool prefix : {false, true}) {
                        ++controls;
                        ++emptyControls;
                        if (!emptyUpdate(api, size, room, prefix)) {
                            ++failures;
                            ++emptyFailures;
                            std::cout << "EMPTY_UPDATE_FAILED api=" << api << " size=" << size
                                      << " room=" << room << " prefix=" << prefix << '\n';
                        }
                    }
        for (unsigned api = 0; api < 4; ++api)
            for (size_t size : {size_t(1), size_t(2), size_t(31), size_t(256), size_t(1024)})
                for (bool hole : {false, true})
                    for (int shape = 0; shape < 3; ++shape) {
                        ++controls;
                        if (!nonemptyUpdate(api, size, hole, shape)) {
                            ++failures;
                            std::cout << "NONEMPTY_UPDATE_FAILED api=" << api << " size=" << size
                                      << " hole=" << hole << " shape=" << shape << '\n';
                        }
                    }
    } catch (const std::exception& error) {
        std::cerr << "CONTROL_ERROR " << error.what() << '\n';
        return 2;
    }
    require(controls == 280 && emptyControls == 160, "incorrect fixed matrix size");
    std::cout << "PAGE_ZERO_LENGTH_UPDATE_COMPLETE " << controls << " FAILURES " << failures
              << " EMPTY_CONTROLS " << emptyControls << " EMPTY_FAILURES " << emptyFailures << '\n';
    return failures ? 1 : 0;
}
