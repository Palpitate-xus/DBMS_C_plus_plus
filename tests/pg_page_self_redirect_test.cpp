// test_sources: src/storage/PgPage.cpp src/storage/PageWrapper.cpp
#include "storage/PgPage.h"
#include "storage/PageWrapper.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace dbms;

namespace {
struct alignas(8) Buffer { std::array<char, PgPage::PAGE_SIZE> bytes{}; };
using Rows = std::vector<std::vector<char>>;
size_t controls = 0, failures = 0, selfAttempts = 0, positiveControls = 0, legacyControls = 0;

void require(bool good) {
    if (!good) throw std::runtime_error("CONTROL_ERROR: fixture setup failed");
}

void check(bool good, const std::string& label) {
    ++controls;
    if (!good) {
        ++failures;
        if (failures <= 16) std::cerr << "FAIL " << label << '\n';
    }
}

bool redirect(Buffer& buffer, bool wrapped, OffsetNumber from, OffsetNumber to) {
    PageWrapper wrapper(buffer.bytes.data(), PgPage::PAGE_SIZE, DATA_FILE_FORMAT_VERSION);
    return wrapped ? wrapper.redirect(static_cast<uint16_t>(from - 1), static_cast<uint16_t>(to - 1))
                   : PgPage(buffer.bytes.data()).redirect(from, to);
}

Rows initialize(Buffer& buffer, size_t count, size_t length) {
    PgPage page(buffer.bytes.data());
    page.init(41);
    page.header()->pd_lsn = 1234567;
    page.setNextPage(73);
    Rows rows(count + 1);
    for (size_t i = 1; i <= count; ++i) {
        rows[i].resize(length);
        for (size_t j = 0; j < length; ++j)
            rows[i][j] = static_cast<char>((i * 31 + j * 73) & 255);
        OffsetNumber slot = 0;
        require(page.insert(rows[i].data(), rows[i].size(), slot) && slot == i);
    }
    require(page.isValid(41));
    return rows;
}

size_t highestUsed(const Rows& rows) {
    size_t last = rows.size() - 1;
    while (last && rows[last].empty()) --last;
    return last;
}

bool verify(Buffer& buffer, bool wrapped, const Rows& rows, size_t live, size_t slots) {
    PgPage page(buffer.bytes.data());
    PageWrapper wrapper(buffer.bytes.data(), PgPage::PAGE_SIZE, DATA_FILE_FORMAT_VERSION);
    bool good = page.isValid(41);
    good &= page.verifyChecksum();
    good &= page.header()->pd_lsn == 1234567;
    good &= page.nextPage() == 73;
    good &= page.liveCount() == live;
    good &= page.numLinePointers() == slots;
    const char sentinel = 'x';
    for (size_t i = 1; i < rows.size(); ++i) {
        const char* bytes = &sentinel;
        size_t length = 98765;
        const bool found = wrapped ? wrapper.read(static_cast<uint16_t>(i - 1), bytes, length)
                                   : page.get(static_cast<OffsetNumber>(i), bytes, length);
        if (rows[i].empty()) {
            good &= !found && bytes == &sentinel && length == 98765;
        } else {
            good &= found && length == rows[i].size() &&
                    std::memcmp(bytes, rows[i].data(), rows[i].size()) == 0;
        }
    }
    return good;
}

void selfCases(size_t count, size_t length, bool wrapped, bool precompact) {
    Buffer original;
    const auto initial = initialize(original, count, length);
    for (OffsetNumber target = 1; target <= count; ++target) {
        for (int shape = 0; shape < 3; ++shape) {
            if (shape != 0 && count < 2) continue;
            Buffer buffer = original;
            auto rows = initial;
            size_t live = count, slots = count;
            const OffsetNumber other = static_cast<OffsetNumber>(target % count + 1);
            PgPage page(buffer.bytes.data());
            if (shape == 1) {
                require(page.remove(other));
                rows[other].clear();
                --live;
            } else if (shape == 2) {
                require(redirect(buffer, wrapped, other, target));
                rows[other] = rows[target];
                --live;
            }
            if (precompact) {
                page.compact();
                slots = highestUsed(rows);
            }
            require(verify(buffer, wrapped, rows, live, slots));
            const Buffer before = buffer;
            const std::string label = std::string(wrapped ? "wrapper" : "page") +
                "/count=" + std::to_string(count) + "/len=" + std::to_string(length) +
                "/target=" + std::to_string(target) + "/shape=" + std::to_string(shape) +
                "/precompact=" + std::to_string(precompact);
            ++selfAttempts;
            const bool accepted = redirect(buffer, wrapped, target, target);
            const bool unchanged = buffer.bytes == before.bytes;
            const bool readable = verify(buffer, wrapped, rows, live, slots);
            check(!accepted && unchanged && readable, label + "/reject-without-data-loss");
            page.compact();
            slots = highestUsed(rows);
            check(verify(buffer, wrapped, rows, live, slots), label + "/compact-after-rejection");

            OffsetNumber destination = 0;
            for (OffsetNumber i = 1; i <= count; ++i) {
                if (i != target && !(shape != 0 && i == other)) {
                    destination = i;
                    break;
                }
            }
            if (destination) {
                const bool retried = redirect(buffer, wrapped, target, destination);
                rows[target] = rows[destination];
                if (shape == 2) rows[other] = rows[destination];
                --live;
                const bool beforeCompact = verify(buffer, wrapped, rows, live, slots);
                page.compact();
                const bool afterCompact = verify(buffer, wrapped, rows, live, highestUsed(rows));
                check(retried && beforeCompact && afterCompact, label + "/distinct-retry-and-compact");
            }
        }
    }
}

void distinctCases(size_t count, size_t length, bool wrapped, bool precompact) {
    Buffer original;
    const auto initial = initialize(original, count, length);
    for (OffsetNumber from = 1; from <= count; ++from) {
        for (OffsetNumber to = 1; to <= count; ++to) {
            if (from == to) continue;
            Buffer buffer = original;
            PgPage page(buffer.bytes.data());
            if (precompact) page.compact();
            auto rows = initial;
            const bool accepted = redirect(buffer, wrapped, from, to);
            rows[from] = rows[to];
            const bool beforeCompact = verify(buffer, wrapped, rows, count - 1, count);
            page.compact();
            const bool afterCompact = verify(buffer, wrapped, rows, count - 1, count);
            ++positiveControls;
            check(accepted && beforeCompact && afterCompact, "distinct-normal-slots");
        }
    }
}

void legacyCases(size_t length, bool wrapped, bool precompact) {
    Buffer original;
    (void)initialize(original, 3, length);
    if (precompact) PgPage(original.bytes.data()).compact();
    const auto reject = [&](Buffer& buffer, OffsetNumber from, OffsetNumber to) {
        const Buffer before = buffer;
        const bool accepted = redirect(buffer, wrapped, from, to);
        ++legacyControls;
        check(!accepted && buffer.bytes == before.bytes, "existing-invalid-slot-or-state-rejection");
    };
    for (const auto& pair : std::vector<std::array<OffsetNumber, 2>>{
            {0, 1}, {1, 0}, {4, 1}, {1, 4}, {65535, 1}, {1, 65535}}) {
        Buffer buffer = original;
        reject(buffer, pair[0], pair[1]);
    }
    Buffer removed = original;
    require(PgPage(removed.bytes.data()).remove(2));
    reject(removed, 2, 2); reject(removed, 1, 2); reject(removed, 2, 1);
    Buffer redirected = original;
    require(redirect(redirected, wrapped, 1, 2));
    reject(redirected, 1, 1); reject(redirected, 1, 3); reject(redirected, 3, 1);
}
} // namespace

int main() {
    try {
        for (size_t length : {size_t{1}, size_t{23}, size_t{257}}) {
            const size_t capacity = (PgPage::PAGE_SIZE - sizeof(uint32_t) -
                sizeof(PgPage::PageHeaderData)) / (sizeof(PgPage::ItemIdData) + length);
            for (bool wrapped : {false, true}) for (bool precompact : {false, true}) {
                for (size_t count : {size_t{1}, size_t{2}, size_t{17}, capacity})
                    selfCases(count, length, wrapped, precompact);
                for (size_t count : {size_t{2}, size_t{3}, size_t{17}})
                    distinctCases(count, length, wrapped, precompact);
                legacyCases(length, wrapped, precompact);
            }
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
    if (controls != 76272 || selfAttempts != 24276 || positiveControls != 3360 || legacyControls != 144) {
        std::cerr << "CONTROL_ERROR: incomplete matrix\n";
        return 2;
    }
    std::cout << "PG_PAGE_SELF_REDIRECT_COMPLETE " << controls << " FAILURES " << failures
              << " SELF_ATTEMPTS " << selfAttempts << " POSITIVES " << positiveControls
              << " LEGACY " << legacyControls << '\n';
    return failures ? 1 : 0;
}
