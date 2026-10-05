#include "PgPage.h"
#include "PageAllocator.h"
#include "Config.h"
#include <array>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

// Required by StorageEngine when linking the default shared source list
dbms::Config g_config;
using namespace dbms;

static void test_compute_checksum_basic() {
    // Fletcher-16 over a zero-filled buffer should be 0
    char zeros[PgPage::PAGE_SIZE] = {};
    uint16_t cs1 = PgPage::computeChecksum(zeros, sizeof(zeros));
    assert(cs1 == 0);

    // Fletcher-16 over an ascending pattern
    char buf[PgPage::PAGE_SIZE];
    for (size_t i = 0; i < sizeof(buf); ++i) {
        buf[i] = static_cast<char>(i & 0xFF);
    }
    uint16_t cs2 = PgPage::computeChecksum(buf, sizeof(buf));
    assert(cs2 != 0);

    // Same data -> same checksum
    uint16_t cs3 = PgPage::computeChecksum(buf, sizeof(buf));
    assert(cs2 == cs3);

    std::cout << "[CHECKSUM] compute basic OK\n";
}

static void test_page_init_and_verify() {
    alignas(8) char buf[PgPage::PAGE_SIZE] = {};
    PgPage page(buf);
    page.init(1);

    assert(page.verifyChecksum());
    assert(page.header()->pd_checksum != 0);
    assert(page.hasBoundPageId());
    assert(page.isValid(1));
    assert(!page.isValid(2));

    // Corrupt a byte in the data area and verify checksum fails.
    // Use += 1 instead of XOR 0xFF because 0xFF ≡ 0 (mod 255) would not
    // change a Fletcher-16 (mod 255) checksum.
    buf[sizeof(PgPage::PageHeaderData) + 10] += 1;
    assert(!page.verifyChecksum());

    // Restore and recompute
    buf[sizeof(PgPage::PageHeaderData) + 10] -= 1;
    page.writeChecksum();
    assert(page.verifyChecksum());

    std::cout << "[CHECKSUM] init and verify OK\n";
}

static void test_insert_keeps_checksum_valid() {
    alignas(8) char buf[PgPage::PAGE_SIZE] = {};
    PgPage page(buf);
    page.init(1);

    const char payload[] = "hello, checksum world";
    OffsetNumber linePtr = 0;
    bool ok = page.insert(payload, sizeof(payload), linePtr);
    assert(ok);
    assert(linePtr == 1);
    assert(page.verifyChecksum());

    // Corrupt the payload and verify failure
    const char* data = nullptr;
    size_t len = 0;
    ok = page.get(linePtr, data, len);
    assert(ok);
    char* mutableData = const_cast<char*>(data);
    mutableData[0] ^= 0xFF;
    assert(!page.verifyChecksum());

    // Restore
    mutableData[0] ^= 0xFF;
    page.writeChecksum();
    assert(page.verifyChecksum());

    std::cout << "[CHECKSUM] insert keeps checksum OK\n";
}

static void test_remove_and_update_checksum() {
    alignas(8) char buf[PgPage::PAGE_SIZE] = {};
    PgPage page(buf);
    page.init(1);

    const char payload[] = "row to update";
    OffsetNumber linePtr = 0;
    bool ok = page.insert(payload, sizeof(payload), linePtr);
    assert(ok);

    ok = page.remove(linePtr);
    assert(ok);
    assert(page.verifyChecksum());

    ok = page.restore(linePtr);
    assert(ok);
    assert(page.verifyChecksum());

    const char newPayload[] = "updated row with more bytes";
    OffsetNumber newLinePtr = 0;
    ok = page.update(linePtr, newPayload, sizeof(newPayload), &newLinePtr);
    assert(ok);
    assert(page.verifyChecksum());

    std::cout << "[CHECKSUM] remove/update/restore OK\n";
}

static void test_zero_checksum_rejected() {
    alignas(8) char buf[PgPage::PAGE_SIZE] = {};
    PgPage page(buf);
    page.init(1);
    page.header()->pd_checksum = 0;
    assert(!page.verifyChecksum()); // clean-storage format rejects unchecked pages

    std::cout << "[CHECKSUM] zero checksum rejected OK\n";
}

static void test_allocator_rejects_corrupt_storage() {
    const std::filesystem::path path = "checksum_allocator.dat";
    std::filesystem::remove(path);
    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        const uint32_t pageId = allocator.allocPage();
        assert(pageId == 1);
        assert(allocator.flush());
    }

    // Corrupt the tuple page after it has been durably written.  The allocator
    // must fail closed instead of returning a page whose checksum is invalid.
    {
        std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
        assert(file);
        file.seekp(static_cast<std::streamoff>(PgPage::PAGE_SIZE +
                                               sizeof(PgPage::PageHeaderData) + 1));
        char byte = 0;
        file.read(&byte, 1);
        assert(file);
        file.clear();
        byte ^= 0x01;
        file.seekp(static_cast<std::streamoff>(PgPage::PAGE_SIZE +
                                               sizeof(PgPage::PageHeaderData) + 1));
        file.write(&byte, 1);
        assert(file);
    }
    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        assert(allocator.fetchPage(1) == nullptr);
    }
    std::filesystem::remove(path);
    std::cout << "[CHECKSUM] allocator rejects corrupt page OK\n";
}

static void test_compaction_preserves_special_space() {
    alignas(8) char buf[PgPage::PAGE_SIZE] = {};
    PgPage page(buf);
    page.init(1);
    const char first[] = "first row";
    const char second[] = "second row";
    OffsetNumber firstLp = 0;
    OffsetNumber secondLp = 0;
    assert(page.insert(first, sizeof(first), firstLp));
    assert(page.insert(second, sizeof(second), secondLp));
    assert(page.remove(firstLp));
    page.compact();
    assert(page.isValid());
    const char* data = nullptr;
    size_t len = 0;
    assert(page.get(secondLp, data, len));
    assert(std::string(data, len) == std::string(second, sizeof(second)));
    assert(page.nextPage() == 0);
    std::cout << "[CHECKSUM] compaction preserves special space OK\n";
}

static void test_allocator_rejects_corrupt_header_and_truncation() {
    const std::filesystem::path path = "checksum_header.dat";
    std::filesystem::remove(path);
    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        assert(allocator.allocPage() == 1);
        assert(allocator.flush());
    }

    {
        std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
        assert(file);
        file.seekp(static_cast<std::streamoff>(offsetof(DataFileHeader, headerChecksum)));
        uint32_t checksum = 0;
        file.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));
        assert(file);
    }
    {
        PageAllocator allocator(path.string(), 32);
        assert(!allocator.open());
    }

    // Recreate a valid current-format file, then truncate one byte.  Opening
    // must reject it instead of silently zero-filling a partial page.
    std::filesystem::remove(path);
    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        assert(allocator.allocPage() == 1);
        assert(allocator.flush());
    }
    const auto size = std::filesystem::file_size(path);
    assert(size > 1);
    std::filesystem::resize_file(path, size - 1);
    {
        PageAllocator allocator(path.string(), 32);
        assert(!allocator.open());
    }
    std::filesystem::remove(path);
    std::cout << "[CHECKSUM] allocator rejects corrupt header/truncation OK\n";
}

static void test_allocator_rejects_swapped_pages() {
    const std::filesystem::path path = "checksum_swapped_pages.dat";
    std::filesystem::remove(path);
    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        assert(allocator.allocPage() == 1);
        assert(allocator.allocPage() == 2);
        const char* rows[] = {"first page", "second page"};
        for (uint32_t pageId = 1; pageId <= 2; ++pageId) {
            char* data = allocator.fetchPage(pageId);
            assert(data);
            PageWrapper page(data, PgPage::PAGE_SIZE,
                             allocator.formatVersion());
            uint16_t slot = 0;
            const char* row = rows[pageId - 1];
            assert(page.insert(row, std::strlen(row), slot));
            allocator.markDirty(pageId);
            allocator.unpinPage(pageId);
        }
        assert(allocator.flush());
    }

    {
        std::fstream file(path, std::ios::in | std::ios::out |
                                std::ios::binary);
        assert(file);
        std::array<char, PgPage::PAGE_SIZE> first{};
        std::array<char, PgPage::PAGE_SIZE> second{};
        file.seekg(static_cast<std::streamoff>(PgPage::PAGE_SIZE));
        file.read(first.data(), first.size());
        assert(file);
        file.seekg(static_cast<std::streamoff>(2 * PgPage::PAGE_SIZE));
        file.read(second.data(), second.size());
        assert(file);
        file.clear();
        file.seekp(static_cast<std::streamoff>(PgPage::PAGE_SIZE));
        file.write(second.data(), second.size());
        assert(file);
        file.seekp(static_cast<std::streamoff>(2 * PgPage::PAGE_SIZE));
        file.write(first.data(), first.size());
        assert(file);
    }

    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        assert(allocator.fetchPage(1) == nullptr);
        assert(allocator.fetchPage(2) == nullptr);
    }
    std::filesystem::remove(path);
    std::cout << "[CHECKSUM] allocator rejects swapped pages OK\n";
}

static void test_allocator_upgrades_legacy_page_identity() {
    const std::filesystem::path path = "checksum_legacy_identity.dat";
    std::filesystem::remove(path);
    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        assert(allocator.allocPage() == 1);
        char* data = allocator.fetchPage(1);
        assert(data);
        PageWrapper page(data, PgPage::PAGE_SIZE, allocator.formatVersion());
        const char row[] = "legacy row";
        uint16_t slot = 0;
        assert(page.insert(row, sizeof(row), slot));
        allocator.markDirty(1);
        allocator.unpinPage(1);
        assert(allocator.flush());
    }

    // Emulate a page written by layout v4, which used this 4-byte field for
    // unused prune metadata and had no block identity.
    {
        std::fstream file(path, std::ios::in | std::ios::out |
                                std::ios::binary);
        assert(file);
        std::array<char, PgPage::PAGE_SIZE> legacy{};
        file.seekg(static_cast<std::streamoff>(PgPage::PAGE_SIZE));
        file.read(legacy.data(), legacy.size());
        assert(file);
        PgPage page(legacy.data());
        page.header()->pd_flags &= ~PgPage::PD_PAGE_ID_BOUND;
        page.header()->pd_page_id = 0;
        page.header()->pd_pagesize_version = static_cast<uint16_t>(
            (PgPage::PAGE_SIZE / 512 << 8) |
            PgPage::LEGACY_PAGE_LAYOUT_VERSION);
        page.writeChecksum();
        file.clear();
        file.seekp(static_cast<std::streamoff>(PgPage::PAGE_SIZE));
        file.write(legacy.data(), legacy.size());
        assert(file);
    }

    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        char* data = allocator.fetchPage(1);
        assert(data);
        PageWrapper page(data, PgPage::PAGE_SIZE, allocator.formatVersion());
        assert(!page.hasBoundPageId());
        assert(page.isValid());
        assert(!page.isValid(1));
        const char* row = nullptr;
        size_t rowSize = 0;
        assert(page.read(0, row, rowSize));
        assert(std::string(row, rowSize) == std::string("legacy row", 11));
        const char appended[] = "new row";
        uint16_t appendedSlot = 0;
        assert(page.insert(appended, sizeof(appended), appendedSlot));
        assert(appendedSlot == 1);
        allocator.markDirty(1);
        assert(page.hasBoundPageId());
        assert(page.isValid(1));
        allocator.unpinPage(1);
        assert(allocator.flush());
    }
    {
        PageAllocator allocator(path.string(), 32);
        assert(allocator.open());
        char* data = allocator.fetchPage(1);
        assert(data);
        PageWrapper page(data, PgPage::PAGE_SIZE, allocator.formatVersion());
        assert(page.isValid(1));
        const char* row = nullptr;
        size_t rowSize = 0;
        assert(page.read(1, row, rowSize));
        assert(std::string(row, rowSize) == std::string("new row", 8));
        allocator.unpinPage(1);
    }
    std::filesystem::remove(path);
    std::cout << "[CHECKSUM] legacy page identity upgrade OK\n";
}

int main() {
    test_compute_checksum_basic();
    test_page_init_and_verify();
    test_insert_keeps_checksum_valid();
    test_remove_and_update_checksum();
    test_zero_checksum_rejected();
    test_allocator_rejects_corrupt_storage();
    test_compaction_preserves_special_space();
    test_allocator_rejects_corrupt_header_and_truncation();
    test_allocator_rejects_swapped_pages();
    test_allocator_upgrades_legacy_page_identity();

    std::cout << "[CHECKSUM] all passed\n";
    return 0;
}
