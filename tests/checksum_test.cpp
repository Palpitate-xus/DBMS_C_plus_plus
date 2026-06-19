#include "PgPage.h"
#include "Config.h"
#include <cassert>
#include <cstring>
#include <iostream>

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

static void test_zero_checksum_ignored() {
    alignas(8) char buf[PgPage::PAGE_SIZE] = {};
    PgPage page(buf);
    page.init(1);
    page.header()->pd_checksum = 0;
    assert(page.verifyChecksum()); // 0 means unchecked / legacy

    std::cout << "[CHECKSUM] zero checksum ignored OK\n";
}

int main() {
    test_compute_checksum_basic();
    test_page_init_and_verify();
    test_insert_keeps_checksum_valid();
    test_remove_and_update_checksum();
    test_zero_checksum_ignored();

    std::cout << "[CHECKSUM] all passed\n";
    return 0;
}
