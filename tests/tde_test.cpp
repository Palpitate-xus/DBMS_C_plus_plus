// ============================================================================
// tde_test — P2-8 transparent data encryption (sidecar envelope model):
//   seal/open round trip: full-page ciphertext, decrypt restores the page
//     byte-for-byte
//   tamper detection: one flipped ciphertext byte fails the MAC
//   page binding: a sealed page replayed at another pageId fails the MAC
//   fresh nonce per seal: two seals of one page differ (page + record)
//   key separation: a page sealed under one key fails to open under another
//   keyring lifecycle: created with 0600 perms, reloaded, malformed rejected
//   engine end-to-end (BufferPool + PageAllocator): a heap page written
//     with TDE on is ciphertext at rest (marker absent from the whole
//     file, sidecar record non-zero) and reads back identical; the
//     structural page 0 stays plaintext; a wrong key fails closed
// ============================================================================

#include "storage/PageCrypto.h"
#include "storage/PageAllocator.h"
#include "storage/PageWrapper.h"
#include "storage/PgPage.h"
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <vector>

using namespace dbms;
namespace fs = std::filesystem;

static const size_t kPage = PgPage::PAGE_SIZE;

static void test_seal_open() {
    assert(PageCrypto::enable(std::string(64, 'a')));
    assert(PageCrypto::enabled());

    std::vector<char> page(kPage);
    for (size_t i = 0; i < kPage; ++i) page[i] = static_cast<char>((i * 7 + 3) & 0xFF);
    const std::vector<char> original = page;

    uint8_t record[PageCrypto::kRecordSize];
    PageCrypto::clearRecord(record);
    assert(PageCrypto::isPlaintextRecord(record));

    assert(PageCrypto::sealPage(5, page.data(), kPage, record));
    assert(!PageCrypto::isPlaintextRecord(record));
    // Whole page is ciphertext now.
    assert(std::memcmp(page.data(), original.data(), kPage) != 0);

    // Open restores the page byte-for-byte.
    assert(PageCrypto::openPage(5, page.data(), kPage, record));
    assert(std::memcmp(page.data(), original.data(), kPage) == 0);

    // Tamper with one ciphertext byte -> MAC failure on open.
    assert(PageCrypto::sealPage(5, page.data(), kPage, record));
    page[100] = static_cast<char>(page[100] ^ 0x01);
    assert(!PageCrypto::openPage(5, page.data(), kPage, record));

    // Page binding: replay the sealed page at another pageId fails.
    assert(PageCrypto::sealPage(5, page.data(), kPage, record));
    assert(!PageCrypto::openPage(6, page.data(), kPage, record));
    assert(PageCrypto::openPage(5, page.data(), kPage, record));

    // Fresh nonce per seal: two seals of the same page differ.
    std::vector<char> first = page;
    uint8_t record2[PageCrypto::kRecordSize];
    assert(PageCrypto::sealPage(5, page.data(), kPage, record2));
    assert(std::memcmp(first.data(), page.data(), kPage) != 0);
    assert(std::memcmp(record, record2, PageCrypto::kNonceSize) != 0);

    // Key separation.
    assert(PageCrypto::enable(std::string(64, '3')));
    assert(PageCrypto::sealPage(5, page.data(), kPage, record));
    assert(PageCrypto::enable(std::string(64, '9')));
    assert(!PageCrypto::openPage(5, page.data(), kPage, record));

    // Disabled crypto: seal and open are both refused.
    PageCrypto::disable();
    assert(!PageCrypto::sealPage(5, page.data(), kPage, record));
    assert(!PageCrypto::openPage(5, page.data(), kPage, record));
    assert(PageCrypto::enable(std::string(64, 'a')));

    std::cout << "[TDE] seal/open, tamper, binding, keys OK" << std::endl;
}

static void test_keyring() {
    const fs::path ring = fs::temp_directory_path() / "tde_test.keyring";
    fs::remove(ring);

    std::string error;
    // First load creates the keyring.
    assert(PageCrypto::loadKeyFromFile(ring.string(), error));
    assert(error.empty());
    assert(PageCrypto::enabled());
    assert(fs::exists(ring));
    // Keyring has restrictive permissions.
    struct stat st;
    std::memset(&st, 0, sizeof(st));
    assert(::stat(ring.string().c_str(), &st) == 0);
    assert((st.st_mode & 0777) == 0600);

    // Reload: the same key keeps working.
    assert(PageCrypto::loadKeyFromFile(ring.string(), error));

    // Malformed keyring rejected.
    {
        std::ofstream out(ring, std::ios::trunc);
        out << "not-hex!!\n";
    }
    assert(!PageCrypto::loadKeyFromFile(ring.string(), error));
    assert(!error.empty());

    fs::remove(ring);
    std::cout << "[TDE] keyring lifecycle OK" << std::endl;
}

static void test_engine_at_rest() {
    const fs::path dataFile = fs::temp_directory_path() / "tde_test.dt";
    const fs::path sidecar = fs::path(dataFile.string() + ".tde");
    fs::remove(dataFile);
    fs::remove(sidecar);

    assert(PageCrypto::enable(std::string(64, '5')));

    const char marker[] = "TDE-ROUND-TRIP-MARKER";
    {
        PageAllocator pa(dataFile.string(), 64, kPage);
        assert(pa.open());
        const uint32_t pageId = pa.allocPage();
        assert(pageId >= 1);
        char* p = pa.fetchPage(pageId);
        assert(p);
        PageWrapper(p, kPage, pa.formatVersion()).init(pageId);
        uint16_t slotId = 0;
        assert(PageWrapper(p, kPage, pa.formatVersion())
                   .insert(marker, sizeof(marker), slotId));
        pa.markDirty(pageId);
        assert(pa.flush());
        pa.close();
    }

    // At rest: page 1 is ciphertext (marker nowhere in the file) with a
    // non-zero sidecar record; page 0 (header) is plaintext.
    {
        std::ifstream in(dataFile, std::ios::binary);
        std::string all((std::istreambuf_iterator<char>(in)),
                        std::istreambuf_iterator<char>());
        assert(all.size() == 2 * kPage);
        assert(all.find(marker) == std::string::npos);

        std::ifstream sc(sidecar, std::ios::binary);
        assert(sc.good());
        std::string recAll((std::istreambuf_iterator<char>(sc)),
                           std::istreambuf_iterator<char>());
        assert(recAll.size() == 2 * PageCrypto::kRecordSize);
        const auto* r0 = reinterpret_cast<const uint8_t*>(recAll.data());
        const auto* r1 = reinterpret_cast<const uint8_t*>(
            recAll.data() + PageCrypto::kRecordSize);
        assert(PageCrypto::isPlaintextRecord(r0));   // header page unencrypted
        assert(!PageCrypto::isPlaintextRecord(r1));  // data page sealed
    }

    // Reads back identical with the same key.
    {
        PageAllocator pa(dataFile.string(), 64, kPage);
        assert(pa.open());
        char* p = pa.fetchPage(1);
        assert(p);
        const char* row = nullptr;
        size_t rowLen = 0;
        assert(PageWrapper(p, kPage, pa.formatVersion()).read(0, row, rowLen));
        assert(rowLen == sizeof(marker));
        assert(std::memcmp(row, marker, sizeof(marker)) == 0);
        pa.close();
    }

    // Wrong key: the sealed page fails closed (MAC mismatch).
    {
        assert(PageCrypto::enable(std::string(64, '6')));
        PageAllocator pa(dataFile.string(), 64, kPage);
        assert(pa.open());  // page 0 plaintext: header still validates
        char* p = pa.fetchPage(1);
        assert(p == nullptr);
        pa.close();
    }

    fs::remove(dataFile);
    fs::remove(sidecar);
    PageCrypto::disable();
    std::cout << "[TDE] engine at-rest encryption OK" << std::endl;
}

int main() {
    test_seal_open();
    test_keyring();
    test_engine_at_rest();
    PageCrypto::disable();
    std::cout << "[TDE] all tests passed" << std::endl;
    return 0;
}
