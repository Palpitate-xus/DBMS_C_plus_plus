// Recovery integrity test: valid WAL records with invalid images fail closed.

#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include "PgPage.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <vector>

dbms::Config g_config;

using namespace dbms;

static void setupDatabase(const std::string& dbname) {
    StorageEngine engine;
    assert(engine.createDatabase(dbname) == DBStatus::OK);
    TableSchema table;
    table.tablename = "t";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 0, true));
    assert(engine.createTable(dbname, table) == DBStatus::OK);
}

static std::vector<char> indexPayload(const std::string& path,
                                      const std::vector<char>& image) {
    const uint32_t pathLen = static_cast<uint32_t>(path.size());
    const uint64_t imageLen = static_cast<uint64_t>(image.size());
    std::vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<const char*>(&pathLen),
                   reinterpret_cast<const char*>(&pathLen) + sizeof(pathLen));
    payload.insert(payload.end(), path.begin(), path.end());
    payload.insert(payload.end(), reinterpret_cast<const char*>(&imageLen),
                   reinterpret_cast<const char*>(&imageLen) + sizeof(imageLen));
    payload.insert(payload.end(), image.begin(), image.end());
    return payload;
}

static void appendIndexRecord(const std::string& dbname,
                              const std::vector<char>& payload) {
    WALManager wal(std::filesystem::path(dbname) / "pg_wal");
    assert(wal.ensureOpen());
    const Lsn lsn = wal.XLogInsert(RM_INDEX_ID, XLOG_INDEX_FILE_AFTER, 0, payload);
    assert(lsn != INVALID_LSN);
    assert(wal.XLogFlush(lsn));
}

static void appendHeapImageRecord(const std::string& dbname,
                                  const std::vector<char>& image,
                                  uint32_t blockNum = 1) {
    WALManager wal(std::filesystem::path(dbname) / "pg_wal");
    assert(wal.ensureOpen());
    const std::string tableName = "t";
    const uint32_t nameLen = static_cast<uint32_t>(tableName.size());
    const uint32_t forkNum = 0;
    const uint16_t slotId = 0;
    const uint32_t imageLen = static_cast<uint32_t>(image.size());
    std::vector<char> payload;
    payload.insert(payload.end(), reinterpret_cast<const char*>(&nameLen),
                   reinterpret_cast<const char*>(&nameLen) + sizeof(nameLen));
    payload.insert(payload.end(), tableName.begin(), tableName.end());
    payload.insert(payload.end(), reinterpret_cast<const char*>(&blockNum),
                   reinterpret_cast<const char*>(&blockNum) + sizeof(blockNum));
    payload.insert(payload.end(), reinterpret_cast<const char*>(&forkNum),
                   reinterpret_cast<const char*>(&forkNum) + sizeof(forkNum));
    payload.insert(payload.end(), reinterpret_cast<const char*>(&slotId),
                   reinterpret_cast<const char*>(&slotId) + sizeof(slotId));
    payload.insert(payload.end(), reinterpret_cast<const char*>(&imageLen),
                   reinterpret_cast<const char*>(&imageLen) + sizeof(imageLen));
    payload.insert(payload.end(), image.begin(), image.end());
    const Lsn lsn = wal.XLogInsert(RM_HEAP_ID, XLOG_HEAP_PAGE_AFTER, 0, payload);
    assert(lsn != INVALID_LSN);
    assert(wal.XLogFlush(lsn));
}

static void expectRecoveryFailure() {
    bool failedClosed = false;
    try {
        StorageEngine recovered;
    } catch (const std::runtime_error&) {
        failedClosed = true;
    }
    assert(failedClosed);
}

static void test_wal_record_chain_and_corrupt_length() {
    const std::filesystem::path walDir = "recovery_integrity_wal";
    std::filesystem::remove_all(walDir);
    {
        WALManager wal(walDir);
        assert(wal.ensureOpen());
        const Lsn first = wal.XLogInsert(RM_CHECKPOINT_ID, XLOG_CHECKPOINT_ONLINE, 0, {});
        const Lsn second = wal.XLogInsert(RM_CHECKPOINT_ID, XLOG_CHECKPOINT_ONLINE, 0, {'x'});
        assert(first == 0);
        assert(second > first);
        assert(wal.XLogFlush(second));
    }
    const auto segment = walDir / "000000010000000000000000";
    {
        std::fstream file(segment, std::ios::in | std::ios::out | std::ios::binary);
        assert(file);
        file.seekp(static_cast<std::streamoff>(sizeof(uint64_t)));
        const uint32_t corruptLength = 0xFFFFFFFFu;
        file.write(reinterpret_cast<const char*>(&corruptLength), sizeof(corruptLength));
        assert(file);
    }
    WALManager corrupted(walDir);
    assert(!corrupted.ensureOpen());
    std::filesystem::remove_all(walDir);

    {
        WALManager walWithBadPrev(walDir);
        assert(walWithBadPrev.ensureOpen());
        const Lsn first = walWithBadPrev.XLogInsert(
            RM_CHECKPOINT_ID, XLOG_CHECKPOINT_ONLINE, 0, {});
        assert(first == 0);
        assert(walWithBadPrev.XLogInsert(
            RM_CHECKPOINT_ID, XLOG_CHECKPOINT_ONLINE, 0, {'x'}) != INVALID_LSN);
        assert(walWithBadPrev.XLogFlush(walWithBadPrev.currentWriteLsn()));
    }
    {
        std::fstream file(segment, std::ios::in | std::ios::out | std::ios::binary);
        assert(file);
        const uint64_t badPrevious = 0x1000;
        file.seekp(static_cast<std::streamoff>(32 + offsetof(XLogRecHeader, xl_prev)));
        file.write(reinterpret_cast<const char*>(&badPrevious), sizeof(badPrevious));
        assert(file);
    }
    WALManager badChain(walDir);
    assert(!badChain.ensureOpen());
    std::filesystem::remove_all(walDir);
}

static void test_heap_image_boundaries() {
    alignas(8) char validPage[PgPage::PAGE_SIZE] = {};
    PgPage page(validPage);
    page.init(1);

    const std::filesystem::path walDir = "recovery_integrity_heap_wal";
    std::filesystem::remove_all(walDir);
    WALManager wal(walDir);
    assert(wal.ensureOpen());

    auto payload = std::vector<char>{};
    const uint32_t nameLen = 1;
    const uint32_t blockNum = 1;
    const uint32_t forkNum = 0;
    const uint16_t slotId = 0;
    const uint32_t pageLen = PgPage::PAGE_SIZE;
    payload.insert(payload.end(), reinterpret_cast<const char*>(&nameLen),
                   reinterpret_cast<const char*>(&nameLen) + sizeof(nameLen));
    payload.push_back('t');
    payload.insert(payload.end(), reinterpret_cast<const char*>(&blockNum),
                   reinterpret_cast<const char*>(&blockNum) + sizeof(blockNum));
    payload.insert(payload.end(), reinterpret_cast<const char*>(&forkNum),
                   reinterpret_cast<const char*>(&forkNum) + sizeof(forkNum));
    payload.insert(payload.end(), reinterpret_cast<const char*>(&slotId),
                   reinterpret_cast<const char*>(&slotId) + sizeof(slotId));
    payload.insert(payload.end(), reinterpret_cast<const char*>(&pageLen),
                   reinterpret_cast<const char*>(&pageLen) + sizeof(pageLen));
    payload.insert(payload.end(), validPage, validPage + sizeof(validPage));
    const Lsn validLsn = wal.XLogInsert(RM_HEAP_ID, XLOG_HEAP_PAGE_AFTER, 0, payload);
    assert(validLsn != INVALID_LSN);

    payload.back() ^= 0x01; // CRC remains valid, page checksum does not.
    assert(wal.XLogInsert(RM_HEAP_ID, XLOG_HEAP_PAGE_AFTER, 0, payload) != INVALID_LSN);

    std::filesystem::remove_all(walDir);
}

int main() {
    const std::string malformedDb = "recovery_integrity_malformed_db";
    const std::string unsafeDb = "recovery_integrity_unsafe_db";
    const std::string malformedHeapDb = "recovery_integrity_malformed_heap_db";
    std::filesystem::remove_all(malformedDb);
    std::filesystem::remove_all(unsafeDb);
    std::filesystem::remove_all(malformedHeapDb);
    std::filesystem::remove_all(".txnid");

    test_wal_record_chain_and_corrupt_length();
    std::cout << "[RECOVERY INTEGRITY] WAL length/chain corruption fails closed OK\n";
    test_heap_image_boundaries();
    std::cout << "[RECOVERY INTEGRITY] heap image checksum boundary encoded OK\n";

    setupDatabase(malformedHeapDb);
    std::vector<char> invalidPage(PgPage::PAGE_SIZE, 0);
    PgPage heapPage(invalidPage.data());
    heapPage.init(1);
    invalidPage[sizeof(PgPage::PageHeaderData) + 1] ^= 0x01;
    appendHeapImageRecord(malformedHeapDb, invalidPage);
    expectRecoveryFailure();
    std::cout << "[RECOVERY INTEGRITY] malformed heap image fails closed OK\n";
    std::filesystem::remove_all(malformedHeapDb);

    setupDatabase(malformedHeapDb);
    PgPage validHeapPage(invalidPage.data());
    validHeapPage.init(1);
    appendHeapImageRecord(malformedHeapDb, invalidPage, 2);
    expectRecoveryFailure();
    std::cout << "[RECOVERY INTEGRITY] non-contiguous heap page ID fails closed OK\n";
    std::filesystem::remove_all(malformedHeapDb);

    setupDatabase(malformedDb);
    appendIndexRecord(malformedDb, std::vector<char>{0x01, 0x02});
    expectRecoveryFailure();
    std::cout << "[RECOVERY INTEGRITY] malformed image fails closed OK\n";
    std::filesystem::remove_all(malformedDb);

    setupDatabase(unsafeDb);
    const auto outsidePath = std::filesystem::current_path().parent_path() /
                             "recovery_integrity_escape.idx";
    std::filesystem::remove(outsidePath);
    const auto payload = indexPayload(outsidePath.string(), {});
    appendIndexRecord(unsafeDb, payload);
    expectRecoveryFailure();
    assert(!std::filesystem::exists(outsidePath));
    std::cout << "[RECOVERY INTEGRITY] unsafe index path rejected OK\n";

    std::filesystem::remove_all(malformedDb);
    std::filesystem::remove_all(unsafeDb);
    std::filesystem::remove_all(malformedHeapDb);
    std::filesystem::remove_all(".txnid");
    std::cout << "[RECOVERY INTEGRITY] all passed\n";
    return 0;
}
