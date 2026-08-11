// Recovery integrity test: valid WAL records with invalid images fail closed.

#include "TableManage.h"
#include "Config.h"
#include "WAL.h"
#include <cassert>
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

static void expectRecoveryFailure() {
    bool failedClosed = false;
    try {
        StorageEngine recovered;
    } catch (const std::runtime_error&) {
        failedClosed = true;
    }
    assert(failedClosed);
}

int main() {
    const std::string malformedDb = "recovery_integrity_malformed_db";
    const std::string unsafeDb = "recovery_integrity_unsafe_db";
    std::filesystem::remove_all(malformedDb);
    std::filesystem::remove_all(unsafeDb);
    std::filesystem::remove_all(".txnid");

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
    std::filesystem::remove_all(".txnid");
    std::cout << "[RECOVERY INTEGRITY] all passed\n";
    return 0;
}
