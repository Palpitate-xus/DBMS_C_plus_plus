#include "Config.h"
#include "TableManage.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>

dbms::Config g_config;

using namespace dbms;

static std::string makePayload(size_t size, uint32_t seed) {
    static constexpr char alphabet[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string value(size, '\0');
    uint32_t state = seed;
    for (char& ch : value) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        ch = alphabet[state % (sizeof(alphabet) - 1)];
    }
    return value;
}

int main() {
    const std::string database = "vacuum_toast_db";
    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine writer;
        assert(writer.createDatabase(database) == DBStatus::OK);
        TableSchema table;
        table.tablename = "documents";
        table.formatVersion = 2;
        table.append(makeIntColumn("id", false, 4, true));
        table.append(makeVarCharColumn("payload", false, 12000, false));
        assert(writer.createTable(database, table) == DBStatus::OK);

        const std::string livePayload = makePayload(9000, 0x10203040u);
        const std::string deletedPayload = makePayload(9000, 0x50607080u);
        const std::string updatedPayload = makePayload(9000, 0x90a0b0c0u);
        assert(writer.insert(database, "documents",
                             {{"id", "1"}, {"payload", livePayload}}) ==
               DBStatus::OK);
        assert(writer.insert(database, "documents",
                             {{"id", "2"}, {"payload", deletedPayload}}) ==
               DBStatus::OK);
        assert(writer.readToast(database, "documents", 1) == livePayload);
        assert(writer.readToast(database, "documents", 2) == deletedPayload);

        {
            StorageEngine reader;
            reader.setIsolationLevel(IsolationLevel::REPEATABLE_READ);
            assert(reader.beginTransaction(database) == DBStatus::OK);

            // REPEATABLE READ acquires its snapshot on the first data read,
            // not on BEGIN. Establish the old snapshot before the writer.
            size_t initialRows = 0;
            assert(reader.forEachRow(
                database, "documents",
                [&](uint32_t, uint16_t, const char* data, size_t length) {
                    const std::string row(data, length);
                    const std::string id = reader.extractColumnValue(
                        row, table, 0, database, true);
                    assert(id == "1" || id == "2");
                    assert(reader.extractColumnValue(
                               row, table, 1, database, true) ==
                           (id == "1" ? livePayload : deletedPayload));
                    ++initialRows;
                }));
            assert(initialRows == 2);

            StorageEngine lazyReader;
            lazyReader.setIsolationLevel(IsolationLevel::REPEATABLE_READ);
            assert(lazyReader.beginTransaction(database) == DBStatus::OK);

            assert(writer.beginTransaction(database) == DBStatus::OK);
            assert(writer.update(database, "documents",
                                 {{"payload", updatedPayload}}, {"=id 1"}) ==
                   DBStatus::OK);
            assert(writer.remove(database, "documents", {"=id 2"}) ==
                   DBStatus::OK);
            assert(writer.commitTransaction() == DBStatus::OK);

            // BEGIN alone does not establish an older snapshot. A distinct
            // first reader after COMMIT sees the current row and TOAST value.
            size_t lazyRows = 0;
            assert(lazyReader.forEachRow(
                database, "documents",
                [&](uint32_t, uint16_t, const char* data, size_t length) {
                    const std::string row(data, length);
                    assert(lazyReader.extractColumnValue(
                               row, table, 0, database, true) == "1");
                    assert(lazyReader.extractColumnValue(
                               row, table, 1, database, true) ==
                           updatedPayload);
                    ++lazyRows;
                }));
            assert(lazyRows == 1);
            assert(lazyReader.commitTransaction() == DBStatus::OK);

            // The reader's older snapshot still sees both superseded tuples
            // and must dereference their old external values after COMMIT.
            bool sawOriginalRow = false;
            bool sawDeletedRow = false;
            assert(reader.forEachRow(
                database, "documents",
                [&](uint32_t, uint16_t, const char* data, size_t length) {
                    const std::string row(data, length);
                    const std::string id = reader.extractColumnValue(
                        row, table, 0, database, true);
                    if (id == "1") {
                        sawOriginalRow = true;
                        assert(reader.extractColumnValue(
                                   row, table, 1, database, true) ==
                               livePayload);
                    } else if (id == "2") {
                        sawDeletedRow = true;
                        assert(reader.extractColumnValue(
                                   row, table, 1, database, true) ==
                               deletedPayload);
                    }
                }));
            assert(sawOriginalRow && sawDeletedRow);
            assert(writer.readToast(database, "documents", 2) ==
                   deletedPayload);
            assert(writer.readToast(database, "documents", 1) == livePayload);
            assert(writer.readToast(database, "documents", 3) ==
                   updatedPayload);

            // Both old values remain snapshot-reachable, so TOAST vacuuming
            // must defer reclamation while the reader is active.
            const size_t removedBeforeHeapVacuum =
                writer.vacuumToast(database, "documents");
            assert(removedBeforeHeapVacuum == 0);
            assert(writer.readToast(database, "documents", 1) == livePayload);
            assert(writer.readToast(database, "documents", 2) ==
                   deletedPayload);
            assert(reader.commitTransaction() == DBStatus::OK);
        }

        // VACUUM FULL drops the dead physical tuple.  Objects 1 and 2 are now
        // orphans eligible for chunk/index removal; object 3 stays live.
        assert(writer.vacuumFull(database, "documents") == 1);
        assert(writer.vacuumToast(database, "documents") == 2);
        assert(writer.readToast(database, "documents", 1).empty());
        assert(writer.readToast(database, "documents", 2).empty());
        assert(writer.readToast(database, "documents", 3) == updatedPayload);
        const auto rows = writer.query(
            database, "documents", {"=id 1"}, {"payload"});
        assert(rows.size() == 1);
        assert(rows.front().find(updatedPayload) != std::string::npos);
        std::cout << "[VACUUM TOAST] snapshot safety and orphan cleanup OK\n";
    }

    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");
    std::filesystem::remove_all(".txnid");
    std::cout << "[VACUUM TOAST] all passed\n";
    return 0;
}
