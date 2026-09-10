#include "commands/TableManage.h"
#include "types/bytea.h"
#include "test_utils.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <limits>
#include <string>

namespace {

std::string makePayload(size_t size, uint32_t seed) {
    std::string bytes(size, '\0');
    uint32_t state = seed;
    for (char& byte : bytes) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        byte = static_cast<char>(state & 0xffU);
    }
    return dbms::ByteaValue::fromBytes(std::move(bytes)).toString();
}

std::string trimResult(std::string value) {
    while (!value.empty() && (value.back() == ' ' || value.back() == '\n'))
        value.pop_back();
    return value;
}

}  // namespace

int main() {
    const std::string database = testDbPath("bytea_large_toast");
    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");

    const std::string inserted = makePayload(70000, 0x13579bdfU);
    const std::string updated = makePayload(90000, 0x2468ace0U);
    assert(inserted.size() > std::numeric_limits<uint16_t>::max());
    assert(updated.size() > inserted.size());

    {
        dbms::StorageEngine engine;
        engine.setBackgroundIntervals(60000, 60000);
        assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

        dbms::TableSchema table;
        table.tablename = "payloads";
        table.append(dbms::makeIntColumn("id", false, 4, true));
        table.append(dbms::makeBlobColumn("payload", false));
        assert(table.cols[1].dsize == static_cast<size_t>(
            std::numeric_limits<int32_t>::max()));
        assert(engine.createTable(database, table) == dbms::DBStatus::OK);
        assert(engine.writeToast(database, "payloads", 999, inserted));
        std::string directRead;
        assert(!engine.readToast(database, "payloads", 999, 65535,
                                 directRead));
        assert(directRead.empty());
        assert(engine.readToast(
            database, "payloads", 999,
            static_cast<size_t>(std::numeric_limits<int32_t>::max()),
            directRead));
        assert(directRead == inserted);
        engine.deleteToast(database, "payloads", 999);
        assert(engine.insert(database, "payloads",
                             {{"id", "1"}, {"payload", inserted}}) ==
               dbms::DBStatus::OK);
        const auto rows = engine.query(
            database, "payloads", {"=id 1"}, {"payload"});
        assert(rows.size() == 1);
        assert(trimResult(rows.front()) == inserted);

        assert(engine.update(database, "payloads", {{"payload", updated}},
                             {"=id 1"}) == dbms::DBStatus::OK);
    }

    // Reopen both schema and TOAST relations to prove the >64 KiB logical
    // value is durable and not only present in an in-memory cache.
    {
        dbms::StorageEngine reopened;
        reopened.setBackgroundIntervals(60000, 60000);
        const dbms::TableSchema schema =
            reopened.getTableSchema(database, "payloads");
        assert(schema.len == 2);
        assert(schema.cols[1].dsize == static_cast<size_t>(
            std::numeric_limits<int32_t>::max()));
        const auto rows = reopened.query(
            database, "payloads", {"=id 1"}, {"payload"});
        assert(rows.size() == 1);
        assert(trimResult(rows.front()) == updated);
    }

    std::filesystem::remove_all(database);
    std::filesystem::remove_all(database + ".txn_backup");
    std::cout << "[BYTEA TOAST] >64 KiB insert/update/reopen OK" << std::endl;
    return 0;
}
