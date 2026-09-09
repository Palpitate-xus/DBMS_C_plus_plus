#include "Config.h"
#include "TableManage.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

dbms::Config g_config;

using namespace dbms;

int main() {
    const std::string dbname = "boolean_update_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    StorageEngine engine;
    assert(engine.createDatabase(dbname) == DBStatus::OK);
    TableSchema table;
    table.tablename = "flags";
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeBooleanColumn("enabled", false, false));
    assert(engine.createTable(dbname, table) == DBStatus::OK);
    assert(engine.insert(dbname, "flags", {{"id", "1"}, {"enabled", "false"}})
           == DBStatus::OK);

    // PostgreSQL accepts unambiguous, case-insensitive prefixes of its six
    // boolean words and stores one canonical boolean value.
    const std::vector<std::pair<std::string, std::string>> spellings = {
        {"t", "t"}, {"TR", "t"}, {"y", "t"}, {"ye", "t"},
        {"on", "t"}, {"1", "t"}, {"f", "f"}, {"FA", "f"},
        {"n", "f"}, {"of", "f"}, {"0", "f"},
    };
    for (const auto& [input, expected] : spellings) {
        assert(engine.update(dbname, "flags", {{"enabled", input}}, {"=id 1"})
               == DBStatus::OK);
        std::vector<std::string> stored;
        const TableSchema storedSchema =
            engine.getTableSchema(dbname, "flags");
        assert(engine.forEachRow(
            dbname, "flags",
            [&](uint32_t, uint16_t, const char* data, size_t length) {
                stored.push_back(engine.extractColumnValue(
                    std::string(data, length), storedSchema, 1, dbname));
            }));
        assert(stored == std::vector<std::string>{expected});
    }
    assert(engine.update(dbname, "flags", {{"enabled", "o"}}, {"=id 1"})
           == DBStatus::INVALID_VALUE);

    assert(engine.update(dbname, "flags", {{"enabled", "true"}}, {"=id 1"})
           == DBStatus::OK);
    assert(engine.query(dbname, "flags", {"=enabled true"}, {"id"}).size() == 1);
    assert(engine.update(dbname, "flags", {{"enabled", "0"}}, {"=id 1"})
           == DBStatus::OK);
    assert(engine.query(dbname, "flags", {"=enabled false"}, {"id"}).size() == 1);
    assert(engine.update(dbname, "flags", {{"enabled", "not-a-bool"}},
                         {"=id 1"}) == DBStatus::INVALID_VALUE);

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");
    std::cout << "[BOOLEAN UPDATE] all passed\n";
    return 0;
}
