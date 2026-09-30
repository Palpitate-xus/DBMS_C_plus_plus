#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("typed_distinct_on");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeDecimalColumn("v", true, 50, 5));
    table.append(dbms::makeIntColumn("id", false, 2));
    table.append(dbms::makeTextColumn("t", true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    const std::vector<dbms::StorageEngine::SqlRow> input = {
        {{"v", "0.50"}, {"id", "1"}, {"t", "same"}},
        {{"v", "0.500"}, {"id", "2"}, {"t", "same"}},
        {{"v", "2.00"}, {"id", "3"}, {"t", ""}},
        {{"v", std::nullopt}, {"id", "4"}, {"t", "NULL"}},
        {{"v", std::nullopt}, {"id", "5"}, {"t", std::nullopt}}
    };
    for (const auto& row : input)
        assert(g_engine.insertRow(db, table.tablename, row) == dbms::DBStatus::OK);
    const auto query = [&](const std::string& name, const std::vector<std::string>& keys,
                           const std::vector<std::vector<std::string>>& expected,
                           const std::vector<std::vector<bool>>& expectedNulls) {
        std::vector<std::vector<std::string>> rows;
        std::vector<std::vector<bool>> nulls;
        const std::vector<dbms::StorageEngine::OrderBySpec> order = {{"v", true}, {"id", true}};
        auto display = g_engine.query(db, name, {}, {}, order,
            false, false, false, 0, keys, &rows, &nulls);
        assert(display.size() == expected.size());
        assert(rows == expected);
        assert(nulls == expectedNulls);
    };
    query("items", {"v"}, {{"0.50", "1", "same"}, {"2.00", "3", ""}, {"", "4", "NULL"}},
        {{false, false, false}, {false, false, false}, {true, false, false}});
    query("items", {"v", "t"},
        {{"0.50", "1", "same"}, {"2.00", "3", ""}, {"", "4", "NULL"}, {"", "5", ""}},
        {{false, false, false}, {false, false, false}, {true, false, false}, {true, false, true}});
    table.tablename = "exact";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    int id = 0;
    for (const auto& value : {"123456789012345678901234567890.1",
                             "123456789012345678901234567890.10",
                             "123456789012345678901234567890.2"}) {
        assert(g_engine.insertRow(db, table.tablename,
            {{"v", value}, {"id", std::to_string(++id)}, {"t", ""}}) == dbms::DBStatus::OK);
    }
    query("exact", {"v"}, {{"123456789012345678901234567890.1", "1", ""},
                            {"123456789012345678901234567890.2", "3", ""}},
        {{false, false, false}, {false, false, false}});
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[TYPED DISTINCT ON] passed\n";
}
