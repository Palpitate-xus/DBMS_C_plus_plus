#include "commands/StoredRowNull.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("stored_row_null_schema");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema schema;
    schema.tablename = "rows";
    schema.formatVersion = DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id", false, 2, true));
    schema.append(makeIntColumn("n", true, 2));
    schema.append(makeTextColumn("t", true));
    for (int i = 0; i < 12; ++i) schema.append(makeIntColumn("c" + std::to_string(i), true, 2));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    for (int id = 1; id <= 3; ++id) {
        std::map<std::string, std::optional<std::string>> cells;
        cells["id"] = std::to_string(id);
        cells["n"] = id == 1 ? std::nullopt : std::optional<std::string>(id == 2 ? "0" : "-2147483648");
        cells["t"] = id == 1 ? std::nullopt : std::optional<std::string>(id == 2 ? "" : "NULL");
        for (int i = 0; i < 12; ++i)
            cells["c" + std::to_string(i)] = (i + id) % 2
                ? std::nullopt : std::optional<std::string>(std::to_string(i));
        assert(g_engine.insertRow(database, schema.tablename, cells) == DBStatus::OK);
    }
    schema = g_engine.getTableSchema(database, schema.tablename);
    size_t rows = 0;
    const bool scanned = g_engine.forEachRow(database, schema.tablename,
        [&](uint32_t page, uint16_t slot, const char* bytes, size_t length) {
            const std::string row(bytes, length);
            StorageEngine::unbindNullRow();
            const int id = std::stoi(StorageEngine::extractColumnValueStatic(row, schema, 0));
            const auto rid = StorageEngine::encodeRid(page, slot);
            StorageEngine::bindNullRow(&g_engine, database, schema.tablename, rid, schema.len);
            for (size_t column = 0; column < schema.len; ++column) {
                const bool expected = column == 0 ? false : column < 3 ? id == 1
                    : (static_cast<int>(column - 3) + id) % 2 != 0;
                assert(detail::storedRowColumnIsNull(g_engine, database, schema.tablename,
                                                   schema, rid, column) == expected);
                assert(g_engine.isColumnNullByRid(database, schema.tablename, rid, column) == expected);
                bool null = false;
                const auto value = g_engine.extractColumnValue(row, schema, column, database, true, &null);
                assert(null == expected);
                assert(StorageEngine::extractColumnValueStatic(row, schema, column) == value);
                if (expected) assert(value.empty());
                if (column == 1 && id == 2) assert(value == "0");
                if (column == 1 && id == 3) assert(value == "-2147483648");
                if (column == 2 && id == 2) assert(value.empty() && !null);
                if (column == 2 && id == 3) assert(value == "NULL" && !null);
            }
            assert(!detail::storedRowColumnIsNull(g_engine, database, schema.tablename,
                                                 schema, rid, schema.len));
            ++rows;
            StorageEngine::unbindNullRow();
        });
    assert(scanned && rows == 3);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[STORED ROW NULL SCHEMA] passed\n";
}
