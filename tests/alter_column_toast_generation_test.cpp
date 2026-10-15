#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <map>
#include <optional>
#include <string>

extern dbms::StorageEngine g_engine;

using Rows = std::map<std::string, dbms::StorageEngine::SqlRow>;

static Rows snapshot(const std::string& database) {
    const auto schema = g_engine.getTableSchema(database, "items");
    Rows result;
    assert(g_engine.forEachRow(database, "items",
        [&](uint32_t page, uint16_t slot, const char* data, size_t length) {
            dbms::StorageEngine::SqlRow row;
            const auto rid = dbms::StorageEngine::encodeRid(page, slot);
            const std::string bytes(data, length);
            for (size_t i = 0; i < schema.len; ++i) {
                if (g_engine.isColumnNullByRid(database, "items", rid, i))
                    row[schema.cols[i].dataName] = std::nullopt;
                else row[schema.cols[i].dataName] =
                    g_engine.extractColumnValue(bytes, schema, i, database);
            }
            assert(row.at("id"));
            assert(result.emplace(*row.at("id"), row).second);
        }));
    return result;
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("alter_column_toast_generation");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeIntColumn("remove_me", true, 4));
    table.append(makeVarCharColumn("body", true, 12000));
    table.append(makeVarCharColumn("note", true, 12000));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    const std::string a(10000, 'a'), b(11000, 'b'), c(10500, 'c');
    Rows expected = {
        {"1", {{"id", "1"}, {"remove_me", "5"}, {"body", a}, {"note", "__TOAST__1"}}},
        {"2", {{"id", "2"}, {"remove_me", std::nullopt}, {"body", std::nullopt}, {"note", "NULL"}}},
        {"3", {{"id", "3"}, {"remove_me", "7"}, {"body", ""}, {"note", std::nullopt}}},
        {"4", {{"id", "4"}, {"remove_me", "-2"}, {"body", b}, {"note", c}}}};
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    for (const auto& entry : expected)
        assert(g_engine.insertRow(database, "items", entry.second) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(snapshot(database) == expected);
    size_t rewrites = 0;

    assert(g_engine.alterTableAddColumn(database, "items", makeIntColumn("extra", true, 4)) == DBStatus::OK);
    for (auto& entry : expected) entry.second["extra"] = std::nullopt;
    assert(snapshot(database) == expected); ++rewrites;
    // Both ADD and DROP recreate an owned TOAST generation while retaining
    // decoded external values. A blanket missing-counter guard must not break them.
    for (const auto* column : {"remove_me", "body", "note"}) {
        assert(g_engine.alterTableDropColumn(database, "items", column) == DBStatus::OK);
        for (auto& entry : expected) entry.second.erase(column);
        assert(snapshot(database) == expected); ++rewrites;
    }
    assert(!std::filesystem::exists(std::filesystem::path(database) / "items.toastmeta"));
    // Adding the first variable column to an existing fixed-only heap is also
    // a legitimate new generation, not repair of a damaged existing relation.
    assert(g_engine.alterTableAddColumn(database, "items", makeVarCharColumn("note", true, 12000)) == DBStatus::OK);
    for (auto& entry : expected) entry.second["note"] = std::nullopt;
    assert(snapshot(database) == expected); ++rewrites;
    expected["5"] = {{"id", "5"}, {"extra", std::nullopt}, {"note", c}};
    assert(g_engine.insertRow(database, "items", expected.at("5")) == DBStatus::OK);
    assert(snapshot(database) == expected);
    std::cout << "ALTER_COLUMN_TOAST_GENERATION_COMPLETE rewrites=" << rewrites
              << " typed_rows=" << expected.size() << '\n';
}
