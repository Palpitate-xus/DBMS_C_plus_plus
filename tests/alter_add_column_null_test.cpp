#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("alter_add_column_null");
    std::filesystem::remove_all(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "null_rewrite";
    table.append(dbms::makeIntColumn("id", false, 2, true));
    table.append(dbms::makeIntColumn("optional_value", true, 2));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "null_rewrite", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.alterTableAddColumn(
               db, "null_rewrite",
               dbms::makeIntColumn("another_value", true, 2)) ==
           dbms::DBStatus::OK);

    const dbms::TableSchema rewritten =
        g_engine.getTableSchema(db, "null_rewrite");
    assert(rewritten.len == 3);
    bool found = false;
    assert(g_engine.forEachRow(
        db, "null_rewrite",
        [&](uint32_t pageId, uint16_t slotId, const char*, size_t) {
            found = true;
            const int64_t rid =
                dbms::StorageEngine::encodeRid(pageId, slotId);
            assert(g_engine.isColumnNullByRid(
                db, "null_rewrite", rid, 1));
            assert(g_engine.isColumnNullByRid(
                db, "null_rewrite", rid, 2));
        }));
    assert(found);

    std::filesystem::remove_all(db);
    std::cout << "[ALTER ADD COLUMN NULL] passed" << std::endl;
    return 0;
}
