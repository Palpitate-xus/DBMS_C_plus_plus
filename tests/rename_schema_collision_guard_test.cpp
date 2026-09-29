#include "commands/TableManage.h"
#include "interfaces/table_schema.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;
using dbms::DBStatus;

static dbms::TableSchema table(const std::string& name) {
    dbms::TableSchema schema;
    schema.tablename = name;
    schema.append(dbms::makeIntColumn("id", false, 2));
    return schema;
}

static size_t countRows(dbms::StorageEngine& engine,
                        const std::string& database,
                        const std::string& relation) {
    size_t rows = 0;
    engine.forEachRow(database, relation,
                      [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    return rows;
}

int main() {
    cleanupAllTestData();
    const std::string database = testDbPath("rename_schema_guard");
    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == DBStatus::OK);
    assert(engine.createSchema(database, "old") == DBStatus::OK);
    assert(engine.createTable(database, table("old__items")) == DBStatus::OK);
    assert(engine.createTable(database, table("new__items")) == DBStatus::OK);
    assert(engine.insert(database, "old__items", {{"id", "1"}}) == DBStatus::OK);
    assert(engine.insert(database, "new__items", {{"id", "2"}}) == DBStatus::OK);

    // The old file-only rename overwrote new__items despite no "new" schema
    // marker. It also omitted catalog, tablespace and dependency updates.
    assert(engine.renameSchema(database, "old", "new") ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(fs::exists(fs::path(database) / ".schema_old"));
    assert(!fs::exists(fs::path(database) / ".schema_new"));
    assert(engine.tableExists(database, "old__items"));
    assert(engine.tableExists(database, "new__items"));
    assert(countRows(engine, database, "old__items") == 1);
    assert(countRows(engine, database, "new__items") == 1);

    assert(engine.dropDatabase(database) == DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[RENAME SCHEMA COLLISION GUARD] passed" << std::endl;
    return 0;
}
