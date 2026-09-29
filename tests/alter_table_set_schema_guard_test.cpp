#include "commands/TableManage.h"
#include "interfaces/table_schema.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;
using dbms::DBStatus;

int main() {
    cleanupAllTestData();
    const std::string source = testDbPath("set_schema_source");
    const std::string target = testDbPath("set_schema_target");
    dbms::StorageEngine engine;
    assert(engine.createDatabase(source, "utf8") == DBStatus::OK);
    assert(engine.createDatabase(target, "utf8") == DBStatus::OK);
    assert(engine.createSchema(source, target) == DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("id", false, 2));
    assert(engine.createTable(source, table) == DBStatus::OK);
    assert(engine.insert(source, "items", {{"id", "7"}}) == DBStatus::OK);

    // A schema is a namespace within source, never a second database. Until
    // the storage and catalog migration is implemented together, reject it
    // rather than move relation forks into a same-named database.
    assert(engine.alterTableSetSchema(source, "items", target) ==
           DBStatus::FEATURE_NOT_SUPPORTED);
    assert(engine.tableExists(source, "items"));
    assert(!engine.tableExists(target, "items"));
    assert(fs::exists(fs::path(source) / "items.stc"));
    assert(!fs::exists(fs::path(target) / "items.stc"));
    size_t rows = 0;
    engine.forEachRow(source, "items",
                      [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);

    assert(engine.dropDatabase(source) == DBStatus::OK);
    assert(engine.dropDatabase(target) == DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[ALTER TABLE SET SCHEMA GUARD] passed" << std::endl;
    return 0;
}
