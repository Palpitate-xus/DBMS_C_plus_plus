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
    const std::string database = testDbPath("tablespace_target_guard");
    const fs::path location =
        fs::absolute(testDbPath("tablespace_target_guard_location"));
    fs::remove_all(location);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == DBStatus::OK);
    assert(engine.createTablespace(database, "fast", location.string()) ==
           DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("id", false, 2));
    assert(engine.createTable(database, table) == DBStatus::OK);
    assert(engine.insert(database, "items", {{"id", "7"}}) == DBStatus::OK);

    const fs::path destination = location / database / "items.dt";
    fs::create_symlink("missing_file", destination);
    assert(fs::is_symlink(destination));
    assert(!fs::exists(destination));

    // exists() follows the symlink and reports false. A plain rename would
    // replace that occupied directory entry, losing unrelated path state.
    assert(engine.alterTableTablespace(database, "items", "fast") ==
           DBStatus::INVALID_VALUE);
    assert(fs::is_symlink(destination));
    assert(fs::read_symlink(destination) == "missing_file");
    const auto storedTablespace =
        engine.getTableSchema(database, "items").tablespace;
    assert(storedTablespace.empty() || storedTablespace == "pg_default");
    assert(fs::is_regular_file(fs::path(database) / "items.dt"));
    size_t rows = 0;
    engine.forEachRow(database, "items",
                      [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);

    fs::remove(destination);
    assert(engine.dropTable(database, "items") == DBStatus::OK);
    assert(engine.dropTablespace(database, "fast") == DBStatus::OK);
    assert(engine.dropDatabase(database) == DBStatus::OK);
    fs::remove_all(location);
    finalCleanupTestData();
    std::cout << "[TABLESPACE DESTINATION ENTRY GUARD] passed" << std::endl;
    return 0;
}
