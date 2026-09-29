#include "commands/TableManage.h"
#include "interfaces/table_schema.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;
using dbms::DBStatus;

static dbms::TableSchema table(const std::string& name,
                               const std::string& tablespace = {}) {
    dbms::TableSchema schema;
    schema.tablename = name;
    schema.tablespace = tablespace;
    schema.append(dbms::makeIntColumn("id", false, 2));
    return schema;
}

int main() {
    cleanupAllTestData();
    const std::string database = testDbPath("drop_schema_space_guard");
    const fs::path location =
        fs::absolute(testDbPath("drop_schema_space_location"));
    fs::remove_all(location);
    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == DBStatus::OK);

    assert(engine.createSchema(database, "occupied") == DBStatus::OK);
    assert(engine.createTable(database, table("occupied__plain")) ==
           DBStatus::OK);
    assert(engine.insert(database, "occupied__plain", {{"id", "7"}}) ==
           DBStatus::OK);
    assert(engine.dropSchema(database, "occupied", false) ==
           DBStatus::INVALID_VALUE);
    assert(engine.schemaExists(database, "occupied"));
    assert(engine.tableExists(database, "occupied__plain"));
    assert(engine.dropTable(database, "occupied__plain") == DBStatus::OK);
    assert(engine.dropSchema(database, "occupied", false) == DBStatus::OK);

    assert(engine.createTablespace(database, "fast", location.string()) ==
           DBStatus::OK);
    assert(engine.createSchema(database, "space") == DBStatus::OK);
    assert(engine.createTable(database, table("space__items", "fast")) ==
           DBStatus::OK);
    assert(engine.insert(database, "space__items", {{"id", "9"}}) ==
           DBStatus::OK);
    assert(fs::is_regular_file(location / database / "space__items.dt"));
    assert(engine.dropSchema(database, "space", true) == DBStatus::OK);
    assert(!engine.schemaExists(database, "space"));
    assert(!engine.tableExists(database, "space__items"));
    assert(!fs::exists(location / database / "space__items.dt"));
    assert(engine.dropTablespace(database, "fast") == DBStatus::OK);

    assert(engine.dropDatabase(database) == DBStatus::OK);
    fs::remove_all(location);
    finalCleanupTestData();
    std::cout << "[DROP SCHEMA TABLESPACE GUARD] passed" << std::endl;
    return 0;
}
