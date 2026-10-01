#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    const std::string database = testDbPath("long_default_expression");
    cleanupTestDb("long_default_expression");
    dbms::StorageEngine engine;
    assert(engine.createDatabase(database) == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "defaults";
    auto value = dbms::makeVarCharColumn("value", false, 512);
    const std::string original = "'" + std::string(180, 'a') + "'";
    value.defaultValue = original;
    table.append(value);
    assert(engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(engine.getTableSchema(database, "defaults").cols[0].defaultValue == original);
    assert(engine.insert(database, "defaults", {}) == dbms::DBStatus::OK);
    assert(engine.query(database, "defaults", {}, {"value"}) == std::vector<std::string>{std::string(180, 'a') + " "});
    const std::string changed = "'" + std::string(260, 'b') + "'";
    assert(engine.alterTableSetDefault(database, "defaults", "value", changed) == dbms::DBStatus::OK);
    assert(engine.alterTableRenameColumn(database, "defaults", "value", "renamed") == dbms::DBStatus::OK);
    assert(engine.getTableSchema(database, "defaults").cols[0].defaultValue == changed);
    assert(engine.alterTableSetDefault(database, "defaults", "renamed", std::string(1024 * 1024 + 1, 'x')) != dbms::DBStatus::OK);
    assert(engine.getTableSchema(database, "defaults").cols[0].defaultValue == changed);
    {
        dbms::StorageEngine reloaded;
        assert(reloaded.getTableSchema(database, "defaults").cols[0].defaultValue == changed);
    }
    assert(engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("long_default_expression");
    std::cout << "[LONG DEFAULT EXPRESSION] passed" << std::endl;
}
