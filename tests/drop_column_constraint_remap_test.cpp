#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "drop_column_constraint_remap";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);

    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("obsolete", false, 4));
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("tenant", false, 4));
    table.append(dbms::makeIntColumn("code", false, 4));
    table.pkColIndices = {1};
    table.uniqueConstraints = {{2, 3}};
    table.uniqueConstraintNames = {"items_tenant_code_key"};
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "items",
               {{"obsolete", "99"}, {"id", "1"},
                {"tenant", "7"}, {"code", "9"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.alterTableDropColumn(database, "items", "obsolete") ==
           dbms::DBStatus::OK);
    dbms::TableSchema changed = g_engine.getTableSchema(database, "items");
    assert(changed.len == 3);
    assert((changed.pkColIndices == std::vector<size_t>{0}));
    assert((changed.uniqueConstraints ==
            std::vector<std::vector<size_t>>{{1, 2}}));
    assert((changed.uniqueConstraintNames ==
            std::vector<std::string>{"items_tenant_code_key"}));

    // Both constraints must still protect the same logical columns after
    // their physical positions shift left.
    assert(g_engine.insert(
               database, "items",
               {{"id", "1"}, {"tenant", "8"}, {"code", "10"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               database, "items",
               {{"id", "2"}, {"tenant", "7"}, {"code", "9"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               database, "items",
               {{"id", "2"}, {"tenant", "8"}, {"code", "10"}}) ==
           dbms::DBStatus::OK);

    // DROP COLUMN has no CASCADE parameter, so removing a member of a table
    // constraint must fail without changing the schema.
    assert(g_engine.alterTableDropColumn(database, "items", "tenant") ==
           dbms::DBStatus::INVALID_VALUE);
    changed = g_engine.getTableSchema(database, "items");
    assert(changed.len == 3);
    assert(changed.cols[1].dataName == "tenant");

    // The remapped metadata must survive a schema reload.
    dbms::StorageEngine restarted;
    changed = restarted.getTableSchema(database, "items");
    assert((changed.pkColIndices == std::vector<size_t>{0}));
    assert((changed.uniqueConstraints ==
            std::vector<std::vector<size_t>>{{1, 2}}));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[DROP COLUMN CONSTRAINT REMAP] all passed" << std::endl;
    return 0;
}
