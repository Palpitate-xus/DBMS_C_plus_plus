#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "foreign_key_referenced_column";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema parent;
    parent.tablename = "parent";
    parent.formatVersion = 2;
    parent.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column code = dbms::makeIntColumn("code", false, 4, false);
    code.isUnique = true;
    parent.append(code);
    assert(g_engine.createTable(database, parent) == dbms::DBStatus::OK);

    dbms::TableSchema child;
    child.tablename = "child";
    child.formatVersion = 2;
    child.append(dbms::makeIntColumn("id", false, 4, true));
    child.append(dbms::makeIntColumn("parent_code", false, 4, false));
    assert(g_engine.createTable(database, child) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "child", "child_parent_code_fk", {"parent_code"},
               "parent", {"code"}) == dbms::DBStatus::OK);

    // The PK value and referenced UNIQUE value deliberately differ. Looking
    // up parent_code in parent.id reverses both expected outcomes below.
    assert(g_engine.insert(database, "parent", {{"id", "7"}, {"code", "9"}}) ==
           dbms::DBStatus::OK);
    const dbms::DBStatus validReference = g_engine.insert(
        database, "child", {{"id", "1"}, {"parent_code", "9"}});
    const dbms::DBStatus missingReference = g_engine.insert(
        database, "child", {{"id", "2"}, {"parent_code", "7"}});
    assert(validReference == dbms::DBStatus::OK);
    assert(missingReference == dbms::DBStatus::INVALID_VALUE);

    dbms::TableSchema tablePrimaryParent;
    tablePrimaryParent.tablename = "table_primary_parent";
    tablePrimaryParent.formatVersion = 2;
    tablePrimaryParent.append(dbms::makeIntColumn("id", false, 4, true));
    tablePrimaryParent.pkColIndices = {0};
    assert(g_engine.createTable(database, tablePrimaryParent) ==
           dbms::DBStatus::OK);

    dbms::TableSchema tablePrimaryChild;
    tablePrimaryChild.tablename = "table_primary_child";
    tablePrimaryChild.formatVersion = 2;
    tablePrimaryChild.append(dbms::makeIntColumn("id", false, 4, true));
    tablePrimaryChild.append(dbms::makeIntColumn("parent_id", false, 4, false));
    assert(g_engine.createTable(database, tablePrimaryChild) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "table_primary_child", "table_primary_child_fk",
               {"parent_id"}, "table_primary_parent", {"id"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "table_primary_parent", {{"id", "42"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "table_primary_child",
                           {{"id", "10"}, {"parent_id", "42"}}) ==
           dbms::DBStatus::OK);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FOREIGN KEY REFERENCED COLUMN] non-PK UNIQUE lookup OK"
              << std::endl;
    return 0;
}
