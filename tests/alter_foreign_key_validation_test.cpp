#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

dbms::TableSchema keyedTable(const std::string& name) {
    dbms::TableSchema table;
    table.tablename = name;
    table.formatVersion = 2;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    return table;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "alter_foreign_key_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema parent = keyedTable("parent");
    dbms::Column code = dbms::makeIntColumn("code", false, 4, false);
    code.isUnique = true;
    parent.append(code);
    parent.append(dbms::makeIntColumn("non_unique", false, 4, false));
    assert(g_engine.createTable(database, parent) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "parent",
                           {{"id", "1"}, {"code", "10"},
                            {"non_unique", "50"}}) == dbms::DBStatus::OK);

    dbms::TableSchema child = keyedTable("child");
    child.append(dbms::makeIntColumn("parent_code", true, 4, false));
    assert(g_engine.createTable(database, child) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child",
                           {{"id", "1"}, {"parent_code", "99"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child",
                           {{"id", "2"}, {"parent_code", "NULL"}}) ==
           dbms::DBStatus::OK);

    // Existing orphan rows must prevent publication and leave the schema
    // untouched.
    assert(g_engine.alterTableAddFKConstraint(
               database, "child", "child_parent_code_fk", {"parent_code"},
               "parent", {"code"}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.getTableSchema(database, "child").fkLen == 0);

    assert(g_engine.update(database, "child", {{"parent_code", "10"}},
                           {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "child", "child_parent_code_fk", {"parent_code"},
               "parent", {"code"}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child",
                           {{"id", "3"}, {"parent_code", "10"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child",
                           {{"id", "4"}, {"parent_code", "77"}}) ==
           dbms::DBStatus::FOREIGN_KEY_VIOLATION);

    dbms::TableSchema invalidChild = keyedTable("invalid_child");
    invalidChild.append(dbms::makeIntColumn("parent_value", false, 4, false));
    invalidChild.append(dbms::makeStringColumn(
        "parent_text", false, 20, false));
    assert(g_engine.createTable(database, invalidChild) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "invalid_child", "missing_column_fk",
               {"parent_value"}, "parent", {"missing"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.alterTableAddFKConstraint(
               database, "invalid_child", "non_unique_column_fk",
               {"parent_value"}, "parent", {"non_unique"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.alterTableAddFKConstraint(
               database, "invalid_child", "mismatched_columns_fk",
               {"parent_value"}, "parent", {"id", "code"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.alterTableAddFKConstraint(
               database, "invalid_child", "incompatible_type_fk",
               {"parent_text"}, "parent", {"code"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.getTableSchema(database, "invalid_child").fkLen == 0);

    dbms::TableSchema defaultColumnsChild = keyedTable("default_columns_child");
    defaultColumnsChild.append(
        dbms::makeIntColumn("parent_id", false, 4, false));
    assert(g_engine.createTable(database, defaultColumnsChild) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "default_columns_child", "default_columns_fk",
               {"parent_id"}, "parent", {}) == dbms::DBStatus::OK);
    const dbms::TableSchema defaultColumnsSchema =
        g_engine.getTableSchema(database, "default_columns_child");
    assert(defaultColumnsSchema.fkLen == 1);
    assert(defaultColumnsSchema.fks[0].refCols ==
           std::vector<std::string>{"id"});
    assert(g_engine.insert(database, "default_columns_child",
                           {{"id", "1"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "default_columns_child",
                           {{"id", "2"}, {"parent_id", "99"}}) ==
           dbms::DBStatus::FOREIGN_KEY_VIOLATION);

    dbms::TableSchema node = keyedTable("node");
    node.append(dbms::makeIntColumn("parent_id", true, 4, false));
    assert(g_engine.createTable(database, node) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "node",
                           {{"id", "1"}, {"parent_id", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "node",
                           {{"id", "2"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "node", "node_parent_fk", {"parent_id"},
               "node", {"id"}) == dbms::DBStatus::OK);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[ALTER FOREIGN KEY] definition and existing rows validated OK"
              << std::endl;
    return 0;
}
