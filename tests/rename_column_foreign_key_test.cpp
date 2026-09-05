#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

struct ForeignKeyValue {
    bool found = false;
    bool isNull = false;
    std::string value;
};

ForeignKeyValue findForeignKeyValue(
    const std::string& database, const std::string& tableName,
    const std::string& idColumn, const std::string& id,
    const std::string& foreignKeyColumn) {
    const dbms::TableSchema table =
        g_engine.getTableSchema(database, tableName);
    size_t idIndex = table.len;
    size_t foreignKeyIndex = table.len;
    for (size_t columnIndex = 0; columnIndex < table.len; ++columnIndex) {
        if (table.cols[columnIndex].dataName == idColumn) {
            idIndex = columnIndex;
        }
        if (table.cols[columnIndex].dataName == foreignKeyColumn) {
            foreignKeyIndex = columnIndex;
        }
    }
    assert(idIndex < table.len && foreignKeyIndex < table.len);

    ForeignKeyValue result;
    assert(g_engine.forEachRow(
        database, tableName,
        [&](uint32_t pageId, uint16_t slotId, const char* data,
            size_t length) {
            const std::string row(data, length);
            if (g_engine.extractColumnValue(
                    row, table, idIndex, database, true) != id) {
                return;
            }
            result.found = true;
            const int64_t rid =
                dbms::StorageEngine::encodeRid(pageId, slotId);
            result.isNull = g_engine.isColumnNullByRid(
                database, tableName, rid, foreignKeyIndex);
            result.value = g_engine.extractColumnValue(
                row, table, foreignKeyIndex, database, true);
        }));
    return result;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "rename_column_foreign_key";
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE parent (id INT PRIMARY KEY)",
                           session));
    assert(!ddl.executeSql(
        "CREATE TABLE local_child ("
        "id INT PRIMARY KEY, parent_id INT, "
        "CONSTRAINT local_parent_fk FOREIGN KEY (parent_id) "
        "REFERENCES parent (id) ON UPDATE CASCADE ON DELETE SET NULL)",
        session));
    assert(!ddl.executeSql(
        "CREATE TABLE referenced_child ("
        "id INT PRIMARY KEY, parent_id INT, "
        "CONSTRAINT referenced_parent_fk FOREIGN KEY (parent_id) "
        "REFERENCES parent (id) ON UPDATE CASCADE ON DELETE SET NULL)",
        session));

    assert(g_engine.insert(database, "parent", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "local_child",
                           {{"id", "10"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "referenced_child",
                           {{"id", "20"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.alterTableRenameColumn(
               database, "local_child", "parent_id", "parent_key") ==
           dbms::DBStatus::OK);
    dbms::TableSchema local =
        g_engine.getTableSchema(database, "local_child");
    assert(local.fkLen == 1);
    assert(local.fks[0].colNames ==
           std::vector<std::string>{"parent_key"});
    assert(g_engine.insert(database, "local_child",
                           {{"id", "11"}, {"parent_key", "999"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "local_child",
                           {{"id", "12"}, {"parent_key", "1"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.alterTableRenameColumn(
               database, "parent", "id", "parent_key") ==
           dbms::DBStatus::OK);
    local = g_engine.getTableSchema(database, "local_child");
    const dbms::TableSchema referenced =
        g_engine.getTableSchema(database, "referenced_child");
    assert(local.fks[0].refCols ==
           std::vector<std::string>{"parent_key"});
    assert(referenced.fks[0].refCols ==
           std::vector<std::string>{"parent_key"});
    {
        dbms::StorageEngine restarted;
        const dbms::TableSchema reloadedLocal =
            restarted.getTableSchema(database, "local_child");
        const dbms::TableSchema reloadedReferenced =
            restarted.getTableSchema(database, "referenced_child");
        assert(reloadedLocal.fks[0].colNames ==
               std::vector<std::string>{"parent_key"});
        assert(reloadedLocal.fks[0].refCols ==
               std::vector<std::string>{"parent_key"});
        assert(reloadedReferenced.fks[0].refCols ==
               std::vector<std::string>{"parent_key"});
    }
    assert(g_engine.insert(database, "referenced_child",
                           {{"id", "21"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.update(database, "parent", {{"parent_key", "2"}},
                           {"=parent_key 1"}) == dbms::DBStatus::OK);
    const ForeignKeyValue cascadedLocal = findForeignKeyValue(
        database, "local_child", "id", "10", "parent_key");
    const ForeignKeyValue cascadedReferenced = findForeignKeyValue(
        database, "referenced_child", "id", "20", "parent_id");
    assert(cascadedLocal.found && cascadedLocal.value == "2");
    assert(cascadedReferenced.found && cascadedReferenced.value == "2");

    assert(g_engine.remove(database, "parent", {"=parent_key 2"}) ==
           dbms::DBStatus::OK);
    const ForeignKeyValue nulledLocal = findForeignKeyValue(
        database, "local_child", "id", "10", "parent_key");
    const ForeignKeyValue nulledReferenced = findForeignKeyValue(
        database, "referenced_child", "id", "20", "parent_id");
    assert(nulledLocal.found && nulledLocal.isNull);
    assert(nulledReferenced.found && nulledReferenced.isNull);

    assert(!ddl.executeSql(
        "CREATE TABLE node ("
        "id INT PRIMARY KEY, parent_id INT, "
        "CONSTRAINT node_parent_fk FOREIGN KEY (parent_id) "
        "REFERENCES node (id) ON UPDATE CASCADE ON DELETE SET NULL)",
        session));
    assert(g_engine.insert(database, "node", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "node",
                           {{"id", "2"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableRenameColumn(
               database, "node", "id", "node_id") ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableRenameColumn(
               database, "node", "parent_id", "parent_node_id") ==
           dbms::DBStatus::OK);
    const dbms::TableSchema node =
        g_engine.getTableSchema(database, "node");
    assert(node.fkLen == 1);
    assert(node.fks[0].colNames ==
           std::vector<std::string>{"parent_node_id"});
    assert(node.fks[0].refCols ==
           std::vector<std::string>{"node_id"});
    assert(g_engine.insert(database, "node",
                           {{"node_id", "3"},
                            {"parent_node_id", "999"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.update(database, "node", {{"node_id", "4"}},
                           {"=node_id 1"}) == dbms::DBStatus::OK);
    const ForeignKeyValue cascadedSelf = findForeignKeyValue(
        database, "node", "node_id", "2", "parent_node_id");
    assert(cascadedSelf.found && cascadedSelf.value == "4");
    assert(g_engine.remove(database, "node", {"=node_id 4"}) ==
           dbms::DBStatus::OK);
    const ForeignKeyValue nulledSelf = findForeignKeyValue(
        database, "node", "node_id", "2", "parent_node_id");
    assert(nulledSelf.found && nulledSelf.isNull);

    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[RENAME COLUMN FK] bindings and actions preserved OK"
              << std::endl;
    return 0;
}
