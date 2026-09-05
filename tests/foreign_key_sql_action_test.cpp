#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

struct ChildReference {
    bool found = false;
    bool isNull = false;
    std::string value;
};

ChildReference childReference(const std::string& database,
                              const std::string& tableName,
                              const std::string& id) {
    const dbms::TableSchema table =
        g_engine.getTableSchema(database, tableName);
    assert(table.len == 2);
    ChildReference result;
    assert(g_engine.forEachRow(
        database, tableName,
        [&](uint32_t pageId, uint16_t slotId, const char* data,
            size_t length) {
            const std::string row(data, length);
            if (g_engine.extractColumnValue(
                    row, table, 0, database, true) != id) {
                return;
            }
            result.found = true;
            const int64_t rid =
                dbms::StorageEngine::encodeRid(pageId, slotId);
            result.isNull = g_engine.isColumnNullByRid(
                database, tableName, rid, 1);
            result.value = g_engine.extractColumnValue(
                row, table, 1, database, true);
        }));
    return result;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "foreign_key_sql_action";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE parent (id INT PRIMARY KEY)",
                           session));
    assert(!ddl.executeSql(
        "CREATE TABLE table_child ("
        "id INT PRIMARY KEY, parent_id INT, "
        "CONSTRAINT table_child_parent_fk FOREIGN KEY (parent_id) "
        "REFERENCES parent (id) ON DELETE SET NULL ON UPDATE CASCADE)",
        session));
    const dbms::TableSchema child =
        g_engine.getTableSchema(database, "table_child");
    assert(child.fkLen == 1);
    assert(child.fks[0].onDelete == "setnull");
    assert(child.fks[0].onUpdate == "cascade");

    assert(g_engine.insert(database, "parent", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "table_child",
                           {{"id", "10"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "parent", {{"id", "2"}}, {"=id 1"}) ==
           dbms::DBStatus::OK);
    assert(childReference(database, "table_child", "10").value == "2");
    assert(g_engine.remove(database, "parent", {"=id 2"}) ==
           dbms::DBStatus::OK);
    const ChildReference nulled =
        childReference(database, "table_child", "10");
    assert(nulled.found && nulled.isNull);

    assert(!ddl.executeSql(
        "CREATE TABLE inline_child ("
        "id INT PRIMARY KEY, "
        "parent_id INT REFERENCES parent (id) "
        "ON UPDATE CASCADE ON DELETE SET NULL)",
        session));
    const dbms::TableSchema inlineChild =
        g_engine.getTableSchema(database, "inline_child");
    assert(inlineChild.fkLen == 1);
    assert(inlineChild.fks[0].onDelete == "setnull");
    assert(inlineChild.fks[0].onUpdate == "cascade");
    assert(g_engine.insert(database, "parent", {{"id", "3"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "inline_child",
                           {{"id", "30"}, {"parent_id", "3"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "parent", {{"id", "4"}}, {"=id 3"}) ==
           dbms::DBStatus::OK);
    assert(childReference(database, "inline_child", "30").value == "4");
    assert(g_engine.remove(database, "parent", {"=id 4"}) ==
           dbms::DBStatus::OK);
    assert(childReference(database, "inline_child", "30").isNull);

    assert(!ddl.executeSql(
        "CREATE TABLE alter_child (id INT PRIMARY KEY, parent_id INT)",
        session));
    assert(g_engine.insert(database, "parent", {{"id", "5"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "alter_child",
                           {{"id", "50"}, {"parent_id", "5"}}) ==
           dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "ALTER TABLE alter_child ADD CONSTRAINT alter_child_parent_fk "
        "FOREIGN KEY (parent_id) REFERENCES parent (id) "
        "ON DELETE SET NULL ON UPDATE CASCADE",
        session));
    const dbms::TableSchema alteredChild =
        g_engine.getTableSchema(database, "alter_child");
    assert(alteredChild.fkLen == 1);
    assert(alteredChild.fks[0].onDelete == "setnull");
    assert(alteredChild.fks[0].onUpdate == "cascade");
    assert(g_engine.update(database, "parent", {{"id", "6"}}, {"=id 5"}) ==
           dbms::DBStatus::OK);
    assert(childReference(database, "alter_child", "50").value == "6");
    assert(g_engine.remove(database, "parent", {"=id 6"}) ==
           dbms::DBStatus::OK);
    assert(childReference(database, "alter_child", "50").isNull);

    assert(!ddl.executeSql(
        "CREATE TABLE no_action_child ("
        "id INT PRIMARY KEY, parent_id INT, "
        "FOREIGN KEY (parent_id) REFERENCES parent (id) "
        "ON DELETE NO ACTION ON UPDATE RESTRICT)",
        session));
    const dbms::TableSchema noActionChild =
        g_engine.getTableSchema(database, "no_action_child");
    assert(noActionChild.fkLen == 1);
    assert(noActionChild.fks[0].onDelete == "restrict");
    assert(noActionChild.fks[0].onUpdate == "restrict");

    assert(!ddl.executeSql(
        "CREATE TABLE api_child (id INT PRIMARY KEY, parent_id INT)",
        session));
    assert(g_engine.alterTableAddFKConstraint(
               database, "api_child", "api_child_parent_fk", {"parent_id"},
               "parent", {"id"}, "SET NULL", "NO ACTION") ==
           dbms::DBStatus::OK);
    const dbms::TableSchema apiChild =
        g_engine.getTableSchema(database, "api_child");
    assert(apiChild.fkLen == 1);
    assert(apiChild.fks[0].onDelete == "setnull");
    assert(apiChild.fks[0].onUpdate == "restrict");

    assert(ddl.executeSql(
        "CREATE TABLE unsupported_default_child ("
        "id INT PRIMARY KEY, parent_id INT, "
        "FOREIGN KEY (parent_id) REFERENCES parent (id) "
        "ON DELETE SET DEFAULT)",
        session));
    assert(!g_engine.tableExists(database, "unsupported_default_child"));
    assert(ddl.executeSql(
        "CREATE TABLE duplicate_action_child ("
        "id INT PRIMARY KEY, parent_id INT, "
        "FOREIGN KEY (parent_id) REFERENCES parent (id) "
        "ON DELETE CASCADE ON DELETE SET NULL)",
        session));
    assert(!g_engine.tableExists(database, "duplicate_action_child"));

    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[FOREIGN KEY SQL ACTION] multi-word actions preserved OK"
              << std::endl;
    return 0;
}
