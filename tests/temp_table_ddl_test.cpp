#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("temp_table_ddl");
    cleanupTestDb("temp_table_ddl");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    session.pid = 424242;
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TEMP TABLE old_name (id INT PRIMARY KEY) "
        "ON COMMIT PRESERVE ROWS",
        session));
    const std::string oldPhysical = tempTablePrefix(session, "old_name");
    const std::string newPhysical = tempTablePrefix(session, "new_name");
    assert(session.tempTables.count("old_name") == 1);
    assert(g_engine.tableExists(database, oldPhysical));
    assert(g_engine.insert(database, oldPhysical, {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(!ddl.executeSql(
        "ALTER TABLE old_name RENAME TO new_name", session));
    assert(session.tempTables.count("old_name") == 0);
    assert(session.tempTables.count("new_name") == 1);
    assert(session.tempTableOnCommit.count("old_name") == 0);
    assert(session.tempTableOnCommit.at("new_name") == "preserve");
    assert(!g_engine.tableExists(database, oldPhysical));
    assert(g_engine.tableExists(database, newPhysical));
    assert(!g_engine.tableExists(database, "new_name"));
    assert(g_engine.query(database, newPhysical, {"=id 1"}, {"id"}).size() ==
           1);

    assert(!ddl.executeSql("DROP TABLE new_name", session));
    assert(session.tempTables.count("new_name") == 0);
    assert(!g_engine.tableExists(database, newPhysical));

    assert(!ddl.executeSql(
        "CREATE TEMP TABLE delete_rows (id INT PRIMARY KEY) "
        "ON COMMIT DELETE ROWS",
        session));
    const std::string deleteOldPhysical =
        tempTablePrefix(session, "delete_rows");
    const std::string deleteNewPhysical =
        tempTablePrefix(session, "renamed_delete_rows");
    assert(g_engine.insert(database, deleteOldPhysical, {{"id", "7"}}) ==
           dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "ALTER TABLE delete_rows RENAME TO renamed_delete_rows", session));
    assert(session.tempTableOnCommit.at("renamed_delete_rows") == "delete");
    assert(g_engine.tableExists(database, deleteNewPhysical));
    assert(g_engine.query(
               database, deleteNewPhysical, {}, {"id"}).empty());
    assert(!ddl.executeSql("DROP TABLE renamed_delete_rows", session));

    cleanupTestDb("temp_table_ddl");
    std::cout << "[TEMP DDL] session temporary table rename/drop lifecycle OK"
              << std::endl;
    return 0;
}
