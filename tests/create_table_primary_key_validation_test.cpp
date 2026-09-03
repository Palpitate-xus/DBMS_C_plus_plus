#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "create_table_primary_key_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;

    assert(ddl.executeSql(
        "CREATE TABLE missing_pk (id INT, PRIMARY KEY (missing))",
        session));
    assert(!g_engine.tableExists(database, "missing_pk"));

    assert(ddl.executeSql(
        "CREATE TABLE duplicate_pk_column (id INT, PRIMARY KEY (id, id))",
        session));
    assert(!g_engine.tableExists(database, "duplicate_pk_column"));

    assert(ddl.executeSql(
        "CREATE TABLE multiple_pk (id INT PRIMARY KEY, code INT, "
        "PRIMARY KEY (code))",
        session));
    assert(!g_engine.tableExists(database, "multiple_pk"));

    assert(!ddl.executeSql(
        "CREATE TABLE valid_pk (id INT NULL, payload TEXT, PRIMARY KEY (id))",
        session));
    const dbms::TableSchema schema =
        g_engine.getTableSchema(database, "valid_pk");
    assert(schema.len == 2);
    assert(schema.cols[0].isPrimaryKey);
    assert(!schema.cols[0].isNull);
    assert(g_engine.insert(
               database, "valid_pk", {{"id", "NULL"}, {"payload", "x"}}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);

    cleanupTestDb(testName);
    std::cout << "[CREATE TABLE PK] definitions validated OK" << std::endl;
    return 0;
}
