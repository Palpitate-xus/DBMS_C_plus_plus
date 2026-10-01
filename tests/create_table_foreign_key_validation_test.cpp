#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "create_table_foreign_key_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE parent (id INT PRIMARY KEY, code INT UNIQUE, "
        "ordinary INT, a INT, b INT, UNIQUE (a, b))",
        session));

    assert(ddl.executeSql(
        "CREATE TABLE missing_parent (id INT, parent_id INT, "
        "FOREIGN KEY (parent_id) REFERENCES absent(id))",
        session));
    assert(!g_engine.tableExists(database, "missing_parent"));

    assert(ddl.executeSql(
        "CREATE TABLE missing_local (id INT, "
        "FOREIGN KEY (parent_id) REFERENCES parent(id))",
        session));
    assert(!g_engine.tableExists(database, "missing_local"));

    assert(ddl.executeSql(
        "CREATE TABLE missing_referenced (id INT, parent_id INT, "
        "FOREIGN KEY (parent_id) REFERENCES parent(absent))",
        session));
    assert(!g_engine.tableExists(database, "missing_referenced"));

    assert(ddl.executeSql(
        "CREATE TABLE mismatched_arity (id INT, x INT, y INT, "
        "FOREIGN KEY (x, y) REFERENCES parent(id))",
        session));
    assert(!g_engine.tableExists(database, "mismatched_arity"));

    assert(ddl.executeSql(
        "CREATE TABLE non_unique_target (id INT, parent_value INT, "
        "FOREIGN KEY (parent_value) REFERENCES parent(ordinary))",
        session));
    assert(!g_engine.tableExists(database, "non_unique_target"));

    assert(ddl.executeSql(
        "CREATE TABLE incompatible_type (id INT, parent_code TEXT, "
        "FOREIGN KEY (parent_code) REFERENCES parent(code))",
        session));
    assert(!g_engine.tableExists(database, "incompatible_type"));

    // The storage API must enforce the same invariant even when SQL parsing
    // is bypassed.
    dbms::TableSchema invalidApiTable;
    invalidApiTable.tablename = "invalid_api_table";
    invalidApiTable.append(dbms::makeIntColumn("parent_value", true, 4));
    dbms::ForeignKey invalidApiFk;
    invalidApiFk.colNames = {"parent_value"};
    invalidApiFk.refTable = "parent";
    invalidApiFk.refCols = {"ordinary"};
    invalidApiTable.appendFK(invalidApiFk);
    assert(g_engine.createTable(database, invalidApiTable) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(!g_engine.tableExists(database, "invalid_api_table"));

    // Omitting the referenced column list means the complete primary key.
    assert(!ddl.executeSql(
        "CREATE TABLE implicit_target (id INT PRIMARY KEY, parent_id INT, "
        "FOREIGN KEY (parent_id) REFERENCES parent)",
        session));
    const dbms::TableSchema implicitSchema =
        g_engine.getTableSchema(database, "implicit_target");
    assert(implicitSchema.fkLen == 1);
    assert(implicitSchema.fks[0].refCols ==
           std::vector<std::string>{"id"});

    assert(!ddl.executeSql(
        "CREATE TABLE unique_target (id INT PRIMARY KEY, parent_code INT, "
        "FOREIGN KEY (parent_code) REFERENCES parent(code))",
        session));
    assert(!ddl.executeSql(
        "CREATE TABLE composite_target (id INT PRIMARY KEY, x INT, y INT, "
        "FOREIGN KEY (x, y) REFERENCES parent(a, b))",
        session));
    assert(!ddl.executeSql(
        "CREATE TABLE node (id INT PRIMARY KEY, parent_id INT, "
        "FOREIGN KEY (parent_id) REFERENCES node(id))",
        session));

    assert(g_engine.insert(database, "parent",
                           {{"id", "1"}, {"code", "10"},
                            {"ordinary", "100"}, {"a", "20"},
                            {"b", "30"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "implicit_target",
                           {{"id", "1"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "implicit_target",
                           {{"id", "2"}, {"parent_id", "999"}}) ==
           dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.insert(database, "unique_target",
                           {{"id", "1"}, {"parent_code", "10"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "composite_target",
                           {{"id", "1"}, {"x", "20"}, {"y", "30"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "node",
                           {{"id", "1"}, {"parent_id", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "node",
                           {{"id", "2"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[CREATE TABLE FK] definitions validated before publication OK"
              << std::endl;
    return 0;
}
