#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("foreign_key_insert_sqlstate");
    cleanupTestDb("foreign_key_insert_sqlstate");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE parent(id INT PRIMARY KEY,code INT UNIQUE,g INT,UNIQUE(code,g))", session));
    assert(!ddl.executeSql("CREATE TABLE child_pk(id INT PRIMARY KEY,pid INT REFERENCES parent(id))", session));
    assert(!ddl.executeSql("CREATE TABLE child_unique(id INT PRIMARY KEY,code INT REFERENCES parent(code))", session));
    assert(!ddl.executeSql("CREATE TABLE child_pair(id INT PRIMARY KEY,code INT,g INT,FOREIGN KEY(code,g) REFERENCES parent(code,g))", session));
    assert(g_engine.insert(database, "parent", {{"id", "7"}, {"code", "9"}, {"g", "11"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child_pk", {{"id", "1"}, {"pid", "99"}}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.insert(database, "child_unique", {{"id", "1"}, {"code", "7"}}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.insert(database, "child_pair", {{"id", "1"}, {"code", "9"}, {"g", "12"}}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    for (const std::string table : {"child_pk", "child_unique", "child_pair"})
        assert(g_engine.query(database, table, {}, {"id"}).empty());
    assert(g_engine.insert(database, "child_pk", {{"id", "1"}, {"pid", "7"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child_unique", {{"id", "1"}, {"code", "9"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child_pair", {{"id", "1"}, {"code", "9"}, {"g", "11"}}) == dbms::DBStatus::OK);
    assert(dbms::sqlstateForDBStatus(dbms::DBStatus::FOREIGN_KEY_VIOLATION) == "23503");
    cleanupTestDb("foreign_key_insert_sqlstate");
    std::cout << "[FOREIGN KEY INSERT SQLSTATE] passed" << std::endl;
}
