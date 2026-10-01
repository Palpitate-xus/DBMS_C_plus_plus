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
    const std::string database = testDbPath("foreign_key_modify_sqlstate");
    cleanupTestDb("foreign_key_modify_sqlstate");
    assert(g_engine.createDatabase(database) == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE parent(id INT PRIMARY KEY,code TEXT UNIQUE)", session));
    assert(!ddl.executeSql("CREATE TABLE child(id INT PRIMARY KEY,pid INT REFERENCES parent(id),code TEXT REFERENCES parent(code))", session));
    assert(g_engine.insert(database, "parent", {{"id", "1"}, {"code", "kept"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child", {{"id", "2"}, {"pid", "1"}, {"code", "kept"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "child", {{"pid", "77"}}, {"=id 2"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.update(database, "child", {{"code", "absent"}}, {"=id 2"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.update(database, "parent", {{"id", "3"}}, {"=id 1"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.update(database, "parent", {{"code", "new"}}, {"=id 1"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.remove(database, "parent", {"=id 1"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.query(database, "parent", {}, {"id", "code"}) == std::vector<std::string>{"1 kept "});
    assert(g_engine.query(database, "child", {}, {"id", "pid", "code"}) == std::vector<std::string>{"2 1 kept "});
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("foreign_key_modify_sqlstate");
    std::cout << "[FOREIGN KEY MODIFY SQLSTATE] passed" << std::endl;
}
