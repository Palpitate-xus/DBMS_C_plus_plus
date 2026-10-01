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
    const std::string database = testDbPath("foreign_key_empty_text");
    cleanupTestDb("foreign_key_empty_text");
    assert(g_engine.createDatabase(database) == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE parent(id INT PRIMARY KEY,code TEXT UNIQUE)", session));
    assert(!ddl.executeSql("CREATE TABLE child(id INT PRIMARY KEY,code TEXT REFERENCES parent(code))", session));
    assert(g_engine.insert(database, "parent", {{"id", "1"}, {"code", "kept"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "parent", {{"id", "2"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child", {{"id", "10"}, {"code", "kept"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "child", {{"code", ""}}, {"=id 10"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.insert(database, "child", {{"id", "20"}, {"code", ""}}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.query(database, "child", {}, {"code"}) == std::vector<std::string>{"kept "});
    assert(g_engine.insert(database, "parent", {{"id", "3"}, {"code", ""}}) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "child", {{"code", ""}}, {"=id 10"}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE deferred_child(id INT PRIMARY KEY,code TEXT,CONSTRAINT deferred_fk FOREIGN KEY(code) REFERENCES parent(code) DEFERRABLE INITIALLY DEFERRED)", session));
    assert(g_engine.remove(database, "child", {"=id 10"}) == dbms::DBStatus::OK);
    assert(g_engine.remove(database, "parent", {"=id 3"}) == dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "deferred_child", {{"id", "30"}, {"code", ""}}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.query(database, "deferred_child", {}, {"id"}).empty());
    assert(!ddl.executeSql("CREATE TABLE pair_parent(a TEXT,b INT,UNIQUE(a,b))", session));
    assert(!ddl.executeSql("CREATE TABLE pair_child(id INT PRIMARY KEY,a TEXT,b INT,FOREIGN KEY(a,b) REFERENCES pair_parent(a,b))", session));
    assert(g_engine.insert(database, "pair_parent", {{"b", "7"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "pair_child", {{"id", "1"}, {"b", "7"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "pair_child", {{"a", ""}}, {"=id 1"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.insert(database, "pair_child", {{"id", "2"}, {"a", ""}, {"b", "7"}}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.insert(database, "pair_parent", {{"a", ""}, {"b", "7"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "pair_child", {{"a", ""}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE self_parent(id INT PRIMARY KEY,code TEXT UNIQUE,parent_code TEXT REFERENCES self_parent(code))", session));
    assert(g_engine.insert(database, "self_parent", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "self_parent", {{"parent_code", ""}}, {"=id 1"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.update(database, "self_parent", {{"code", ""}, {"parent_code", ""}}, {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("foreign_key_empty_text");
    std::cout << "[FOREIGN KEY EMPTY TEXT] passed" << std::endl;
}
