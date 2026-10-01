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
    const std::string database = testDbPath("foreign_key_statement_visibility");
    cleanupTestDb("foreign_key_statement_visibility");
    assert(g_engine.createDatabase(database) == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE nodes(id INT PRIMARY KEY,pid INT REFERENCES nodes(id))", session));
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    assert(g_engine.insert(database, "nodes", {{"id", "1"}, {"pid", "2"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "nodes", {{"id", "2"}}) == dbms::DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.validateImmediateForeignKeyChecks() == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(g_engine.query(database, "nodes", {}, {"id", "pid"}).size() == 2);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    assert(g_engine.insert(database, "nodes", {{"id", "3"}, {"pid", "999"}}) == dbms::DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.validateImmediateForeignKeyChecks() == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.query(database, "nodes", {}, {"id", "pid"}).size() == 2);
    assert(!ddl.executeSql("CREATE TABLE char_nodes(id INT PRIMARY KEY,code CHAR(4) UNIQUE,parent_code CHAR(2) REFERENCES char_nodes(code))", session));
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    assert(g_engine.insert(database, "char_nodes", {{"id", "1"}, {"code", "aa"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "char_nodes", {{"id", "2"}, {"code", "bb"}, {"parent_code", "aa"}}) == dbms::DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.validateImmediateForeignKeyChecks() == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE char_primary(code CHAR(4) PRIMARY KEY,parent_code CHAR(2) REFERENCES char_primary(code))", session));
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    assert(g_engine.insert(database, "char_primary", {{"code", "aa"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "char_primary", {{"code", "bb"}, {"parent_code", "aa"}}) == dbms::DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.validateImmediateForeignKeyChecks() == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE long_primary(code TEXT PRIMARY KEY,parent_code TEXT REFERENCES long_primary(code))", session));
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    assert(g_engine.insert(database, "long_primary", {{"code", "12345678901234567890first"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "long_primary", {{"code", "child"}, {"parent_code", "12345678901234567890other"}}) == dbms::DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.validateImmediateForeignKeyChecks() == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!ddl.executeSql("CREATE TABLE deferred_nodes(id INT PRIMARY KEY,pid INT,CONSTRAINT statement_deferred_fk FOREIGN KEY(pid) REFERENCES deferred_nodes(id) DEFERRABLE INITIALLY DEFERRED)", session));
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    assert(g_engine.insert(database, "deferred_nodes", {{"id", "1"}, {"pid", "999"}}) == dbms::DBStatus::OK);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.validateImmediateForeignKeyChecks() == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.query(database, "deferred_nodes", {}, {"id"}).empty());
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("foreign_key_statement_visibility");
    std::cout << "[FOREIGN KEY STATEMENT VISIBILITY] passed" << std::endl;
}
