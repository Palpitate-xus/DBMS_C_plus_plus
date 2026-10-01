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
    const std::string database = testDbPath("foreign_key_self_insert");
    cleanupTestDb("foreign_key_self_insert");
    assert(g_engine.createDatabase(database) == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE nodes(id INT PRIMARY KEY,pid INT REFERENCES nodes(id))", session));
    assert(g_engine.insert(database, "nodes", {{"id", "1"}, {"pid", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "nodes", {{"id", "2"}, {"pid", "999"}}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.insert(database, "nodes", {{"id", "2"}, {"pid", "2"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "nodes", {{"id", "3"}, {"pid", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "nodes", {{"id", "1"}, {"pid", "1"}}) == dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.query(database, "nodes", {}, {"id", "pid"}).size() == 3);
    assert(!ddl.executeSql("CREATE TABLE unique_nodes(id INT PRIMARY KEY,code TEXT UNIQUE,parent_code TEXT REFERENCES unique_nodes(code))", session));
    assert(g_engine.insert(database, "unique_nodes", {{"id", "1"}, {"code", "own"}, {"parent_code", "own"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_nodes", {{"id", "2"}, {"code", ""}, {"parent_code", ""}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_nodes", {{"id", "3"}, {"code", "other"}, {"parent_code", "missing"}}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(!ddl.executeSql("CREATE TABLE pair_nodes(id INT PRIMARY KEY,a INT,b TEXT,pa INT,pb TEXT,UNIQUE(a,b),FOREIGN KEY(pa,pb) REFERENCES pair_nodes(a,b))", session));
    assert(g_engine.insert(database, "pair_nodes", {{"id", "1"}, {"a", "7"}, {"b", ""}, {"pa", "07"}, {"pb", ""}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "pair_nodes", {{"id", "2"}, {"a", "8"}, {"b", "other"}, {"pa", "7"}, {"pb", ""}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "pair_nodes", {{"id", "3"}, {"a", "9"}, {"b", "third"}, {"pa", "9"}, {"pb", "missing"}}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    cleanupTestDb("foreign_key_self_insert");
    std::cout << "[FOREIGN KEY SELF INSERT] passed" << std::endl;
}
