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
    const std::string database = testDbPath("drop_multiple_fk_group");
    cleanupTestDb("drop_multiple_fk_group");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE parent(id INT PRIMARY KEY)", session));
    assert(!ddl.executeSql("CREATE TABLE child(id INT PRIMARY KEY,pid INT REFERENCES parent(id))", session));
    assert(!ddl.executeSql("DROP TABLE parent,child", session));
    assert(!g_engine.tableExists(database, "parent") && !g_engine.tableExists(database, "child"));
    assert(!ddl.executeSql("CREATE TABLE parent(id INT PRIMARY KEY)", session));
    assert(!ddl.executeSql("CREATE TABLE child(id INT PRIMARY KEY,pid INT REFERENCES parent(id))", session));
    assert(!ddl.executeSql("CREATE TABLE survivor(id INT PRIMARY KEY,pid INT REFERENCES parent(id))", session));
    assert(g_engine.insert(database, "parent", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "child", {{"id", "2"}, {"pid", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "survivor", {{"id", "3"}, {"pid", "1"}}) == dbms::DBStatus::OK);
    assert(ddl.executeSql("DROP TABLE child,parent", session));
    assert(g_engine.query(database, "parent", {}, {"id"}) == std::vector<std::string>{"1 "});
    assert(g_engine.query(database, "child", {}, {"id"}) == std::vector<std::string>{"2 "});
    assert(g_engine.query(database, "survivor", {}, {"id"}) == std::vector<std::string>{"3 "});
    assert(!ddl.executeSql("DROP TABLE survivor", session));
    assert(!ddl.executeSql("DROP TABLE parent,child", session));
    assert(!ddl.executeSql("CREATE TABLE cycle_a(id INT PRIMARY KEY,bid INT)", session));
    assert(!ddl.executeSql("CREATE TABLE cycle_b(id INT PRIMARY KEY,aid INT REFERENCES cycle_a(id))", session));
    assert(!ddl.executeSql("ALTER TABLE cycle_a ADD CONSTRAINT cycle_a_fkey FOREIGN KEY(bid) REFERENCES cycle_b(id)", session));
    assert(!ddl.executeSql("DROP TABLE public.cycle_a,cycle_b", session));
    assert(!g_engine.tableExists(database, "cycle_a") && !g_engine.tableExists(database, "cycle_b"));
    cleanupTestDb("drop_multiple_fk_group");
    std::cout << "[DROP MULTIPLE FK GROUP] passed" << std::endl;
}
