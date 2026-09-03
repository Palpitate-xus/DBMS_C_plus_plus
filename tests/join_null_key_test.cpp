#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <set>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "join_null_key";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE join_left (id INT PRIMARY KEY, k TEXT)", session));
    assert(!ddl.executeSql(
        "CREATE TABLE join_right (id INT PRIMARY KEY, k TEXT)", session));

    assert(g_engine.insert(
               database, "join_left", {{"id", "1"}, {"k", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "join_left", {{"id", "2"}, {"k", ""}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "join_left", {{"id", "3"}, {"k", "x"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "join_right", {{"id", "10"}, {"k", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "join_right", {{"id", "20"}, {"k", ""}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "join_right", {{"id", "30"}, {"k", "x"}}) ==
           dbms::DBStatus::OK);

    const std::set<std::string> columns = {"join_left.id", "join_right.id"};
    assert(g_engine.join(
               database, "join_left", "join_right", "k", "k", {}, columns) ==
           (std::vector<std::string>{"2 20 ", "3 30 "}));
    assert(g_engine.leftJoin(
               database, "join_left", "join_right", "k", "k", {}, columns) ==
           (std::vector<std::string>{"1 NULL ", "2 20 ", "3 30 "}));
    assert(g_engine.rightJoin(
               database, "join_left", "join_right", "k", "k", {}, columns) ==
           (std::vector<std::string>{"NULL 10 ", "2 20 ", "3 30 "}));
    assert(g_engine.fullOuterJoin(
               database, "join_left", "join_right", "k", "k", {}, columns) ==
           (std::vector<std::string>{
               "1 NULL ", "2 20 ", "3 30 ", "NULL 10 "}));

    assert(!ddl.executeSql(
        "CREATE TABLE bag_left (id INT, k INT)", session));
    assert(!ddl.executeSql(
        "CREATE TABLE bag_right (id INT, k INT)", session));
    assert(g_engine.insert(
               database, "bag_left", {{"id", "1"}, {"k", "1"}}) ==
           dbms::DBStatus::OK);
    for (int i = 0; i < 2; ++i) {
        assert(g_engine.insert(
                   database, "bag_right", {{"id", "20"}, {"k", "2"}}) ==
               dbms::DBStatus::OK);
    }
    assert(g_engine.fullOuterJoin(
               database, "bag_left", "bag_right", "k", "k", {}, {}) ==
           (std::vector<std::string>{
               "1 1 NULL NULL ",
               "NULL NULL 20 2 ",
               "NULL NULL 20 2 "}));

    cleanupTestDb(testName);
    std::cout << "[JOIN NULL KEY] NULL never equals NULL or empty string"
              << std::endl;
    return 0;
}
