#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("unique_prefix_collision");
    std::filesystem::remove_all(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE unique_prefix(id INT PRIMARY KEY,code TEXT COLLATE \"C\" UNIQUE)", session));
    assert(!ddl.executeSql(
        "CREATE INDEX unique_prefix_code ON unique_prefix(code)", session));
    const std::string prefix = "12345678901234567890";
    assert(g_engine.insert(database, "unique_prefix", {{"id", "1"}, {"code", prefix + "-alpha"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_prefix", {{"id", "2"}, {"code", prefix + "-beta"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_prefix", {{"id", "3"}, {"code", prefix}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_prefix", {{"id", "4"}, {"code", prefix + "-alpha"}}) == dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(database, "unique_prefix", {{"id", "4"}, {"code", prefix}}) == dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(database, "unique_prefix", {{"id", "5"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_prefix", {{"id", "6"}}) == dbms::DBStatus::OK);
    assert(g_engine.remove(database, "unique_prefix", {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_prefix", {{"id", "7"}, {"code", prefix + "-alpha"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_prefix", {{"id", "8"}, {"code", prefix + "-beta"}}) == dbms::DBStatus::DUPLICATE_KEY);
    assert(!ddl.executeSql(
        "CREATE TABLE unique_numeric(id INT PRIMARY KEY,n NUMERIC UNIQUE)", session));
    assert(g_engine.insert(database, "unique_numeric", {{"id", "1"}, {"n", "123456789012345678901"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_numeric", {{"id", "2"}, {"n", "123456789012345678902"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_numeric", {{"id", "3"}, {"n", "123456789012345678901.0"}}) == dbms::DBStatus::DUPLICATE_KEY);
    std::filesystem::remove_all(database);
    std::cout << "[UNIQUE PREFIX COLLISION] passed" << std::endl;
}
