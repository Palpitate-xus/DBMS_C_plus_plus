#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void cleanup(const std::string& database) {
    if (std::filesystem::exists(database)) {
        std::filesystem::remove_all(database);
    }
}

Session makeSession(const std::string& database) {
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    return session;
}

void testInlineUniqueDistinguishesEmptyFromNull() {
    const std::string database = testDbPath("empty_inline_unique");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE inline_unique ("
        "id INT PRIMARY KEY, code VARCHAR(20) UNIQUE)", session));

    assert(g_engine.insert(database, "inline_unique",
                           {{"id", "1"}, {"code", ""}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "inline_unique",
                           {{"id", "2"}, {"code", ""}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    // SQL NULL values remain distinct under an ordinary UNIQUE constraint.
    assert(g_engine.insert(database, "inline_unique", {{"id", "3"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "inline_unique", {{"id", "4"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.update(database, "inline_unique", {{"code", ""}},
                           {"=id 3"}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.remove(database, "inline_unique", {"=id 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "inline_unique", {{"code", ""}},
                           {"=id 3"}) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "inline_unique", {{"code", ""}},
                           {"=id 4"}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    cleanup(database);
}

void testCompositeUniqueKeepsFieldBoundaries() {
    const std::string database = testDbPath("empty_composite_unique");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE composite_unique ("
        "id INT PRIMARY KEY, left_value VARCHAR(40), "
        "right_value VARCHAR(40), UNIQUE (left_value, right_value))",
        session));

    assert(g_engine.insert(
               database, "composite_unique",
               {{"id", "1"}, {"left_value", ""}, {"right_value", "x"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "composite_unique",
               {{"id", "2"}, {"left_value", ""}, {"right_value", "x"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(
               database, "composite_unique",
               {{"id", "3"}, {"left_value", ""}, {"right_value", "y"}}) ==
           dbms::DBStatus::OK);

    const std::string separator(1, '\x01');
    assert(g_engine.insert(
               database, "composite_unique",
               {{"id", "4"}, {"left_value", "a"},
                {"right_value", "b" + separator + "c"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "composite_unique",
               {{"id", "5"}, {"left_value", "a" + separator + "b"},
                {"right_value", "c"}}) == dbms::DBStatus::OK);

    // Any physical NULL suppresses the composite UNIQUE comparison.
    assert(g_engine.insert(database, "composite_unique",
                           {{"id", "6"}, {"right_value", "x"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "composite_unique",
                           {{"id", "7"}, {"right_value", "x"}}) ==
           dbms::DBStatus::OK);
    cleanup(database);
}

void testDeferredUniqueChecksEmptyAtCommit() {
    const std::string database = testDbPath("empty_deferred_unique");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE deferred_unique ("
        "id INT PRIMARY KEY, code VARCHAR(20), "
        "CONSTRAINT deferred_code_key UNIQUE (code) "
        "DEFERRABLE INITIALLY DEFERRED)", session));

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "deferred_unique",
                           {{"id", "1"}, {"code", ""}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "deferred_unique",
                           {{"id", "2"}, {"code", ""}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::INVALID_VALUE);
    cleanup(database);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    testInlineUniqueDistinguishesEmptyFromNull();
    testCompositeUniqueKeepsFieldBoundaries();
    testDeferredUniqueChecksEmptyAtCommit();
    std::cout << "[EMPTY UNIQUE] all passed" << std::endl;
    return 0;
}
