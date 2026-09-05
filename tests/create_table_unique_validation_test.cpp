#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

dbms::TableSchema apiTable(
    const std::string& name,
    const std::vector<size_t>& uniqueColumns) {
    dbms::TableSchema table;
    table.tablename = name;
    table.append(dbms::makeIntColumn("id", true, 4));
    table.append(dbms::makeIntColumn("code", true, 4));
    table.uniqueConstraints.push_back(uniqueColumns);
    table.uniqueConstraintNames.push_back(name + "_key");
    return table;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "create_table_unique_validation";
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;

    assert(ddl.executeSql(
        "CREATE TABLE missing_unique (id INT, code INT, "
        "UNIQUE (code, missing))",
        session));
    assert(!g_engine.tableExists(database, "missing_unique"));

    assert(ddl.executeSql(
        "CREATE TABLE duplicate_unique (id INT, UNIQUE (id, id))",
        session));
    assert(!g_engine.tableExists(database, "duplicate_unique"));

    assert(ddl.executeSql(
        "CREATE TABLE empty_unique (id INT, UNIQUE ())", session));
    assert(!g_engine.tableExists(database, "empty_unique"));

    dbms::TableSchema emptyApi = apiTable("empty_api_unique", {});
    assert(g_engine.createTable(database, emptyApi) ==
           dbms::DBStatus::INVALID_ARGUMENT);
    assert(!g_engine.tableExists(database, emptyApi.tablename));

    dbms::TableSchema duplicateApi =
        apiTable("duplicate_api_unique", {0, 0});
    assert(g_engine.createTable(database, duplicateApi) ==
           dbms::DBStatus::INVALID_ARGUMENT);
    assert(!g_engine.tableExists(database, duplicateApi.tablename));

    dbms::TableSchema outOfRangeApi =
        apiTable("out_of_range_api_unique", {0, 2});
    assert(g_engine.createTable(database, outOfRangeApi) ==
           dbms::DBStatus::INVALID_ARGUMENT);
    assert(!g_engine.tableExists(database, outOfRangeApi.tablename));

    dbms::TableSchema orphanNameApi;
    orphanNameApi.tablename = "orphan_name_api_unique";
    orphanNameApi.append(dbms::makeIntColumn("id", true, 4));
    orphanNameApi.uniqueConstraintNames.push_back("orphan_key");
    assert(g_engine.createTable(database, orphanNameApi) ==
           dbms::DBStatus::INVALID_ARGUMENT);
    assert(!g_engine.tableExists(database, orphanNameApi.tablename));

    assert(!ddl.executeSql(
        "CREATE TABLE valid_unique (id INT PRIMARY KEY, a INT, b INT, "
        "CONSTRAINT valid_pair_key UNIQUE (a, b))",
        session));
    const dbms::TableSchema valid =
        g_engine.getTableSchema(database, "valid_unique");
    const std::vector<std::vector<size_t>> expectedUnique{{1, 2}};
    assert(valid.uniqueConstraints == expectedUnique);
    assert(valid.uniqueConstraintNames ==
           std::vector<std::string>{"valid_pair_key"});
    assert(g_engine.insert(database, "valid_unique",
                           {{"id", "1"}, {"a", "10"}, {"b", "20"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "valid_unique",
                           {{"id", "2"}, {"a", "10"}, {"b", "20"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[CREATE TABLE UNIQUE] definitions validated OK"
              << std::endl;
    return 0;
}
