#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "composite_semi_join_key";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE outer_keys (id INT PRIMARY KEY, a VARCHAR(40), "
        "b VARCHAR(40))", session));
    assert(!ddl.executeSql(
        "CREATE TABLE inner_keys (a VARCHAR(40), b VARCHAR(40))", session));

    const std::string separator(1, '\x01');
    assert(g_engine.insert(
               database, "inner_keys", {{"a", ""}, {"b", "x"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "inner_keys",
               {{"a", "a"}, {"b", "b" + separator + "c"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "inner_keys", {{"a", "NULL"}, {"b", "z"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.insert(
               database, "outer_keys",
               {{"id", "1"}, {"a", ""}, {"b", "x"}}) ==
           dbms::DBStatus::OK);
    // This pair collides with ("a", "b\\x01c") if fields are joined with
    // a raw delimiter, but it is not the same SQL row value.
    assert(g_engine.insert(
               database, "outer_keys",
               {{"id", "2"}, {"a", "a" + separator + "b"}, {"b", "c"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "outer_keys",
               {{"id", "3"}, {"a", "a"},
                {"b", "b" + separator + "c"}}) == dbms::DBStatus::OK);
    // A physical NULL never equals a non-NULL empty string.
    assert(g_engine.insert(
               database, "outer_keys",
               {{"id", "4"}, {"a", ""}, {"b", "z"}}) ==
           dbms::DBStatus::OK);

    dbms::SemiJoinSpec semi;
    semi.dbname = database;
    semi.tablename = "inner_keys";
    semi.correlations = {{"a", "a"}, {"b", "b"}};

    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = "outer_keys";
    context.selectCols = {"id"};
    context.orderByCol = "id";
    context.semiJoins.push_back(std::move(semi));
    auto result = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
    assert(result.ok);
    assert(result.rows == (std::vector<std::string>{"1 ", "3 "}));

    dbms::ExistenceSpec notExists;
    notExists.dbname = database;
    notExists.tablename = "inner_keys";
    notExists.correlations = {{"a", "a"}, {"b", "b"}};
    notExists.anti = true;
    dbms::PlanContext notExistsContext;
    notExistsContext.dbname = database;
    notExistsContext.tablename = "outer_keys";
    notExistsContext.selectCols = {"id"};
    notExistsContext.orderByCol = "id";
    notExistsContext.existenceFilters.push_back(std::move(notExists));
    auto notExistsResult = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, notExistsContext));
    assert(notExistsResult.ok);
    assert(notExistsResult.rows == (std::vector<std::string>{"2 ", "4 "}));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[COMPOSITE SEMI JOIN] structured key semantics OK"
              << std::endl;
    return 0;
}
