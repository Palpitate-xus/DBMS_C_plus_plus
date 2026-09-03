#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <set>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::vector<std::string> executePlan(dbms::PlanContext context) {
    auto result = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
    assert(result.ok);
    return result.rows;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "empty_string_query";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE empty_output ("
        "id INT PRIMARY KEY, required_text TEXT NOT NULL, "
        "optional_text TEXT)", session));

    assert(g_engine.insert(
               database, "empty_output",
               {{"id", "1"}, {"required_text", ""},
                {"optional_text", ""}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "empty_output",
               {{"id", "2"}, {"required_text", "value"},
                {"optional_text", "NULL"}}) == dbms::DBStatus::OK);

    // The legacy storage query API uses one trailing space per output cell.
    // An empty value is therefore " ", while a physical NULL is "NULL ".
    assert(g_engine.query(database, "empty_output", {"=id 1"},
                          {"required_text"}) ==
           std::vector<std::string>{" "});
    assert(g_engine.query(database, "empty_output", {"=id 1"},
                          {"optional_text"}) ==
           std::vector<std::string>{" "});
    assert(g_engine.query(database, "empty_output", {"=id 2"},
                          {"optional_text"}) ==
           std::vector<std::string>{"NULL "});

    // The volcano projection path feeds the PostgreSQL protocol layer and
    // must preserve exactly the same distinction.
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = "empty_output";
    context.selectCols = {"required_text", "optional_text"};
    assert(executePlan(context) ==
           (std::vector<std::string>{"  ", "value NULL "}));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[EMPTY QUERY] empty strings remain distinct from NULL"
              << std::endl;
    return 0;
}
