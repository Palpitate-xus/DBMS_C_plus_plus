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

namespace {

std::vector<std::string> execute(dbms::PlanContext context) {
    auto result = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
    assert(result.ok);
    return std::move(result.rows);
}

dbms::PlanContext baseContext(const std::string& database) {
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = "outer_rows";
    context.selectCols = {"payload"};
    context.orderByCol = "id";
    return context;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "subquery_filter_null_metadata";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE outer_rows ("
        "id INT PRIMARY KEY, match_key INT, payload TEXT)", session));
    assert(!ddl.executeSql(
        "CREATE TABLE inner_rows (match_key INT)", session));
    assert(!ddl.executeSql(
        "CREATE TABLE empty_inner_rows (match_key INT)", session));

    assert(g_engine.insert(
               database, "outer_rows",
               {{"id", "1"}, {"match_key", "10"}, {"payload", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "outer_rows",
               {{"id", "2"}, {"match_key", "20"}, {"payload", ""}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "outer_rows",
               {{"id", "3"}, {"match_key", "30"}, {"payload", "value"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "outer_rows",
               {{"id", "4"}, {"match_key", "NULL"},
                {"payload", "null-key"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "outer_rows",
               {{"id", "5"}, {"match_key", "40"},
                {"payload", "unmatched"}}) == dbms::DBStatus::OK);
    for (const std::string key : {"10", "20", "30"}) {
        assert(g_engine.insert(database, "inner_rows", {{"match_key", key}}) ==
               dbms::DBStatus::OK);
    }
    assert(g_engine.insert(
               database, "inner_rows", {{"match_key", "NULL"}}) ==
           dbms::DBStatus::OK);

    dbms::PlanContext semiContext = baseContext(database);
    dbms::SemiJoinSpec semi;
    semi.dbname = database;
    semi.tablename = "inner_rows";
    semi.outerColumn = "match_key";
    semi.innerColumn = "match_key";
    semiContext.semiJoins.push_back(std::move(semi));
    assert(execute(std::move(semiContext)) ==
           (std::vector<std::string>{"NULL ", " ", "value "}));

    dbms::PlanContext existenceContext = baseContext(database);
    dbms::ExistenceSpec existence;
    existence.dbname = database;
    existence.tablename = "inner_rows";
    existenceContext.existenceFilters.push_back(std::move(existence));
    assert(execute(std::move(existenceContext)) ==
           (std::vector<std::string>{
               "NULL ", " ", "value ", "null-key ", "unmatched "}));

    dbms::PlanContext correlatedNotExistsContext = baseContext(database);
    dbms::ExistenceSpec correlatedNotExists;
    correlatedNotExists.dbname = database;
    correlatedNotExists.tablename = "inner_rows";
    correlatedNotExists.outerColumn = "match_key";
    correlatedNotExists.innerColumn = "match_key";
    correlatedNotExists.anti = true;
    correlatedNotExistsContext.existenceFilters.push_back(
        std::move(correlatedNotExists));
    assert(execute(std::move(correlatedNotExistsContext)) ==
           (std::vector<std::string>{"null-key ", "unmatched "}));

    dbms::PlanContext quantifiedContext = baseContext(database);
    dbms::QuantifiedSubquerySpec quantified;
    quantified.dbname = database;
    quantified.tablename = "inner_rows";
    quantified.outerColumn = "match_key";
    quantified.innerColumn = "match_key";
    quantified.op = "=";
    quantifiedContext.quantifiedSubqueries.push_back(std::move(quantified));
    assert(execute(std::move(quantifiedContext)) ==
           (std::vector<std::string>{"NULL ", " ", "value "}));

    dbms::PlanContext emptyNotInContext = baseContext(database);
    dbms::SemiJoinSpec emptyNotIn;
    emptyNotIn.dbname = database;
    emptyNotIn.tablename = "empty_inner_rows";
    emptyNotIn.outerColumn = "match_key";
    emptyNotIn.innerColumn = "match_key";
    emptyNotIn.anti = true;
    emptyNotInContext.semiJoins.push_back(std::move(emptyNotIn));
    assert(execute(std::move(emptyNotInContext)) ==
           (std::vector<std::string>{
               "NULL ", " ", "value ", "null-key ", "unmatched "}));

    dbms::PlanContext emptyAllContext = baseContext(database);
    dbms::QuantifiedSubquerySpec emptyAll;
    emptyAll.dbname = database;
    emptyAll.tablename = "empty_inner_rows";
    emptyAll.outerColumn = "match_key";
    emptyAll.innerColumn = "match_key";
    emptyAll.op = ">";
    emptyAll.all = true;
    emptyAllContext.quantifiedSubqueries.push_back(std::move(emptyAll));
    assert(execute(std::move(emptyAllContext)) ==
           (std::vector<std::string>{
               "NULL ", " ", "value ", "null-key ", "unmatched "}));

    dbms::PlanContext emptyAnyContext = baseContext(database);
    dbms::QuantifiedSubquerySpec emptyAny;
    emptyAny.dbname = database;
    emptyAny.tablename = "empty_inner_rows";
    emptyAny.outerColumn = "match_key";
    emptyAny.innerColumn = "match_key";
    emptyAny.op = ">";
    emptyAnyContext.quantifiedSubqueries.push_back(std::move(emptyAny));
    assert(execute(std::move(emptyAnyContext)).empty());

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SUBQUERY NULL METADATA] buffered filters preserve NULL"
              << std::endl;
    return 0;
}
