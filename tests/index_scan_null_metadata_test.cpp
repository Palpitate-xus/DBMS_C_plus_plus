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

std::pair<std::string, std::vector<std::string>> runPlan(
        const std::string& database, dbms::PlanContext context) {
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    const std::string explanation =
        dbms::QueryPlanner::explain(plan, &g_engine, database);
    auto result = dbms::QueryPlanner::executePlanChecked(std::move(plan));
    assert(result.ok);
    return {explanation, std::move(result.rows)};
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "index_scan_null_metadata";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE indexed_nulls ("
        "id INT PRIMARY KEY, tenant INT, state INT, ranged INT, payload TEXT)",
        session));
    assert(!ddl.executeSql(
        "CREATE INDEX indexed_nulls_tenant_idx ON indexed_nulls(tenant)",
        session));
    assert(!ddl.executeSql(
        "CREATE INDEX indexed_nulls_state_idx ON indexed_nulls(state)",
        session));
    assert(!ddl.executeSql(
        "CREATE INDEX indexed_nulls_ranged_gist "
        "ON indexed_nulls USING GIST (ranged)", session));

    assert(g_engine.insert(
               database, "indexed_nulls",
               {{"id", "1"}, {"tenant", "7"}, {"state", "1"},
                {"ranged", "50"}, {"payload", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "indexed_nulls",
               {{"id", "2"}, {"tenant", "7"}, {"state", "2"},
                {"ranged", "60"}, {"payload", ""}}) ==
           dbms::DBStatus::OK);

    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = "indexed_nulls";
    context.selectCols = {"payload"};

    context.conds = {{"=", "id", "1"}};
    auto [primaryExplain, primaryRows] = runPlan(database, context);
    assert(primaryExplain.find("IndexScan") != std::string::npos);
    assert(primaryRows == std::vector<std::string>{"NULL "});

    context.conds = {{"=", "state", "1"}};
    auto [secondaryExplain, secondaryRows] = runPlan(database, context);
    assert(secondaryExplain.find("IndexScan") != std::string::npos);
    assert(secondaryRows == std::vector<std::string>{"NULL "});

    // A buffering sort must retain the originating indexed RID as well.
    context.orderByCol = "payload";
    auto [sortedExplain, sortedRows] = runPlan(database, context);
    assert(sortedExplain.find("Sort") != std::string::npos);
    assert(sortedRows == std::vector<std::string>{"NULL "});
    context.orderByCol.clear();

    context.conds = {{"=", "tenant", "7"}, {"=", "state", "1"}};
    auto bitmapPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    auto* bitmapProject = dynamic_cast<dbms::ProjectOp*>(bitmapPlan.get());
    assert(bitmapProject != nullptr);
    auto* bitmapFilter =
        dynamic_cast<dbms::FilterOp*>(bitmapProject->child());
    assert(bitmapFilter != nullptr);
    assert(dynamic_cast<dbms::BitmapHeapScanOp*>(bitmapFilter->child()) !=
           nullptr);
    auto bitmapResult =
        dbms::QueryPlanner::executePlanChecked(std::move(bitmapPlan));
    assert(bitmapResult.ok);
    assert(bitmapResult.rows == std::vector<std::string>{"NULL "});

    context.conds = {{">=", "ranged", "50"}, {"<=", "ranged", "50"}};
    auto [gistExplain, gistRows] = runPlan(database, context);
    assert(gistExplain.find("GiSTScan") != std::string::npos);
    assert(gistRows == std::vector<std::string>{"NULL "});

    // A real empty string remains a zero-length value on the same paths.
    context.conds = {{"=", "id", "2"}};
    assert(runPlan(database, context).second ==
           std::vector<std::string>{" "});
    context.conds = {{">=", "ranged", "60"}, {"<=", "ranged", "60"}};
    assert(runPlan(database, context).second ==
           std::vector<std::string>{" "});

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[INDEX NULL METADATA] all indexed scans preserve NULL"
              << std::endl;
    return 0;
}
