#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "process/RuntimeStats.h"
#include "Session.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

#include "test_utils.h"

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) {
    if (std::filesystem::exists(db)) std::filesystem::remove_all(db);
}

static void setupSession(Session& session, const std::string& db) {
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
}

static void insertRows(const std::string& db, const std::string& table,
                       int count) {
    for (int id = 0; id < count; ++id) {
        assert(g_engine.insert(db, table, {{"id", std::to_string(id)}}) ==
               dbms::DBStatus::OK);
    }
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::resetRuntimeStats();

    const std::string db = testDbPath("planner_runtime_stats");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session session;
    setupSession(session, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE left_side (id INT)", session));
    assert(!ddl.executeSql("CREATE TABLE right_side (id INT)", session));
    insertRows(db, "left_side", 100);
    insertRows(db, "right_side", 10);
    assert(g_engine.analyzeTable(db, "left_side"));
    assert(g_engine.analyzeTable(db, "right_side"));

    // With no exact process-local scan evidence, the planner falls back to
    // durable physical counts and selects hash join for 100 x 10.
    auto physicalPlan = dbms::QueryPlanner::buildJoinPlan(
        &g_engine, db, "left_side", "right_side", "id", "id", {}, {});
    assert(dynamic_cast<dbms::HashJoinOp*>(physicalPlan.get()));

    // A complete scan is exact enough to feed planning. Deliberately make
    // the observed cardinalities reverse the join preference and verify the
    // choice changes without changing on-disk rows.
    dbms::recordTableScan(db, "left_side", 1, false, true);
    dbms::recordTableScan(db, "right_side", 1000, false, true);
    uint64_t rows = 0;
    assert(dbms::getRuntimeLiveRowEstimate(db, "left_side", rows) && rows == 1);
    assert(dbms::getRuntimeLiveRowEstimate(db, "right_side", rows) && rows == 1000);

    auto runtimePlan = dbms::QueryPlanner::buildJoinPlan(
        &g_engine, db, "left_side", "right_side", "id", "id", {}, {});
    assert(dynamic_cast<dbms::NestedLoopJoinOp*>(runtimePlan.get()));

    dbms::PlanContext context;
    context.dbname = db;
    context.tablename = "left_side";
    auto explainPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    const std::string explain = dbms::QueryPlanner::explain(
        explainPlan, &g_engine, db);
    assert(explain.find("rows=1") != std::string::npos);

    // Recreating a same-name relation must not inherit the old estimate.
    assert(g_engine.dropTable(db, "left_side") == dbms::DBStatus::OK);
    assert(!dbms::getRuntimeLiveRowEstimate(db, "left_side", rows));

    cleanup(db);
    dbms::resetRuntimeStats();
    std::cout << "[PLANNER-STATS] runtime row estimates, fallback and invalidation OK\n";
    return 0;
}
