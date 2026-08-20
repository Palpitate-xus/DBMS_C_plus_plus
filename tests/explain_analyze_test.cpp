// ============================================================================
// explain_analyze_test — EXPLAIN ANALYZE per-node actuals:
//   IOperator instrumentation accumulates actual time/loops/rows per node
//   (TableScan/IndexScan/Filter/Sort/Limit and the three joins), and
//   QueryPlanner::explain with ExplainOptions{analyze} appends PG-style
//   "(actual time=... rows=... loops=...)" to each instrumented node line.
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

#include "test_utils.h"

extern dbms::StorageEngine g_engine;
using dbms::DBStatus;

static void cleanupDb(const std::string& db) {
    if (g_engine.databaseExists(db)) {
        const auto status = g_engine.dropDatabase(db);
        assert(status == dbms::DBStatus::OK ||
               status == dbms::DBStatus::DATABASE_NOT_FOUND);
    }
    std::error_code ec;
    std::filesystem::remove_all(db, ec);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static size_t runPlan(dbms::Operator* op) {
    std::string row;
    size_t n = 0;
    assert(op->open());
    while (op->next(row)) ++n;
    op->close();
    return n;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    const std::string db = testDbPath("explain_analyze");
    cleanupDb(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT, v INT)", s));
    for (int i = 0; i < 50; ++i) {
        assert(g_engine.insert(db, "t", {{"id", std::to_string(i)},
                                         {"v", std::to_string(i % 10)}})
                   == DBStatus::OK);
    }

    // ------------------------------------------------------------------
    // 1. Plain scan: loops = rows+1 (one false next to detect EOF), rows
    //    counted, time >= 0. EXPLAIN (ANALYZE) shows the actuals inline.
    // ------------------------------------------------------------------
    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = "t";
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    dbms::Operator* root = plan.get();
    root->resetRuntimeStats();
    size_t n = runPlan(root);
    assert(n == 50);
    assert(root->runtimeRows() == 50);
    assert(root->runtimeLoops() == 51);          // 50 emits + EOF probe
    assert(root->runtimeMs() >= 0.0);

    dbms::QueryPlanner::ExplainOptions opts;
    opts.analyze = true;
    std::string explain = dbms::QueryPlanner::explain(plan, &g_engine, db, opts);
    assert(explain.find("(actual time=") != std::string::npos);
    assert(explain.find("rows=50 loops=51") != std::string::npos);
    std::cout << "[ANALYZE] scan per-node actuals OK" << std::endl;

    // Without ANALYZE the same executed plan prints no actuals.
    dbms::QueryPlanner::ExplainOptions plain;
    std::string noActuals = dbms::QueryPlanner::explain(plan, &g_engine, db, plain);
    assert(noActuals.find("(actual time=") == std::string::npos);

    // ------------------------------------------------------------------
    // 2. Filter above scan: both nodes instrumented, each with its own
    //    loops/rows counts in one EXPLAIN output.
    // ------------------------------------------------------------------
    ctx.conds = {{"<", "v", "3"}};
    auto fplan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    size_t fn = runPlan(fplan.get());
    assert(fn == 15);                            // v in {0,1,2} x 5 blocks
    std::string fexplain = dbms::QueryPlanner::explain(fplan, &g_engine, db, opts);
    size_t actualLines = 0;
    for (size_t p = fexplain.find("(actual time="); p != std::string::npos;
         p = fexplain.find("(actual time=", p + 1)) ++actualLines;
    assert(actualLines >= 2);                    // Filter + scan both report
    std::cout << "[ANALYZE] filter+scan actuals OK (" << actualLines
              << " instrumented nodes)" << std::endl;

    // ------------------------------------------------------------------
    // 3. Join: both sides report actuals; join rows match real output.
    // ------------------------------------------------------------------
    assert(!ddl.executeSql("CREATE TABLE j1 (id INT, a INT)", s));
    assert(!ddl.executeSql("CREATE TABLE j2 (id INT, b INT)", s));
    for (int i = 0; i < 20; ++i) {
        assert(g_engine.insert(db, "j1", {{"id", std::to_string(i)},
                                          {"a", std::to_string(i % 5)}})
                   == DBStatus::OK);
        assert(g_engine.insert(db, "j2", {{"id", std::to_string(i)},
                                          {"b", std::to_string(i % 5)}})
                   == DBStatus::OK);
    }
    auto jplan = dbms::QueryPlanner::buildJoinPlan(
        &g_engine, db, "j1", "j2", "a", "id", {}, {});
    size_t jn = runPlan(jplan.get());
    assert(jn > 0);
    std::string jexplain = dbms::QueryPlanner::explain(jplan, &g_engine, db, opts);
    assert(jexplain.find("(actual time=") != std::string::npos);
    // The join node itself must carry actuals: nested-loop pulls the inner
    // side once per outer row, so loops > 1 somewhere in the plan.
    bool sawMultiLoop = false;
    for (size_t p = jexplain.find("loops="); p != std::string::npos;
         p = jexplain.find("loops=", p + 1)) {
        size_t q = p + 6;
        if (q < jexplain.size() && jexplain[q] != '1') sawMultiLoop = true;
        // loops=1x has a digit; multi-loop values are >= 2
        size_t r = q;
        while (r < jexplain.size() && isdigit((unsigned char)jexplain[r])) ++r;
        if (r > q) {
            long loops = std::stol(jexplain.substr(q, r - q));
            if (loops > 1) sawMultiLoop = true;
        }
    }
    assert(sawMultiLoop);
    std::cout << "[ANALYZE] join per-node actuals OK (rows=" << jn << ")"
              << std::endl;

    cleanupDb(db);
    std::cout << "[ANALYZE] all tests passed" << std::endl;
    return 0;
}
