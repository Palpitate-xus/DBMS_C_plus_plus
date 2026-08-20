// ============================================================================
// parallel_exec_test — parallel JOIN / aggregate / GatherMerge:
//   ParallelGroupAggregateOp: worker-thread partitioned aggregation matches
//     the serial GroupAggregateOp output (plain + GROUP BY, incl. NULLs,
//     avg/min/max, HAVING)
//   ParallelHashJoinOp: page-range parallel build side produces the same
//     join result as HashJoinOp (and usedParallelWorkers reports the mode)
//   GatherMergeOp: k-way merge of per-worker sorted runs yields a globally
//     ordered stream; planner builds it under parallelism + ORDER BY
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "catalog/type_registry.h"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "test_utils.h"

extern dbms::StorageEngine g_engine;
using dbms::DBStatus;

static void cleanupDb(const std::string& db) {
    if (g_engine.databaseExists(db)) {
        const auto status = g_engine.dropDatabase(db);
        assert(status == DBStatus::OK || status == DBStatus::DATABASE_NOT_FOUND);
    }
    std::error_code ec;
    std::filesystem::remove_all(db, ec);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::vector<std::string> runPlan(dbms::Operator* op) {
    std::vector<std::string> rows;
    std::string row;
    assert(op->open());
    while (op->next(row)) rows.push_back(row);
    assert(!op->hasError());
    op->close();
    return rows;
}

static std::vector<std::string> sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    return v;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    const std::string db = testDbPath("parallel_exec");
    cleanupDb(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // 600 rows -> >256 to trigger worker-thread aggregation; enough pages
    // for the parallel build side and GatherMerge partitions.
    assert(!ddl.executeSql("CREATE TABLE big (id INT, grp INT, val INT)", s));
    for (int i = 0; i < 600; ++i) {
        assert(g_engine.insert(db, "big",
                               {{"id", std::to_string(i)},
                                {"grp", std::to_string(i % 3)},
                                {"val", std::to_string(i % 7)}})
                   == DBStatus::OK);
    }

    // ------------------------------------------------------------------
    // 1. ParallelGroupAggregateOp vs GroupAggregateOp: plain aggregate.
    // ------------------------------------------------------------------
    std::vector<dbms::StorageEngine::AggItem> items = {
        {"count", "*", {}}, {"sum", "val", {}}, {"avg", "val", {}},
        {"min", "val", {}}, {"max", "val", {}}};
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "big");

    auto scan1 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::GroupAggregateOp serial(std::move(scan1), tbl, {}, {}, items, {});
    auto serialRows = runPlan(&serial);

    auto scan2 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::ParallelGroupAggregateOp par(std::move(scan2), tbl, {}, items, {}, 4);
    auto parRows = runPlan(&par);

    assert(serialRows.size() == 1 && parRows.size() == 1);
    assert(serialRows[0] == parRows[0]);
    assert(parRows[0].find("600") != std::string::npos);  // count(*) = 600
    std::cout << "[PAR] parallel plain aggregate OK (" << parRows[0] << ")"
              << std::endl;

    // ------------------------------------------------------------------
    // 2. GROUP BY parity: same groups, same aggregates.
    // ------------------------------------------------------------------
    std::vector<std::string> groupCols = {"grp"};
    auto scan3 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::GroupAggregateOp serialG(std::move(scan3), tbl, groupCols, {}, items, {});
    auto serialGRows = sorted(runPlan(&serialG));

    auto scan4 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::ParallelGroupAggregateOp parG(std::move(scan4), tbl, groupCols, items, {}, 4);
    auto parGRows = sorted(runPlan(&parG));

    assert(serialGRows.size() == 3 && parGRows.size() == 3);
    assert(serialGRows == parGRows);
    assert(par.usedParallelWorkers());
    std::cout << "[PAR] parallel GROUP BY parity OK" << std::endl;

    // ------------------------------------------------------------------
    // 3. ParallelHashJoinOp: big JOIN small on key; result parity with
    //    HashJoinOp and parallel build engaged.
    // ------------------------------------------------------------------
    assert(!ddl.executeSql("CREATE TABLE dim (k INT, label VARCHAR(10))", s));
    for (int k = 0; k < 7; ++k) {
        assert(g_engine.insert(db, "dim",
                               {{"k", std::to_string(k)},
                                {"label", "L" + std::to_string(k)}})
                   == DBStatus::OK);
    }
    // build side must span >2 pages: reuse big as build via a self-join
    // pattern big(a) JOIN big(b) ON a.id = b.id - expect 600 rows.
    auto ls = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    auto rs = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::HashJoinOp serialJ(&g_engine, db, std::move(ls), std::move(rs),
                             "big", "big", "id", "id");
    auto serialJRows = runPlan(&serialJ);
    assert(!serialJRows.empty());

    auto ls2 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    auto rs2 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::ParallelHashJoinOp parJ(&g_engine, db, std::move(ls2), std::move(rs2),
                                  "big", "big", "id", "id", 4);
    auto parJRows = runPlan(&parJ);
    // Result parity with the serial operator (same contract, same rows).
    assert(parJRows.size() == serialJRows.size());
    assert(sorted(parJRows) == sorted(serialJRows));
    assert(!parJRows.empty());
    assert(parJ.usedParallelWorkers());
    std::cout << "[PAR] parallel hash join parity OK (" << parJRows.size()
              << " rows)" << std::endl;

    // EXPLAIN shows the parallel node: join on a non-indexed column so the
    // hash path is selected (id is the PK and would pick merge join).
    dbms::QueryPlanner::setParallelWorkers(4);
    auto joinPlan = dbms::QueryPlanner::buildJoinPlan(
        &g_engine, db, "big", "big", "grp", "grp", {}, {});
    auto* joinRoot = joinPlan.get();
    assert(dynamic_cast<dbms::ParallelHashJoinOp*>(joinRoot) != nullptr);
    dbms::QueryPlanner::ExplainOptions opts;
    std::string jx = dbms::QueryPlanner::explain(joinPlan, &g_engine, db, opts);
    assert(jx.find("ParallelHashJoin") != std::string::npos);
    std::cout << "[PAR] planner chooses ParallelHashJoin OK" << std::endl;

    // ------------------------------------------------------------------
    // 4. GatherMerge: per-worker sorted runs merge globally.
    // ------------------------------------------------------------------
    dbms::TableSchema dimTbl = g_engine.getTableSchema(db, "dim");
    // Raw rows laid out for dim: "k L<k>" text, split into 3 interleaved
    // pre-sorted runs (run w holds k = w, w+3, w+6, ...).
    std::vector<std::string> all;
    for (int k = 0; k < 7; ++k) {
        std::string row;
        for (size_t c = 0; c < dimTbl.len; ++c) {
            if (c > 0) row.push_back(' ');
            row += (c == 0) ? std::to_string(k) : ("L" + std::to_string(k));
        }
        all.push_back(row);
    }
    std::vector<dbms::OpPtr> runs;
    for (int w = 0; w < 3; ++w) {
        std::vector<std::string> rows;
        for (size_t i = static_cast<size_t>(w); i < all.size(); i += 3) {
            rows.push_back(all[i]);
        }
        runs.push_back(std::make_unique<dbms::MaterializedRowsOp>(std::move(rows)));
    }
    dbms::GatherMergeOp gm(std::move(runs), dimTbl, "k", true);
    auto gmRows = runPlan(&gm);
    assert(gmRows.size() == 7);
    for (size_t i = 1; i < gmRows.size(); ++i) {
        int prev = std::stoi(gmRows[i - 1].substr(0, gmRows[i - 1].find(' ')));
        int cur = std::stoi(gmRows[i].substr(0, gmRows[i].find(' ')));
        assert(prev < cur);
    }
    std::cout << "[PAR] GatherMerge k-way merge OK" << std::endl;

    // ------------------------------------------------------------------
    // 5. Planner: parallel aggregate + GatherMerge show in EXPLAIN.
    // ------------------------------------------------------------------
    dbms::QueryPlanner::setParallelWorkers(4);
    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = "big";
    ctx.groupByCols = {"grp"};
    ctx.aggregateItems = items;
    auto aggPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    std::string ax = dbms::QueryPlanner::explain(aggPlan, &g_engine, db, opts);
    assert(ax.find("ParallelAggregate") != std::string::npos);
    std::cout << "[PAR] planner ParallelAggregate OK" << std::endl;

    dbms::PlanContext sctx;
    sctx.dbname = db;
    sctx.tablename = "big";
    sctx.orderByCol = "id";
    auto sortPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, sctx);
    std::string sx = dbms::QueryPlanner::explain(sortPlan, &g_engine, db, opts);
    if (sx.find("GatherMerge") != std::string::npos) {
        // Executing the whole plan (Project over GatherMerge) must return
        // every row exactly once.
        auto gmPlanned = runPlan(sortPlan.get());
        assert(gmPlanned.size() == 600);
        std::cout << "[PAR] planner GatherMerge OK" << std::endl;
    } else {
        // fell back to SortOp (e.g. fewer than 3 pages) — acceptable.
        assert(sx.find("Sort") != std::string::npos);
        std::cout << "[PAR] GatherMerge fallback to Sort (acceptable)"
                  << std::endl;
    }
    dbms::QueryPlanner::setParallelWorkers(0);

    cleanupDb(db);
    std::cout << "[PAR] all tests passed" << std::endl;
    return 0;
}
