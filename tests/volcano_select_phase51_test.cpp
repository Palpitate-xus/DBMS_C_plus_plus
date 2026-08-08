// Phase 5.1 test: verify SELECT execution produces correct results through
// the volcano operator tree (QueryPlanner::buildSelectPlan + executePlan).
//
// Strategy: populate a table, then build a PlanContext and execute via the
// operator tree. Compare the row count and sort order with the same query
// issued through g_engine.query() (the legacy string-based path). They must
// match exactly for the volcano path to be considered equivalent.

#include "executor/ExecutionPlan.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <set>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (fs::exists(db)) fs::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

static void insertRow(const std::string& db, const std::string& tname,
                      const std::map<std::string, std::string>& vals) {
    assert(g_engine.insert(db, tname, vals) == dbms::DBStatus::OK);
}

// Build a condition string in the format expected by parseConditions:
// operator-first (e.g. "=age 30"), which is the modifyViewLogic output format.
static std::string makeCond(const std::string& col, const std::string& op,
                            const std::string& val) {
    return op + col + " " + val;
}

// -------- Test 1: full table scan, no WHERE --------
static void test_full_scan() {
    std::string db = testDbPath("volc_full");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));

    insertRow(db, "t", {{"id", "1"}, {"name", "alice"}});
    insertRow(db, "t", {{"id", "2"}, {"name", "bob"}});
    insertRow(db, "t", {{"id", "3"}, {"name", "carol"}});

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t";

    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert(rows.size() == 3);

    // Compare with legacy path
    auto legacy = g_engine.query(db, "t", {}, {});
    assert(legacy.size() == rows.size());

    cleanup(db);
    std::cout << "[VOLCANO-5.1] full scan OK (" << rows.size() << " rows)" << std::endl;
}

// -------- Test 2: WHERE + equal index scan on secondary index --------
static void test_where_index_scan() {
    std::string db = testDbPath("volc_idx");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20), age INT)", s));
    assert(!ddl.executeSql("CREATE INDEX ON t(age)", s));

    insertRow(db, "t", {{"id", "1"}, {"name", "alice"}, {"age", "30"}});
    insertRow(db, "t", {{"id", "2"}, {"name", "bob"},   {"age", "25"}});
    insertRow(db, "t", {{"id", "3"}, {"name", "carol"}, {"age", "30"}});

    std::vector<std::string> condStrs = { makeCond("age", "=", "30") };
    auto conds = dbms::StorageEngine::parseConditions(condStrs);
    assert(conds.size() == 1);

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t"; ctx.conds = conds;

    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert(rows.size() == 2);  // alice + carol

    cleanup(db);
    std::cout << "[VOLCANO-5.1] index scan OK (" << rows.size() << " rows)" << std::endl;
}

// -------- Test 3: ORDER BY + LIMIT --------
static void test_order_by_limit() {
    std::string db = testDbPath("volc_ord");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));

    insertRow(db, "t", {{"id", "3"}, {"name", "carol"}});
    insertRow(db, "t", {{"id", "1"}, {"name", "alice"}});
    insertRow(db, "t", {{"id", "2"}, {"name", "bob"}});

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t";
    ctx.orderByCol = "id"; ctx.orderByAsc = true; ctx.limit = 2;

    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert(rows.size() == 2);

    // Verify ascending order (parse first column from each row).
    auto extractId = [](const std::string& r) -> int {
        std::stringstream ss(r); int v = -1; ss >> v; return v;
    };
    assert(extractId(rows[0]) == 1);
    assert(extractId(rows[1]) == 2);

    cleanup(db);
    std::cout << "[VOLCANO-5.1] ORDER BY + LIMIT OK" << std::endl;
}

// -------- Test 4: DISTINCT --------
static void test_distinct() {
    std::string db = testDbPath("volc_dist");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));

    insertRow(db, "t", {{"id", "1"}, {"name", "alice"}});
    insertRow(db, "t", {{"id", "1"}, {"name", "alice"}});
    insertRow(db, "t", {{"id", "2"}, {"name", "bob"}});

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t"; ctx.distinct = true;

    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert(rows.size() == 2);  // distinct removes the duplicate

    cleanup(db);
    std::cout << "[VOLCANO-5.1] DISTINCT OK" << std::endl;
}

// -------- Test 5: projection (subset of columns) --------
static void test_projection() {
    std::string db = testDbPath("volc_proj");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20), age INT)", s));

    insertRow(db, "t", {{"id", "1"}, {"name", "alice"}, {"age", "30"}});
    insertRow(db, "t", {{"id", "2"}, {"name", "bob"},   {"age", "25"}});

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t";
    ctx.selectCols = {"id", "name"};

    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert(rows.size() == 2);
    // Each row should have only id and name (space-separated).
    for (const auto& r : rows) {
        std::stringstream ss(r); std::string a, b, c;
        ss >> a >> b >> c;
        assert(c.empty());  // no third column
    }

    cleanup(db);
    std::cout << "[VOLCANO-5.1] projection OK" << std::endl;
}

// -------- Test 6: LIKE filter (uses FilterOp, no index) --------
static void test_like_filter() {
    std::string db = testDbPath("volc_like");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));

    insertRow(db, "t", {{"id", "1"}, {"name", "alice"}});
    insertRow(db, "t", {{"id", "2"}, {"name", "bob"}});
    insertRow(db, "t", {{"id", "3"}, {"name", "alexa"}});

    // LIKE condition format expected by parseConditions: "like" + colName + " " + value.
    std::vector<std::string> condStrs = { "likename alice" };
    auto conds = dbms::StorageEngine::parseConditions(condStrs);
    assert(conds.size() == 1);

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t"; ctx.conds = conds;

    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert(rows.size() == 1);

    cleanup(db);
    std::cout << "[VOLCANO-5.1] LIKE filter OK" << std::endl;
}

// -------- Test 7: bitmap candidate intersection + heap recheck --------
static void test_bitmap_and() {
    std::string db = testDbPath("volc_bitmap");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, tenant INT, state INT)", s));
    assert(!ddl.executeSql("CREATE INDEX ON t(tenant)", s));
    assert(g_engine.createHashIndex(db, "t", "state") == dbms::DBStatus::OK);

    insertRow(db, "t", {{"id", "1"}, {"tenant", "7"}, {"state", "1"}});
    insertRow(db, "t", {{"id", "2"}, {"tenant", "7"}, {"state", "2"}});
    insertRow(db, "t", {{"id", "3"}, {"tenant", "8"}, {"state", "1"}});

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t";
    ctx.selectCols = {"id"};
    ctx.conds = dbms::StorageEngine::parseConditions({"=tenant 7", "=state 1"});
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto* project = dynamic_cast<dbms::ProjectOp*>(plan.get());
    assert(project);
    auto* filter = dynamic_cast<dbms::FilterOp*>(project->child());
    assert(filter);
    assert(dynamic_cast<dbms::BitmapHeapScanOp*>(filter->child()) != nullptr);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert(rows.size() == 1);
    assert(rows[0].find("1") != std::string::npos);

    cleanup(db);
    std::cout << "[VOLCANO-5.1] bitmap AND + heap recheck OK" << std::endl;
}

// -------- Test 8: bitmap candidate union + disjunctive recheck --------
static void test_bitmap_or() {
    std::string db = testDbPath("volc_bitmap_or");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, tenant INT, state INT)", s));
    assert(!ddl.executeSql("CREATE INDEX ON t(tenant)", s));
    assert(!ddl.executeSql("CREATE INDEX ON t(state)", s));

    insertRow(db, "t", {{"id", "1"}, {"tenant", "7"}, {"state", "1"}});
    insertRow(db, "t", {{"id", "2"}, {"tenant", "7"}, {"state", "2"}});
    insertRow(db, "t", {{"id", "3"}, {"tenant", "8"}, {"state", "1"}});

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t"; ctx.selectCols = {"id"};
    std::vector<std::vector<dbms::StorageEngine::Condition>> branches = {
        dbms::StorageEngine::parseConditions({"=tenant 7"}),
        dbms::StorageEngine::parseConditions({"=state 1"})
    };
    auto plan = dbms::QueryPlanner::buildDisjunctiveSelectPlan(&g_engine, ctx, branches);
    auto* project = dynamic_cast<dbms::ProjectOp*>(plan.get());
    assert(project);
    assert(dynamic_cast<dbms::BitmapOrHeapScanOp*>(project->child()) != nullptr);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert(rows.size() == 3);

    cleanup(db);
    std::cout << "[VOLCANO-5.1] bitmap OR + disjunctive recheck OK" << std::endl;
}

// -------- Test 9: parallel heap page ranges + deterministic gather --------
static void test_parallel_scan() {
    std::string db = testDbPath("volc_parallel_scan");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, payload VARCHAR(32))", s));
    for (int i = 0; i < 300; ++i) {
        insertRow(db, "t", {{"id", std::to_string(i)}, {"payload", "v" + std::to_string(i)}});
    }

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t";
    dbms::QueryPlanner::setParallelWorkers(3);
    auto parallelPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto* project = dynamic_cast<dbms::ProjectOp*>(parallelPlan.get());
    assert(project);
    auto* scan = dynamic_cast<dbms::ParallelTableScanOp*>(project->child());
    assert(scan);
    assert(parallelPlan->open());
    assert(scan->usedParallelWorkers());
    std::vector<std::string> parallelRows;
    std::string row;
    while (parallelPlan->next(row)) parallelRows.push_back(row);
    parallelPlan->close();
    parallelPlan.reset();

    dbms::QueryPlanner::setParallelWorkers(0);
    auto sequentialPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto sequentialRows = dbms::QueryPlanner::executePlan(std::move(sequentialPlan));
    assert(parallelRows == sequentialRows);

    // Transaction-local visibility/SSI state must stay on the backend
    // thread; the parallel node therefore falls back safely in a transaction.
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    dbms::QueryPlanner::setParallelWorkers(3);
    auto transactionalPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto* transactionalProject = dynamic_cast<dbms::ProjectOp*>(transactionalPlan.get());
    assert(transactionalProject);
    auto* transactionalScan = dynamic_cast<dbms::ParallelTableScanOp*>(transactionalProject->child());
    assert(transactionalScan);
    assert(transactionalPlan->open());
    assert(!transactionalScan->usedParallelWorkers());
    transactionalPlan->close();
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    dbms::QueryPlanner::setParallelWorkers(0);

    cleanup(db);
    std::cout << "[VOLCANO-5.1] parallel page scan + deterministic gather OK" << std::endl;
}

// -------- Test 10: structured WindowAgg ranking and offsets --------
static void test_window_agg() {
    std::string db = testDbPath("volc_window");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, dept VARCHAR(20), score INT)", s));

    // Insert a tie in each partition.  The final id sort makes the expected
    // output deterministic while each window keeps its own score ordering.
    insertRow(db, "t", {{"id", "3"}, {"dept", "A"}, {"score", "20"}});
    insertRow(db, "t", {{"id", "1"}, {"dept", "A"}, {"score", "10"}});
    insertRow(db, "t", {{"id", "2"}, {"dept", "A"}, {"score", "20"}});
    insertRow(db, "t", {{"id", "4"}, {"dept", "B"}, {"score", "10"}});
    insertRow(db, "t", {{"id", "5"}, {"dept", "B"}, {"score", "20"}});

    dbms::WindowFunctionSpec rowNumber;
    rowNumber.name = "row_number";
    rowNumber.partitionBy = {"dept"};
    rowNumber.orderBy = "score";
    dbms::WindowFunctionSpec rank = rowNumber;
    rank.name = "rank";
    dbms::WindowFunctionSpec denseRank = rowNumber;
    denseRank.name = "dense_rank";
    dbms::WindowFunctionSpec lag = rowNumber;
    lag.name = "lag";
    lag.argument = "score";

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t";
    ctx.orderByCol = "id"; ctx.orderByAsc = true;
    ctx.windowFunctions = {rowNumber, rank, denseRank, lag};
    ctx.windowTargets = {
        {false, "id", 0}, {true, "", 0}, {true, "", 1},
        {true, "", 2}, {true, "", 3}
    };

    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    auto* window = dynamic_cast<dbms::WindowOp*>(plan.get());
    assert(window);
    assert(window->functions().size() == 4);
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    const std::vector<std::string> expected = {
        "1 1 1 1 NULL", "2 3 2 2 20", "3 2 2 2 10",
        "4 1 1 1 NULL", "5 2 2 2 10"
    };
    assert(rows == expected);

    dbms::WindowFunctionSpec defaultSum;
    defaultSum.name = "sum";
    defaultSum.argument = "score";
    defaultSum.partitionBy = {"dept"};
    defaultSum.orderBy = "score";
    dbms::PlanContext defaultFrameCtx = ctx;
    defaultFrameCtx.windowFunctions = {defaultSum};
    defaultFrameCtx.windowTargets = {{false, "id", 0}, {true, "", 0}};
    auto defaultFrameRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, defaultFrameCtx));
    assert((defaultFrameRows == std::vector<std::string>{
        "1 10", "2 50", "3 50", "4 10", "5 30"
    }));

    dbms::WindowFunctionSpec excludedSum = defaultSum;
    excludedSum.hasFrame = true;
    excludedSum.frameStartOffset = -1;
    excludedSum.frameEndOffset = -1;
    excludedSum.frameExclusion = "current row";
    dbms::PlanContext excludedFrameCtx = defaultFrameCtx;
    excludedFrameCtx.windowFunctions = {excludedSum};
    auto excludedFrameRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, excludedFrameCtx));
    assert((excludedFrameRows == std::vector<std::string>{
        "1 40", "2 30", "3 30", "4 20", "5 10"
    }));

    dbms::WindowFunctionSpec countRows;
    countRows.name = "count";
    countRows.argument = "*";
    countRows.partitionBy = {"dept"};
    countRows.orderBy = "score";
    dbms::WindowFunctionSpec firstValue = defaultSum;
    firstValue.name = "first_value";
    dbms::WindowFunctionSpec lastValue = defaultSum;
    lastValue.name = "last_value";
    dbms::WindowFunctionSpec ntile = defaultSum;
    ntile.name = "ntile";
    ntile.argument = "2";
    dbms::WindowFunctionSpec percentRank = defaultSum;
    percentRank.name = "percent_rank";
    percentRank.argument.clear();
    dbms::WindowFunctionSpec cumeDist = percentRank;
    cumeDist.name = "cume_dist";
    dbms::PlanContext analyticCtx = ctx;
    analyticCtx.windowFunctions = {
        countRows, firstValue, lastValue, ntile, percentRank, cumeDist
    };
    analyticCtx.windowTargets = {
        {false, "id", 0}, {true, "", 0}, {true, "", 1},
        {true, "", 2}, {true, "", 3}, {true, "", 4}, {true, "", 5}
    };
    auto analyticRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, analyticCtx));
    assert((analyticRows == std::vector<std::string>{
        "1 1 10 10 1 0.0000 0.3333",
        "2 3 10 20 2 0.5000 1.0000",
        "3 3 10 20 1 0.5000 1.0000",
        "4 1 10 10 1 0.0000 0.5000",
        "5 2 10 20 2 1.0000 1.0000"
    }));

    auto explainPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    const auto explain = dbms::QueryPlanner::explain(explainPlan, &g_engine, db);
    assert(explain.find("WindowAgg(functions=4)") != std::string::npos);
    const auto explainJson = dbms::QueryPlanner::explainJson(explainPlan, &g_engine, db);
    assert(explainJson.find("\"nodeType\":\"WindowAgg\"") != std::string::npos);

    cleanup(db);
    std::cout << "[VOLCANO-5.1] WindowAgg ranking + lag OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_full_scan();
    test_where_index_scan();
    test_order_by_limit();
    test_distinct();
    test_projection();
    test_like_filter();
    test_bitmap_and();
    test_bitmap_or();
    test_parallel_scan();
    test_window_agg();
    std::cout << "[VOLCANO-5.1] all Phase 5.1 volcano SELECT tests passed" << std::endl;
    return 0;
}
