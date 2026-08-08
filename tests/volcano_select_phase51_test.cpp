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

    ctx.offset = 1;
    ctx.limit = 2;
    auto offsetPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    assert(dynamic_cast<dbms::LimitOp*>(offsetPlan.get()));
    auto* offset = dynamic_cast<dbms::OffsetOp*>(
        dynamic_cast<dbms::LimitOp*>(offsetPlan.get())->child());
    assert(offset && offset->offset() == 1);
    auto offsetRows = dbms::QueryPlanner::executePlan(std::move(offsetPlan));
    assert(offsetRows.size() == 2);
    assert(extractId(offsetRows[0]) == 2);
    assert(extractId(offsetRows[1]) == 3);

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

    dbms::WindowFunctionSpec rangeSum = defaultSum;
    rangeSum.hasFrame = true;
    rangeSum.frameType = dbms::WindowFunctionSpec::FrameType::RANGE;
    rangeSum.frameStartOffset = 10;
    rangeSum.frameEndOffset = 0;
    dbms::PlanContext rangeCtx = defaultFrameCtx;
    rangeCtx.windowFunctions = {rangeSum};
    auto rangeRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, rangeCtx));
    assert((rangeRows == std::vector<std::string>{
        "1 10", "2 50", "3 50", "4 10", "5 30"
    }));

    dbms::WindowFunctionSpec groupsSum = defaultSum;
    groupsSum.hasFrame = true;
    groupsSum.frameType = dbms::WindowFunctionSpec::FrameType::GROUPS;
    groupsSum.frameStartOffset = 1;
    groupsSum.frameEndOffset = 0;
    dbms::PlanContext groupsCtx = defaultFrameCtx;
    groupsCtx.windowFunctions = {groupsSum};
    auto groupsRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, groupsCtx));
    assert((groupsRows == std::vector<std::string>{
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

// -------- Test 11: structured GroupAggregate and grouping sets --------
static void test_group_aggregate() {
    std::string db = testDbPath("volc_group");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (dept VARCHAR(20), team VARCHAR(20), score INT)", s));

    insertRow(db, "t", {{"dept", "A"}, {"team", "X"}, {"score", "10"}});
    insertRow(db, "t", {{"dept", "A"}, {"team", "X"}, {"score", "20"}});
    insertRow(db, "t", {{"dept", "A"}, {"team", "Y"}, {"score", "5"}});
    insertRow(db, "t", {{"dept", "B"}, {"team", "X"}, {"score", "7"}});

    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = "t";
    ctx.groupByCols = {"dept"};
    ctx.aggregateItems = {
        {"count", "*", {}}, {"sum", "score", {}}, {"avg", "score", {}},
        {"min", "score", {}}, {"max", "score", {}}
    };
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    assert(dynamic_cast<dbms::GroupAggregateOp*>(plan.get()));
    auto rows = dbms::QueryPlanner::executePlan(std::move(plan));
    assert((rows == std::vector<std::string>{
        "A 3 35 11.666667 5 20", "B 1 7 7.000000 7 7"
    }));

    dbms::PlanContext plainCtx;
    plainCtx.dbname = db;
    plainCtx.tablename = "t";
    plainCtx.conds = dbms::StorageEngine::parseConditions({"=dept A"});
    plainCtx.aggregateItems = {{"count", "*", {}}, {"sum", "score", {}}};
    auto plainPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, plainCtx);
    assert(dynamic_cast<dbms::GroupAggregateOp*>(plainPlan.get()));
    auto plainRows = dbms::QueryPlanner::executePlan(std::move(plainPlan));
    assert((plainRows == std::vector<std::string>{"3 35"}));

    dbms::PlanContext havingCtx = ctx;
    havingCtx.aggregateItems = {{"count", "*", {}}};
    havingCtx.havingConds = {"count(*) > 1"};
    auto havingRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, havingCtx));
    assert((havingRows == std::vector<std::string>{"A 3"}));

    dbms::PlanContext setsCtx = ctx;
    setsCtx.groupByCols = {"dept", "team"};
    setsCtx.groupingSets = {{"dept", "team"}, {"dept"}, {}};
    setsCtx.aggregateItems = {{"count", "*", {}}};
    auto setsPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, setsCtx);
    auto* group = dynamic_cast<dbms::GroupAggregateOp*>(setsPlan.get());
    assert(group && group->groupingSetCount() == 3);
    auto setsRows = dbms::QueryPlanner::executePlan(std::move(setsPlan));
    assert((setsRows == std::vector<std::string>{
        "A X 2", "A Y 1", "B X 1", "A NULL 3", "B NULL 1", "NULL NULL 4"
    }));

    auto explainPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    const auto explain = dbms::QueryPlanner::explain(explainPlan, &g_engine, db);
    assert(explain.find("GroupAggregate(grouping_sets=1)") != std::string::npos);
    const auto explainJson = dbms::QueryPlanner::explainJson(explainPlan, &g_engine, db);
    assert(explainJson.find("\"nodeType\":\"GroupAggregate\"") != std::string::npos);

    cleanup(db);
    std::cout << "[VOLCANO-5.1] GroupAggregate + grouping sets OK" << std::endl;
}

// -------- Test 11: structured uncorrelated IN / NOT IN --------
static void test_semi_and_anti_join() {
    std::string db = testDbPath("volc_semi");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE outer_t (id INT, payload INT)", s));
    assert(!ddl.executeSql("CREATE TABLE inner_t (id INT, enabled INT)", s));
    assert(!ddl.executeSql("CREATE TABLE clean_inner_t (id INT NOT NULL)", s));

    insertRow(db, "outer_t", {{"id", "1"}, {"payload", "10"}});
    insertRow(db, "outer_t", {{"id", "2"}, {"payload", "20"}});
    insertRow(db, "outer_t", {{"id", "3"}, {"payload", "30"}});
    insertRow(db, "outer_t", {{"id", "4"}, {"payload", "40"}});
    insertRow(db, "inner_t", {{"id", "2"}, {"enabled", "1"}});
    insertRow(db, "inner_t", {{"id", "3"}, {"enabled", "1"}});
    // Omitting a nullable column creates a SQL NULL key.
    insertRow(db, "inner_t", {{"enabled", "1"}});
    insertRow(db, "clean_inner_t", {{"id", "2"}});
    insertRow(db, "clean_inner_t", {{"id", "3"}});

    dbms::PlanContext inCtx;
    inCtx.dbname = db;
    inCtx.tablename = "outer_t";
    inCtx.selectCols = {"id"};
    inCtx.semiJoins.push_back({db, "inner_t", "id", "id",
                               dbms::StorageEngine::parseConditions({"=enabled 1"}), false});
    auto inPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, inCtx);
    auto* inProject = dynamic_cast<dbms::ProjectOp*>(inPlan.get());
    assert(inProject && dynamic_cast<dbms::SemiJoinOp*>(inProject->child()));
    auto inRows = dbms::QueryPlanner::executePlan(std::move(inPlan));
    assert((inRows == std::vector<std::string>{"2 ", "3 "}));

    dbms::PlanContext antiCtx = inCtx;
    antiCtx.semiJoins = {{db, "clean_inner_t", "id", "id", {}, true}};
    auto antiPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, antiCtx);
    auto* antiProject = dynamic_cast<dbms::ProjectOp*>(antiPlan.get());
    assert(antiProject);
    auto* anti = dynamic_cast<dbms::SemiJoinOp*>(antiProject->child());
    assert(anti && anti->isAnti());
    auto antiRows = dbms::QueryPlanner::executePlan(std::move(antiPlan));
    assert((antiRows == std::vector<std::string>{"1 ", "4 "}));

    dbms::PlanContext nullAntiCtx = inCtx;
    nullAntiCtx.semiJoins = {{db, "inner_t", "id", "id",
                              dbms::StorageEngine::parseConditions({"=enabled 1"}), true}};
    auto nullAntiRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, nullAntiCtx));
    assert(nullAntiRows.empty());

    dbms::PlanContext existsCtx;
    existsCtx.dbname = db;
    existsCtx.tablename = "outer_t";
    existsCtx.selectCols = {"id"};
    existsCtx.existenceFilters.push_back({
        db, "inner_t", dbms::StorageEngine::parseConditions({"=enabled 1"}), false});
    auto existsPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, existsCtx);
    auto* existsProject = dynamic_cast<dbms::ProjectOp*>(existsPlan.get());
    assert(existsProject);
    auto* exists = dynamic_cast<dbms::ExistenceFilterOp*>(existsProject->child());
    assert(exists && !exists->isAnti());
    auto existsRows = dbms::QueryPlanner::executePlan(std::move(existsPlan));
    assert((existsRows == std::vector<std::string>{"1 ", "2 ", "3 ", "4 "}));

    dbms::PlanContext notExistsCtx = existsCtx;
    notExistsCtx.existenceFilters = {{
        db, "inner_t", dbms::StorageEngine::parseConditions({"=enabled 9"}), true}};
    auto notExistsPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, notExistsCtx);
    auto* notExistsProject = dynamic_cast<dbms::ProjectOp*>(notExistsPlan.get());
    assert(notExistsProject);
    auto* notExists = dynamic_cast<dbms::ExistenceFilterOp*>(notExistsProject->child());
    assert(notExists && notExists->isAnti());
    auto notExistsRows = dbms::QueryPlanner::executePlan(std::move(notExistsPlan));
    assert((notExistsRows == std::vector<std::string>{"1 ", "2 ", "3 ", "4 "}));

    auto explainPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, existsCtx);
    const auto explain = dbms::QueryPlanner::explain(explainPlan, &g_engine, db);
    assert(explain.find("ExistenceFilter") != std::string::npos);
    const auto explainJson = dbms::QueryPlanner::explainJson(explainPlan, &g_engine, db);
    assert(explainJson.find("\"nodeType\":\"ExistenceFilter\"") != std::string::npos);

    dbms::PlanContext scalarCtx;
    scalarCtx.dbname = db;
    scalarCtx.tablename = "outer_t";
    scalarCtx.selectCols = {"id"};
    scalarCtx.projectionTargets = {{false, "id"}, {true, {}}};
    scalarCtx.scalarSubquery = {
        db, "inner_t", "id", dbms::StorageEngine::parseConditions({"=id 2"})};
    auto scalarPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, scalarCtx);
    auto* scalar = dynamic_cast<dbms::ScalarSubqueryProjectOp*>(scalarPlan.get());
    assert(scalar);
    auto scalarRows = dbms::QueryPlanner::executePlan(std::move(scalarPlan));
    assert((scalarRows == std::vector<std::string>{
        "1 2 ", "2 2 ", "3 2 ", "4 2 "}));

    scalarCtx.scalarSubquery.innerConds =
        dbms::StorageEngine::parseConditions({"=id 9"});
    auto nullScalarRows = dbms::QueryPlanner::executePlan(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, scalarCtx));
    assert((nullScalarRows == std::vector<std::string>{
        "1 NULL ", "2 NULL ", "3 NULL ", "4 NULL "}));

    scalarCtx.scalarSubquery.innerConds =
        dbms::StorageEngine::parseConditions({"=enabled 1"});
    auto multiScalarPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, scalarCtx);
    auto* multiScalar = dynamic_cast<dbms::ScalarSubqueryProjectOp*>(multiScalarPlan.get());
    assert(multiScalar && !multiScalar->open());
    assert(multiScalar->errorMessage().find("more than one row") != std::string::npos);

    cleanup(db);
    std::cout << "[VOLCANO-5.1] SemiJoin/AntiJoin and ExistenceFilter semantics OK" << std::endl;
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
    test_group_aggregate();
    test_semi_and_anti_join();
    std::cout << "[VOLCANO-5.1] all Phase 5.1 volcano SELECT tests passed" << std::endl;
    return 0;
}
