// ============================================================================
// stats_planner_test — statistics-driven selectivity and join costing:
//   ANALYZE feeds MCV-based equality selectivity (hot values get exact
//   frequencies, not uniform 1/N guesses)
//   histogram-based range selectivity (skewed data shifts '< x' estimates)
//   equality-join selectivity 1/max(nd_l, nd_r) in EXPLAIN join rows and
//   buildJoinPlan algorithm choice
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

static int parseRows(const std::string& explain, const std::string& marker) {
    size_t p = explain.find(marker);
    if (p == std::string::npos) return -1;
    size_t r = explain.find("rows=", p);
    if (r == std::string::npos) return -1;
    try {
        return std::stoi(explain.substr(r + 5));
    } catch (...) {
        return -1;
    }
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    const std::string db = testDbPath("stats_planner");
    cleanupDb(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // ------------------------------------------------------------------
    // 1. MCV equality selectivity: 90 rows share 'hot', 10 spread others.
    // ------------------------------------------------------------------
    assert(!ddl.executeSql("CREATE TABLE m (id INT, tag VARCHAR(10))", s));
    for (int i = 0; i < 100; ++i) {
        std::string tag = (i < 90) ? "hot" : ("c" + std::to_string(i));
        assert(g_engine.insert(db, "m", {{"id", std::to_string(i)}, {"tag", tag}})
                   == dbms::DBStatus::OK);
    }
    assert(g_engine.analyzeTable(db, "m"));

    auto stats = g_engine.getColumnStats(db, "m", "tag");
    assert(stats.cardinality >= 11);          // 'hot' + 10 distinct others
    assert(!stats.mcv.empty());
    assert(stats.mcv[0].first == "hot");      // frequency-ordered
    assert(stats.mcv[0].second == 90);

    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = "m";
    ctx.conds = {{"=", "tag", "hot"}};
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    std::string explain = dbms::QueryPlanner::explain(plan, &g_engine, db);
    int hotRows = parseRows(explain, "Filter");
    // MCV: 90/100 rows -> estimate must be far above the uniform 1/N = 1.
    assert(hotRows >= 45 && hotRows <= 90);

    // A cold (non-MCV) value falls back to 1/ndistinct = 1/11 (~9 of 100).
    ctx.conds = {{"=", "tag", "c95"}};
    plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    explain = dbms::QueryPlanner::explain(plan, &g_engine, db);
    int coldRows = parseRows(explain, "Filter");
    assert(coldRows >= 1 && coldRows <= 2);   // exact MCV count is 1

    std::cout << "[STATS-PLAN] MCV equality selectivity OK (hot=" << hotRows
              << " cold=" << coldRows << ")" << std::endl;

    // ------------------------------------------------------------------
    // 2. Histogram range selectivity: values clustered in [0, 10).
    // ------------------------------------------------------------------
    assert(!ddl.executeSql("CREATE TABLE h (id INT, v INT)", s));
    for (int i = 0; i < 100; ++i) {
        int v = i % 10;
        assert(g_engine.insert(db, "h", {{"id", std::to_string(i)},
                                         {"v", std::to_string(v)}})
                   == dbms::DBStatus::OK);
    }
    assert(g_engine.analyzeTable(db, "h"));
    ctx.tablename = "h";
    ctx.conds = {{"<", "v", "3"}};   // true selectivity ~0.3
    plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    explain = dbms::QueryPlanner::explain(plan, &g_engine, db);
    int ltRows = parseRows(explain, "Filter");
    assert(ltRows > 0 && ltRows <= 60);   // near 30, not the flat 30% of 100

    ctx.conds = {{"<", "v", "1"}};   // ~0.1
    plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    explain = dbms::QueryPlanner::explain(plan, &g_engine, db);
    int lt1Rows = parseRows(explain, "Filter");
    assert(lt1Rows >= 0 && lt1Rows <= 30);
    assert(lt1Rows < ltRows);             // tighter range -> fewer rows

    std::cout << "[STATS-PLAN] histogram range selectivity OK (<3=" << ltRows
              << " <1=" << lt1Rows << ")" << std::endl;

    // ------------------------------------------------------------------
    // 3. Join selectivity: orders(1000 rows) x customers(100 rows) on
    //    customer_id with ndistinct 100 -> est rows ~= 1000*100/100 = 1000.
    // ------------------------------------------------------------------
    assert(!ddl.executeSql("CREATE TABLE customers (id INT, name VARCHAR(20))", s));
    assert(!ddl.executeSql("CREATE TABLE orders (id INT, cid INT, amt INT)", s));
    for (int i = 0; i < 100; ++i) {
        assert(g_engine.insert(db, "customers",
                               {{"id", std::to_string(i)}, {"name", "n"}})
                   == dbms::DBStatus::OK);
    }
    for (int i = 0; i < 1000; ++i) {
        int cid = i % 100;
        assert(g_engine.insert(db, "orders",
                               {{"id", std::to_string(i)},
                                {"cid", std::to_string(cid)},
                                {"amt", "1"}})
                   == dbms::DBStatus::OK);
    }
    assert(g_engine.analyzeTable(db, "customers"));
    assert(g_engine.analyzeTable(db, "orders"));

    auto joinPlan = dbms::QueryPlanner::buildJoinPlan(
        &g_engine, db, "orders", "customers", "cid", "id", {}, {});
    // 1000 x 100 with stats: hash join (as before), but now with a
    // stats-driven rows estimate rather than the flat 0.1 guess.
    std::string joinExplain = dbms::QueryPlanner::explain(joinPlan, &g_engine, db);
    int joinRows = -1;
    {
        size_t p = joinExplain.find("Join(");
        if (p != std::string::npos) {
            size_t r = joinExplain.find("rows=", p);
            if (r != std::string::npos) joinRows = std::stoi(joinExplain.substr(r + 5));
        }
    }
    // sel = 1/max(100, 100) -> 1000 * 100 * 0.01 = 1000 (floor/ceil wobble ok)
    assert(joinRows >= 500 && joinRows <= 2000);
    std::cout << "[STATS-PLAN] join selectivity OK (est=" << joinRows << ")"
              << std::endl;

    cleanupDb(db);
    std::cout << "[STATS-PLAN] all tests passed" << std::endl;
    return 0;
}
