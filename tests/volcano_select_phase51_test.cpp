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

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_full_scan();
    test_where_index_scan();
    test_order_by_limit();
    test_distinct();
    test_projection();
    test_like_filter();
    std::cout << "[VOLCANO-5.1] all Phase 5.1 volcano SELECT tests passed" << std::endl;
    return 0;
}
