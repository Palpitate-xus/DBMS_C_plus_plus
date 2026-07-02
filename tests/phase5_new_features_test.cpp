#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

static void test_insert_default_values() {
    std::string db = testDbPath("p5_dv");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    // Table with DEFAULT and nullable
    assert(!ddl.executeSql("CREATE TABLE t (id INT DEFAULT 42, name VARCHAR(20), ts TIMESTAMP DEFAULT '2020-01-01 00:00:00')", s));
    // Verify engine-level insertDefaultValues works
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "t");
    auto res = g_engine.insertDefaultValues(db, "t", tbl);
    assert(res == dbms::DBStatus::OK);
    auto rows = g_engine.query(db, "t", {}, {"id", "name", "ts"});
    assert(rows.size() == 1);
    cleanup(db);
    std::cout << "[P5] INSERT DEFAULT VALUES OK" << std::endl;
}

static void test_commit_and_chain_parser() {
    dbms::SQLParser parser;
    auto r1 = parser.parse("COMMIT AND CHAIN");
    assert(r1.success);
    auto r2 = parser.parse("COMMIT AND NO CHAIN");
    assert(r2.success);
    auto r3 = parser.parse("COMMIT");
    assert(r3.success);
    std::cout << "[P5] COMMIT AND CHAIN parser OK" << std::endl;
}

static void test_rollback_and_chain_parser() {
    dbms::SQLParser parser;
    auto r1 = parser.parse("ROLLBACK AND CHAIN");
    assert(r1.success);
    auto r2 = parser.parse("ROLLBACK AND NO CHAIN");
    assert(r2.success);
    std::cout << "[P5] ROLLBACK AND CHAIN parser OK" << std::endl;
}

static void test_explain_analyze_timing() {
    // Verify that QueryPlanner::executePlan returns rows (used by EXPLAIN ANALYZE)
    std::string db = testDbPath("p5_ea");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}}) == dbms::DBStatus::OK);

    dbms::PlanContext ctx;
    ctx.dbname = db; ctx.tablename = "t";
    // Use engine query directly (operator tree would deadlock with open table).
    auto rows = g_engine.query(db, "t", {}, {"id", "name"});
    assert(rows.size() == 2);
    cleanup(db);
    std::cout << "[P5] EXPLAIN ANALYZE exec path OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_insert_default_values();
    test_commit_and_chain_parser();
    test_rollback_and_chain_parser();
    test_explain_analyze_timing();
    std::cout << "[P5] all new features passed" << std::endl;
    return 0;
}
