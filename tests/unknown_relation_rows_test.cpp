#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "process/RuntimeStats.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void expectRows(dbms::OpPtr& plan, const std::string& db, size_t rows) {
    const auto text = dbms::QueryPlanner::explain(plan, &g_engine, db);
    if (text.find("rows=" + std::to_string(rows)) == std::string::npos) std::cerr << text;
    assert(text.find("rows=" + std::to_string(rows)) != std::string::npos);
    dbms::QueryPlanner::ExplainOptions options;
    const auto json = dbms::QueryPlanner::explainJson(plan, &g_engine, db, options);
    assert(json.find("\"rows\":" + std::to_string(rows)) != std::string::npos);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("unknown_relation_rows");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("v", false, 4));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const std::string id : {"1", "2", "3"})
        assert(g_engine.insert(db, "items", {{"id", id}, {"v", "7"}}) == dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(db, "items", "v") == dbms::DBStatus::OK);
    dbms::resetRuntimeStats();
    assert(g_engine.getTableRowCount(db, "items") == 0); // legacy absent-count API
    dbms::PlanContext context;
    context.dbname = db;
    context.tablename = "items";
    auto scan = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    expectRows(scan, db, 1000); // documented unknown, not a proven empty table
    dbms::OpPtr bitmap = std::make_unique<dbms::BitmapHeapScanOp>(
        &g_engine, db, "items", std::vector<dbms::StorageEngine::Condition>{{"=", "v", "7"}});
    expectRows(bitmap, db, 1000);
    assert(g_engine.analyzeTable(db, "items"));
    expectRows(scan, db, 3);
    expectRows(bitmap, db, 3);
    table.tablename = "empty";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    context.tablename = "empty";
    auto empty = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    expectRows(empty, db, 1000);
    assert(g_engine.analyzeTable(db, "empty"));
    expectRows(empty, db, 0);
    dbms::recordTableScan(db, "items", 2, false, true);
    expectRows(scan, db, 2); // exact process-local evidence remains preferred
    assert(g_engine.dropTable(db, "items") == dbms::DBStatus::OK);
    table.tablename = "items";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    context.tablename = "items";
    scan = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    expectRows(scan, db, 1000); // neither old runtime nor old ANALYZE survives
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    dbms::resetRuntimeStats();
    std::cout << "[UNKNOWN RELATION ROWS] passed\n";
}
