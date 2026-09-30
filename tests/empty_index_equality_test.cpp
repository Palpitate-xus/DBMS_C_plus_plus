#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("empty_index_equality");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 2, true));
    table.append(dbms::makeTextColumn("k", true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, "items", {{"id", "1"}, {"k", ""}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, "items", {{"id", "2"}, {"k", std::nullopt}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, "items", {{"id", "3"}, {"k", "x"}}) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(db, "items", "k") == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, "items", {{"id", "4"}, {"k", ""}}) == dbms::DBStatus::OK);
    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = "items";
    ctx.selectCols = {"id", "k"};
    ctx.orderByCol = "id";
    ctx.conds = {{"=", "k", ""}};
    auto result = dbms::QueryPlanner::executePlanChecked(dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
    assert(result.ok && result.rows.size() == 2);
    assert(result.structuredRowsAvailable);
    assert((result.structuredRows == std::vector<std::vector<std::string>>{{"1", ""}, {"4", ""}}));
    assert((result.structuredNulls == std::vector<std::vector<bool>>{{false, false}, {false, false}}));
    result = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(&g_engine, db, "items", "k", ""));
    assert(result.ok && result.rows.size() == 2);
    ctx.conds.push_back({"=", "id", "1"});
    result = dbms::QueryPlanner::executePlanChecked(dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
    assert(result.ok && result.rows.size() == 1);
    assert((result.structuredRows == std::vector<std::vector<std::string>>{{"1", ""}}));
    auto disjunction = dbms::QueryPlanner::buildDisjunctiveSelectPlan(
        &g_engine, ctx, {{{"=", "k", ""}}, {{"=", "id", "3"}}});
    assert(!disjunction); // Empty-key branch cannot be narrowed by the incomplete index.
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[EMPTY INDEX EQUALITY] passed\n";
}
