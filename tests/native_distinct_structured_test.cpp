#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("native_distinct_structured");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeDecimalColumn("v", true, 50, 5));
    table.append(dbms::makeTextColumn("t", true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    const std::vector<dbms::StorageEngine::SqlRow> input = {
        {{"v", "0.50"}, {"t", "same"}},
        {{"v", "0.500"}, {"t", "same"}},
        {{"v", "2.00"}, {"t", ""}},
        {{"v", std::nullopt}, {"t", "NULL"}},
        {{"v", std::nullopt}, {"t", std::nullopt}}
    };
    for (const auto& row : input)
        assert(g_engine.insertRow(db, table.tablename, row) == dbms::DBStatus::OK);
    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = table.tablename;
    ctx.selectCols = {"v"};
    ctx.orderByCol = "v";
    ctx.distinct = true;
    auto result = dbms::QueryPlanner::executePlanChecked(dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
    assert(result.ok && result.rows.size() == 3);
    assert(result.structuredRowsAvailable);
    assert((result.structuredRows == std::vector<std::vector<std::string>>{{"0.50"}, {"2.00"}, {""}}));
    assert((result.structuredNulls == std::vector<std::vector<bool>>{{false}, {false}, {true}}));
    ctx.offset = 1;
    ctx.limit = 1;
    result = dbms::QueryPlanner::executePlanChecked(dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
    assert(result.ok && result.structuredRowsAvailable);
    assert((result.structuredRows == std::vector<std::vector<std::string>>{{"2.00"}}));
    ctx.offset = ctx.limit = 0;
    ctx.orderByCol.clear();
    ctx.selectCols = {"t"};
    result = dbms::QueryPlanner::executePlanChecked(dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
    assert(result.ok && result.rows.size() == 4 && result.structuredRowsAvailable);
    assert((result.structuredRows == std::vector<std::vector<std::string>>{{"same"}, {""}, {"NULL"}, {""}}));
    assert((result.structuredNulls == std::vector<std::vector<bool>>{{false}, {false}, {false}, {true}}));
    ctx.selectCols = {"v", "t"};
    result = dbms::QueryPlanner::executePlanChecked(dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
    assert(result.ok && result.rows.size() == 4 && result.structuredRowsAvailable);
    assert((result.structuredRows == std::vector<std::vector<std::string>>{{"0.50", "same"}, {"2.00", ""}, {"", "NULL"}, {"", ""}}));
    assert((result.structuredNulls == std::vector<std::vector<bool>>{{false, false}, {false, false}, {true, false}, {true, true}}));
    dbms::TableSchema generated;
    generated.tablename = "generated";
    generated.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    generated.append(dbms::makeIntColumn("f", true, 2));
    auto computed = dbms::makeIntColumn("p", true, 2);
    computed.generatedKind = 'v';
    computed.generatedExpr = "f + 1";
    generated.append(computed);
    assert(g_engine.createTable(db, generated) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, generated.tablename, {{"f", "0"}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, generated.tablename, {{"f", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, generated.tablename, {{"f", std::nullopt}}) == dbms::DBStatus::OK);
    for (bool parallel : {false, true}) {
        dbms::OpPtr scan;
        if (parallel) scan = std::make_unique<dbms::ParallelTableScanOp>(&g_engine, db, generated.tablename, 4);
        else scan = std::make_unique<dbms::TableScanOp>(&g_engine, db, generated.tablename);
        if (!parallel) scan = std::make_unique<dbms::SortOp>(std::move(scan), generated, "f", true);
        scan = std::make_unique<dbms::ProjectOp>(std::move(scan), generated, std::set<std::string>{"p"});
        result = dbms::QueryPlanner::executePlanChecked(std::make_unique<dbms::DistinctOp>(std::move(scan)));
        assert(result.ok && result.structuredRowsAvailable);
        assert((result.structuredRows == std::vector<std::vector<std::string>>{{"1"}, {"2"}, {""}}));
        assert((result.structuredNulls == std::vector<std::vector<bool>>{{false}, {false}, {true}}));
    }
    // Raw materialized streams still retain their explicit legacy behavior.
    auto raw = std::make_unique<dbms::MaterializedRowsOp>(std::vector<std::string>{"a", "a", "b"});
    result = dbms::QueryPlanner::executePlanChecked(std::make_unique<dbms::DistinctOp>(std::move(raw)));
    assert(result.ok && result.rows == std::vector<std::string>({"a", "b"}));
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[NATIVE DISTINCT STRUCTURED] passed\n";
}
