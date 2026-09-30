#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("index_scan_full_value_recheck");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    const std::string prefix = "12345678901234567890";
    const auto first = prefix + "-alpha";
    const auto second = prefix + "-beta";
    const auto absent = prefix + "-missing";
    dbms::TableSchema table;
    table.tablename = "secondary_values";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 2, true));
    table.append(dbms::makeTextColumn("k", true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"id", "1"}, {"k", first}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"id", "2"}, {"k", second}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"id", "3"}, {"k", std::nullopt}}) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(db, table.tablename, "k") == dbms::DBStatus::OK);
    const auto lookup = [&](const std::string& name, const std::string& key, size_t count) {
        auto result = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<dbms::IndexScanOp>(&g_engine, db, name, "k", key));
        assert(result.ok && result.rows.size() == count);
        dbms::PlanContext ctx;
        ctx.dbname = db;
        ctx.tablename = name;
        ctx.selectCols = {"k"};
        ctx.conds = {{"=", "k", key}};
        result = dbms::QueryPlanner::executePlanChecked(dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
        assert(result.ok && result.rows.size() == count);
        if (count) {
            assert(result.structuredRowsAvailable);
            assert((result.structuredRows == std::vector<std::vector<std::string>>{{key}}));
            assert((result.structuredNulls == std::vector<std::vector<bool>>{{false}}));
        }
    };
    lookup(table.tablename, first, 1);
    lookup(table.tablename, second, 1);
    lookup(table.tablename, absent, 0);
    table.tablename = "primary_values";
    table.cols[0].isPrimaryKey = false;
    table.cols[1].isPrimaryKey = true;
    table.cols[1].isNull = false;
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"id", "1"}, {"k", first}}) == dbms::DBStatus::OK);
    lookup(table.tablename, first, 1);
    lookup(table.tablename, absent, 0);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[INDEX SCAN FULL VALUE RECHECK] passed\n";
}
