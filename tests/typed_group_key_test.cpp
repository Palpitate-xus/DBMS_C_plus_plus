#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void checkGroups(const std::string& db, const dbms::TableSchema& table,
                        const std::vector<std::string>& keys,
                        std::vector<std::string> expected) {
    const std::vector<dbms::StorageEngine::AggItem> items = {{"count", "*", {}, {}}};
    std::sort(expected.begin(), expected.end());
    const auto checkLegacy = [&](std::vector<std::string> rows) {
        for (auto& row : rows) if (!row.empty() && row.back() == ' ') row.pop_back();
        std::sort(rows.begin(), rows.end());
        assert(rows == expected);
    };
    checkLegacy(g_engine.groupAggregate(db, table.tablename, {}, items, keys, {}));
    checkLegacy(g_engine.groupAggregateSets(db, table.tablename, {}, items, keys, {keys}, {}));
    for (bool parallel : {false, true}) {
        auto scan = std::make_unique<dbms::TableScanOp>(&g_engine, db, table.tablename);
        dbms::OpPtr plan;
        if (parallel)
            plan = std::make_unique<dbms::ParallelGroupAggregateOp>(
                std::move(scan), table, keys, items, std::vector<std::string>{}, 4);
        else
            plan = std::make_unique<dbms::GroupAggregateOp>(
                std::move(scan), table, keys, std::vector<std::vector<std::string>>{}, items);
        auto result = dbms::QueryPlanner::executePlanChecked(std::move(plan));
        std::sort(result.rows.begin(), result.rows.end());
        assert(result.ok && result.rows == expected);
    }
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("typed_group_key");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "numeric_values";
    table.append(dbms::makeDecimalColumn("v", true, 50, 5));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& value : {"0.50", "0.500", "2.00"})
        assert(g_engine.insertRow(db, table.tablename, {{"v", value}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"v", std::nullopt}}) == dbms::DBStatus::OK);
    checkGroups(db, table, {"v"}, {"0.50 2", "2.00 1", "NULL 1"});
    table.tablename = "exact_numeric_values";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& value : {"123456789012345678901234567890.1",
                             "123456789012345678901234567890.10",
                             "123456789012345678901234567890.2"})
        assert(g_engine.insertRow(db, table.tablename, {{"v", value}}) == dbms::DBStatus::OK);
    checkGroups(db, table, {"v"}, {"123456789012345678901234567890.1 2",
                                 "123456789012345678901234567890.2 1"});
    table.tablename = "many_numeric_values";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (int i = 0; i < 512; ++i)
        assert(g_engine.insertRow(db, table.tablename, {{"v", i % 2 ? "0.500" : "0.50"}})
            == dbms::DBStatus::OK);
    checkGroups(db, table, {"v"}, {"0.50 512"});
    auto parallel = std::make_unique<dbms::ParallelGroupAggregateOp>(
        std::make_unique<dbms::TableScanOp>(&g_engine, db, table.tablename), table,
        std::vector<std::string>{"v"},
        std::vector<dbms::StorageEngine::AggItem>{{"count", "*", {}, {}}},
        std::vector<std::string>{}, 4);
    assert(parallel->open() && parallel->usedParallelWorkers());
    std::string row;
    assert(parallel->next(row) && row == "0.50 512");
    assert(!parallel->next(row) && !parallel->hasError());
    parallel->close();
    table.tablename = "text_values";
    table.cols[0] = dbms::makeTextColumn("v", true);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& value : {"0.50", "0.500", "", "NULL"})
        assert(g_engine.insertRow(db, table.tablename, {{"v", value}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"v", std::nullopt}}) == dbms::DBStatus::OK);
    checkGroups(db, table, {"v"}, {"0.50 1", "0.500 1", " 1", "NULL 1", "NULL 1"});
    table.tablename = "composite_text";
    table.cols[0].dataName = "a";
    table.append(dbms::makeTextColumn("b", true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    const std::string left = std::string("x") + '\x01' + "y";
    const std::string right = std::string("y") + '\x01' + "z";
    assert(g_engine.insertRow(db, table.tablename, {{"a", left}, {"b", "z"}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"a", "x"}, {"b", right}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"a", ""}, {"b", ""}}) == dbms::DBStatus::OK);
    checkGroups(db, table, {"a", "b"}, {left + " z 1", "x " + right + " 1", "  1"});
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[TYPED GROUP KEY] passed\n";
}
