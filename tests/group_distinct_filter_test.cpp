#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("group_distinct_filter");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("g", false, 4));
    table.append(dbms::makeDecimalColumn("v", true, 20, 5));
    table.append(dbms::makeIntColumn("f", true, 4));
    const std::vector<dbms::StorageEngine::SqlRow> rows = {
        {{"g", "1"}, {"v", "0.50"}, {"f", "1"}},
        {{"g", "1"}, {"v", "0.500"}, {"f", "1"}},
        {{"g", "1"}, {"v", "2.00"}, {"f", "0"}},
        {{"g", "1"}, {"v", std::nullopt}, {"f", "1"}},
        {{"g", "2"}, {"v", "4.00"}, {"f", "0"}},
        {{"g", "2"}, {"v", "8.00"}, {"f", std::nullopt}}
    };
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& row : rows)
        assert(g_engine.insertRow(db, table.tablename, row) == dbms::DBStatus::OK);
    const auto sorted = [](std::vector<std::string> values) {
        for (auto& value : values) if (!value.empty() && value.back() == ' ') value.pop_back();
        std::sort(values.begin(), values.end());
        return values;
    };
    for (const auto& flag : {"1", "0", "9"}) {
        const std::vector<dbms::StorageEngine::AggItem> items = {
            {"count", "distinct v", {"=f " + std::string(flag)}, {}},
            {"count", "*", {}, {}}
        };
        const std::vector<std::string> expected = flag == std::string("1")
            ? std::vector<std::string>{"1 1 4", "2 0 2"}
            : flag == std::string("0") ? std::vector<std::string>{"1 1 4", "2 1 2"}
            : std::vector<std::string>{"1 0 4", "2 0 2"};
        assert(sorted(g_engine.groupAggregate(db, table.tablename, {}, items, {"g"}, {})) == expected);
        auto withTotal = expected;
        withTotal.push_back(flag == std::string("1") ? "NULL 1 6" :
                           flag == std::string("0") ? "NULL 2 6" : "NULL 0 6");
        assert(sorted(g_engine.groupAggregateSets(db, table.tablename, {}, items,
            {"g"}, {{"g"}, {}}, {})) == withTotal);
        for (bool parallel : {false, true}) {
            auto scan = std::make_unique<dbms::TableScanOp>(&g_engine, db, table.tablename);
            dbms::OpPtr plan;
            if (parallel)
                plan = std::make_unique<dbms::ParallelGroupAggregateOp>(
                    std::move(scan), table, std::vector<std::string>{"g"}, items,
                    std::vector<std::string>{}, 4);
            else
                plan = std::make_unique<dbms::GroupAggregateOp>(std::move(scan), table,
                    std::vector<std::string>{"g"}, std::vector<std::vector<std::string>>{}, items);
            auto result = dbms::QueryPlanner::executePlanChecked(std::move(plan));
            assert(result.ok && sorted(result.rows) == expected);
        }
    }
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[GROUP DISTINCT FILTER] passed\n";
}
