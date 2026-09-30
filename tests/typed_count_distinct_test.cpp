#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void expectCount(const std::string& db, const dbms::TableSchema& table,
                        const std::string& col, const std::string& expected,
                        bool hasInput = true) {
    const std::vector<dbms::StorageEngine::AggItem> items = {{"count", "distinct " + col, {}, {}}};
    auto scalar = g_engine.aggregate(db, table.tablename, {}, items);
    assert(scalar == std::vector<std::string>{expected + " "});
    if (hasInput) {
        const auto checkLegacyCount = [&](const std::vector<std::string>& rows) {
            assert(rows.size() == 1);
            const auto first = rows[0].find_first_not_of(' ');
            const auto last = rows[0].find_last_not_of(' ');
            assert(first != std::string::npos && rows[0].substr(first, last - first + 1) == expected);
        };
        checkLegacyCount(g_engine.groupAggregate(db, table.tablename, {}, items, {}, {}));
        checkLegacyCount(g_engine.groupAggregateSets(db, table.tablename, {}, items, {}, {{}}, {}));
    }
    for (bool parallel : {false, true}) {
        auto scan = std::make_unique<dbms::TableScanOp>(&g_engine, db, table.tablename);
        dbms::OpPtr plan;
        if (parallel)
            plan = std::make_unique<dbms::ParallelGroupAggregateOp>(
                std::move(scan), table, std::vector<std::string>{}, items,
                std::vector<std::string>{}, 4);
        else
            plan = std::make_unique<dbms::GroupAggregateOp>(
                std::move(scan), table, std::vector<std::string>{},
                std::vector<std::vector<std::string>>{}, items);
        auto result = dbms::QueryPlanner::executePlanChecked(std::move(plan));
        assert(result.ok && result.rows == std::vector<std::string>{expected});
        assert(result.structuredRows == std::vector<std::vector<std::string>>{{expected}});
        assert(result.structuredNulls == std::vector<std::vector<bool>>{{false}});
    }
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("typed_count_distinct");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "numeric_values";
    table.append(dbms::makeDecimalColumn("v", true, 50, 5));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& value : {"0.50", "0.500", "2.00", "2.000"})
        assert(g_engine.insert(db, table.tablename, {{"v", value}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"v", std::nullopt}}) == dbms::DBStatus::OK);
    expectCount(db, table, "v", "2");
    table.tablename = "exact_numeric_values";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& value : {"123456789012345678901234567890.1",
                             "123456789012345678901234567890.10",
                             "123456789012345678901234567890.2"})
        assert(g_engine.insert(db, table.tablename, {{"v", value}}) == dbms::DBStatus::OK);
    expectCount(db, table, "v", "2");
    table.tablename = "text_values";
    table.cols[0] = dbms::makeTextColumn("v", true);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& value : {"0.50", "0.500", "NULL", ""})
        assert(g_engine.insertRow(db, table.tablename, {{"v", value}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"v", std::nullopt}}) == dbms::DBStatus::OK);
    expectCount(db, table, "v", "4");
    table.tablename = "all_null";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"v", std::nullopt}}) == dbms::DBStatus::OK);
    expectCount(db, table, "v", "0");
    table.tablename = "empty";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    expectCount(db, table, "v", "0", false);
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[TYPED COUNT DISTINCT] passed\n";
}
