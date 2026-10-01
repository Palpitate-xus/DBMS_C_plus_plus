#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include "types/numeric.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "exact_sum";
    const std::string database = testDbPath(name);
    const std::string maximum = "9223372036854775807";
    const std::string minimum = "-9223372036854775808";
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "bigint_values";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 2, true));
    table.append(dbms::makeIntColumn("grp", false, 2));
    table.append(dbms::makeIntColumn("v", true, 4));
    table.cols[2].dataType = "bigint";
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    const auto insert = [&](int id, const dbms::StorageEngine::SqlCell& value) {
        assert(g_engine.insertRow(database, table.tablename,
            {{"id", std::to_string(id)}, {"grp", "1"}, {"v", value}}) == DBStatus::OK);
    };
    insert(1, maximum);
    insert(2, maximum);
    insert(3, std::nullopt);
    const std::vector<dbms::StorageEngine::AggItem> items = {
        {"sum", "v", {}, {}}, {"avg", "v", {}, {}}};
    // The compatibility APIs must not overflow an int64_t accumulator even
    // though they already render the independently computed numeric result.
    const std::string legacy = "18446744073709551614 " + maximum + " ";
    const auto legacyRows = g_engine.aggregate(database, table.tablename, {}, items);
    if (legacyRows != std::vector<std::string>{legacy}) {
        for (const auto& row : legacyRows) std::cerr << "legacy aggregate [" << row << "]\n";
    }
    assert(legacyRows == std::vector<std::string>{legacy});
    assert(g_engine.groupAggregate(database, table.tablename, {}, items, {"grp"}, {}) ==
           std::vector<std::string>{"1 " + legacy});
    assert(g_engine.groupAggregateSets(database, table.tablename, {}, items,
           {"grp"}, {{"grp"}}, {}) == std::vector<std::string>{"1 " + legacy});
    const auto plan = [&](bool parallel, bool grouped) {
        dbms::OpPtr scan = std::make_unique<dbms::TableScanOp>(
            &g_engine, database, table.tablename);
        const std::vector<std::string> groups = grouped
            ? std::vector<std::string>{"grp"} : std::vector<std::string>{};
        dbms::OpPtr aggregate;
        if (parallel) {
            aggregate = std::make_unique<dbms::ParallelGroupAggregateOp>(
                std::move(scan), table, groups, items,
                std::vector<std::string>{}, 4);
        } else {
            aggregate = std::make_unique<dbms::GroupAggregateOp>(
                std::move(scan), table, groups,
                std::vector<std::vector<std::string>>{}, items,
                std::vector<std::string>{});
        }
        return dbms::QueryPlanner::executePlanChecked(std::move(aggregate));
    };
    const auto expect = [&](const std::string& sum) {
        for (const bool parallel : {false, true}) {
            for (const bool grouped : {false, true}) {
                const auto result = plan(parallel, grouped);
                assert(result.ok && result.structuredRowsAvailable);
                const std::vector<std::string> row = grouped
                    ? std::vector<std::string>{"1", sum, maximum}
                    : std::vector<std::string>{sum, maximum};
                assert(result.structuredRows == std::vector<std::vector<std::string>>{row});
                assert(result.structuredNulls == std::vector<std::vector<bool>>{
                    std::vector<bool>(row.size(), false)});
            }
        }
    };
    expect("18446744073709551614");
    for (int id = 4; id <= 301; ++id) insert(id, maximum);
    expect((dbms::Numeric(maximum) * dbms::Numeric(300)).toString());
    assert(g_engine.truncateTable(database, table.tablename) == DBStatus::OK);
    insert(1, minimum);
    insert(2, minimum);
    const auto negative = plan(false, false);
    assert(negative.ok && (negative.structuredRows ==
        std::vector<std::vector<std::string>>{{"-18446744073709551616", minimum}}));
    assert(g_engine.truncateTable(database, table.tablename) == DBStatus::OK);
    insert(1, std::nullopt);
    for (const bool parallel : {false, true}) {
        const auto empty = plan(parallel, false);
        assert(empty.ok && (empty.structuredNulls ==
            std::vector<std::vector<bool>>{{true, true}}));
    }
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[EXACT SUM] passed\n";
}
