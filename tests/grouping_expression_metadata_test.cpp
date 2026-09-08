#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "grouping_expression_metadata";
    cleanupTestDb(name);
    const std::string db = testDbPath(name);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("v", false, 4));
    table.append(dbms::makeTextColumn("tag", true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename,
        {{"id", "1"}, {"v", "10"}, {"tag", std::nullopt}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename,
        {{"id", "2"}, {"v", "21"}, {"tag", ""}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename,
        {{"id", "3"}, {"v", "32"}, {"tag", "NULL"}}) == dbms::DBStatus::OK);
    auto run = [&](const std::vector<std::string>& keys,
                   const std::vector<std::vector<std::string>>& sets,
                   bool parallel, const dbms::StorageEngine::AggItem& aggregate) {
        auto scan = std::make_unique<dbms::TableScanOp>(&g_engine, db, table.tablename);
        dbms::OpPtr plan;
        if (parallel) {
            plan = std::make_unique<dbms::ParallelGroupAggregateOp>(
                std::move(scan), table, keys,
                std::vector<dbms::StorageEngine::AggItem>{aggregate},
                std::vector<std::string>{}, 4);
        } else {
            plan = std::make_unique<dbms::GroupAggregateOp>(
                std::move(scan), table, keys, sets,
                std::vector<dbms::StorageEngine::AggItem>{aggregate},
                std::vector<std::string>{});
        }
        auto result = dbms::QueryPlanner::executePlanChecked(std::move(plan));
        std::sort(result.rows.begin(), result.rows.end());
        return result;
    };
    auto expect = [&](const std::vector<std::string>& keys,
                      const std::vector<std::vector<std::string>>& sets,
                      std::vector<std::string> expected) {
        auto result = run(keys, sets, false, {"sum", "v", {}, {}});
        std::sort(expected.begin(), expected.end());
        assert(result.ok && result.rows == expected);
    };
    expect({"id / 10"}, {{"id / 10"}, {}}, {"0 63", "NULL 63"});
    expect({"v % 2", "id / 10"}, {{"v % 2"}, {"id / 10"}, {}},
           {"0 NULL 42", "1 NULL 21", "NULL 0 63", "NULL NULL 63"});
    expect({"v % 2"}, {{"v % 2"}, {}}, {"0 42", "1 21", "NULL 63"});
    for (bool parallel : {false, true}) {
        const auto grouped = run({"tag"}, {}, parallel, {"count", "*", {}, {}});
        assert(grouped.ok);
        assert((grouped.rows == std::vector<std::string>{" 1", "NULL 1", "NULL 1"}));
        const auto expression = run({"coalesce(tag, 'missing')"}, {}, parallel,
                                    {"count", "*", {}, {}});
        assert(expression.ok);
        assert((expression.rows == std::vector<std::string>{" 1", "NULL 1", "missing 1"}));
        const auto error = run({"1 / (id - 1)"}, {}, parallel,
                              {"count", "*", {}, {}});
        assert(!error.ok && error.error.find("SQLSTATE 22012") != std::string::npos);
    }
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[GROUPING EXPRESSION METADATA] passed\n";
}
