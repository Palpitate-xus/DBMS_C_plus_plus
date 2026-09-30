#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("bitmap_explain");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("value", false, 4));
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
    for (const std::string id : {"1", "2", "3"}) {
        assert(g_engine.insert(database, "items",
            {{"id", id}, {"value", id == "3" ? "8" : "7"}}) == dbms::DBStatus::OK);
    }
    assert(g_engine.createHashIndex(database, "items", "value") == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(database, "items"));
    for (const bool disjunction : {false, true}) {
        dbms::OpPtr plan;
        if (disjunction) {
            plan = std::make_unique<dbms::BitmapOrHeapScanOp>(
                &g_engine, database, "items",
                std::vector<std::vector<dbms::StorageEngine::Condition>>{
                    {{"=", "value", "7"}}, {{"=", "id", "3"}}});
        } else {
            plan = std::make_unique<dbms::BitmapHeapScanOp>(
                &g_engine, database, "items",
                std::vector<dbms::StorageEngine::Condition>{
                    {"=", "value", "7"}, {"=", "id", "1"}});
        }
        const std::string node = disjunction ? "BitmapOrHeapScan" : "BitmapHeapScan";
        const auto text = dbms::QueryPlanner::explain(plan, &g_engine, database);
        if (text.find(node) == std::string::npos) std::cerr << text;
        assert(text.find(node + "(table=items)") != std::string::npos);
        assert(text.find("Unknown") == std::string::npos);
        assert(text.find("rows=3") != std::string::npos);
        dbms::QueryPlanner::ExplainOptions options;
        const auto json = dbms::QueryPlanner::explainJson(plan, &g_engine, database, options);
        assert(json.find("\"nodeType\":\"" + node + "\"") != std::string::npos);
        assert(json.find("\"table\":\"items\"") != std::string::npos);
        assert(json.find("\"rows\":3") != std::string::npos);
        assert(json.find("Unknown") == std::string::npos);

        assert(plan->open());
        std::string row;
        size_t rows = 0;
        while (plan->next(row)) ++rows;
        plan->close();
        assert(rows == (disjunction ? 3 : 1));
        options.analyze = true;
        const auto actual = dbms::QueryPlanner::explain(plan, &g_engine, database, options);
        assert(actual.find("(actual time=") != std::string::npos);
        assert(actual.find("rows=" + std::to_string(rows) +
                           " loops=" + std::to_string(rows + 1)) != std::string::npos);
        options.costs = false;
        assert(dbms::QueryPlanner::explain(plan, &g_engine, database, options)
                   .find("cost=") == std::string::npos);
    }
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[BITMAP EXPLAIN] passed\n";
}
