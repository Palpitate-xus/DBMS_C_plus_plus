#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("explain_json_actuals");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "1"}}) == dbms::DBStatus::OK);
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = "items";
    context.selectCols = {"id"};
    context.conds = {{"=", "id", "1"}};
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    assert(plan->open());
    std::string row;
    assert(plan->next(row) && !plan->next(row));
    plan->close();
    dbms::QueryPlanner::ExplainOptions options;
    options.analyze = true;
    auto json = dbms::QueryPlanner::explainJson(plan, &g_engine, database, options);
    if (json.find("\"actualRows\":1") == std::string::npos) std::cerr << json;
    assert(json.find("\"actualRows\":1") != std::string::npos);
    assert(json.find("\"actualLoops\":2") != std::string::npos);
    assert(json.find("\"actualTimeMs\":") != std::string::npos);
    options.timing = false;
    json = dbms::QueryPlanner::explainJson(plan, &g_engine, database, options);
    assert(json.find("\"actualRows\":1") != std::string::npos);
    assert(json.find("\"actualTimeMs\":") == std::string::npos);
    options.buffers = true;
    dbms::QueryPlanner::ExplainExecutionStats execution;
    execution.actualRows = 1;
    execution.executionTimeMs = 2.5;
    execution.sharedHits = 3;
    execution.sharedReads = 1;
    json = dbms::QueryPlanner::explainJson(plan, &g_engine, database, options, execution);
    assert(json.find("\"actualRows\": 1") != std::string::npos);
    assert(json.find("\"executionTimeMs\":") == std::string::npos);
    assert(json.find("\"sharedHit\": 3") != std::string::npos);
    assert(json.find("\"sharedRead\": 1") != std::string::npos);
    assert(json.find("\"hitRate\": 75") != std::string::npos);
    options.timing = true;
    json = dbms::QueryPlanner::explainJson(plan, &g_engine, database, options, execution);
    assert(json.find("\"executionTimeMs\": 2.500000") != std::string::npos);
    options.analyze = false;
    json = dbms::QueryPlanner::explainJson(plan, &g_engine, database, options);
    assert(json.find("\"actualRows\":") == std::string::npos);
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[EXPLAIN JSON ACTUALS] passed\n";
}
