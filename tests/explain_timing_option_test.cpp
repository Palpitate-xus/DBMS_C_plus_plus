#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("explain_timing_option");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, table.tablename, {{"id", "1"}}) == dbms::DBStatus::OK);
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = table.tablename;
    context.selectCols = {"id"};
    context.conds = {{"=", "id", "1"}};
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    assert(plan->open());
    std::string row;
    assert(plan->next(row) && !plan->next(row));
    plan->close();
    dbms::QueryPlanner::ExplainOptions options;
    options.analyze = true;
    options.timing = false;
    auto text = dbms::QueryPlanner::explain(plan, &g_engine, database, options);
    if (text.find("actual time=") != std::string::npos) std::cerr << text;
    assert(text.find("actual time=") == std::string::npos);
    const auto first = text.find("(actual rows=1 loops=2)");
    assert(first != std::string::npos);
    assert(text.find("(actual rows=1 loops=2)", first + 1) != std::string::npos);
    options.timing = true;
    text = dbms::QueryPlanner::explain(plan, &g_engine, database, options);
    assert(text.find("actual time=") != std::string::npos);
    assert(text.find("rows=1 loops=2") != std::string::npos);
    options.analyze = false;
    text = dbms::QueryPlanner::explain(plan, &g_engine, database, options);
    assert(text.find("(actual ") == std::string::npos);
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[EXPLAIN TIMING OPTION] passed\n";
}
