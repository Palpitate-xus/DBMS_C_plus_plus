#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void estimatedHotRows(const std::string& db, const std::string& table,
                             const std::string& probe, size_t expected) {
    dbms::PlanContext context;
    context.dbname = db;
    context.tablename = table;
    context.conds = {{"=", "v", probe}};
    auto plan = dbms::QueryPlanner::buildSelectPlan(&g_engine, context);
    const auto explain = dbms::QueryPlanner::explain(plan, &g_engine, db);
    const auto filter = explain.find("Filter");
    assert(filter != std::string::npos);
    const auto rows = explain.find("rows=", filter);
    assert(rows != std::string::npos);
    if (std::stoull(explain.substr(rows + 5)) != expected) std::cerr << explain;
    assert(std::stoull(explain.substr(rows + 5)) == expected);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("typed_statistics_mcv");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "numeric_values";
    table.append(dbms::makeDecimalColumn("v", true, 20, 4));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (int i = 0; i < 40; ++i)
        assert(g_engine.insert(db, table.tablename, {{"v", i < 20 ? "0.50" : i < 35 ? "0.500" : "2.00"}})
            == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(db, table.tablename));
    const auto numeric = g_engine.getColumnStats(db, table.tablename, "v");
    assert(numeric.cardinality == 2 && numeric.mcv.size() == 2);
    assert(numeric.mcv.front().second == 35);
    assert(dbms::StorageEngine::compareValues(table.cols[0], numeric.mcv.front().first,
        false, "0.5", false, "=") == dbms::StorageEngine::PredicateTruth::True);
    for (const auto& probe : {"0.5", "0.50", "0.5000"})
        estimatedHotRows(db, table.tablename, probe, 35);
    table.tablename = "floating_values";
    table.cols[0] = dbms::makeDoubleColumn("v", false);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (int i = 0; i < 40; ++i)
        assert(g_engine.insert(db, table.tablename, {{"v", i < 35 ? "1.25" : "2.25"}})
            == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(db, table.tablename));
    estimatedHotRows(db, table.tablename, "1.2500", 35);
    table.tablename = "text_values";
    table.cols[0] = dbms::makeVarCharColumn("v", false, 32);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& value : {"0.50", "0.500", "0.5", ""})
        assert(g_engine.insert(db, table.tablename, {{"v", value}}) == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(db, table.tablename));
    const auto text = g_engine.getColumnStats(db, table.tablename, "v");
    assert(text.cardinality == 4 && text.mcv.size() == 4 && text.nullCount == 0);
    table.tablename = "empty";
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.analyzeTable(db, table.tablename));
    const auto empty = g_engine.getColumnStats(db, table.tablename, "v");
    assert(empty.cardinality == 0 && empty.mcv.empty());
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[TYPED STATISTICS MCV] passed\n";
}
