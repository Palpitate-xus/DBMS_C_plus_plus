#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::TableSchema table;
    table.tablename = "null_expressions";
    table.append(dbms::makeIntColumn("f", true, 2));
    const std::string zero(4, '\0');
    assert(dbms::StorageEngine::evalConditionOnRow(
        {"isnull", "abs(f)", ""}, zero, table, {true}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"isnull", "abs(f)", ""}, zero, table, {false}));
    assert(dbms::StorageEngine::evalConditionOnRow(
        {"isnotnull", "abs(f)", ""}, zero, table, {false}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"isnotnull", "abs(f)", ""}, zero, table, {true}));
    assert(dbms::StorageEngine::evalConditionOnRow(
        {"isnull", "(f=0)", ""}, zero, table, {true}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"isnull", "(f=0)", ""}, zero, table, {false}));
    assert(dbms::StorageEngine::evalConditionOnRow(
        {"isnotnull", "coalesce(f,0)", ""}, zero, table, {true}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"isnull", "coalesce(f,0)", ""}, zero, table, {true}));
    const auto db = testDbPath("expression_null_predicate");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    auto generated = dbms::makeIntColumn("g", true, 2);
    generated.generatedKind = 'v';
    generated.generatedExpr = "f+1";
    table.append(generated);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"f", "0"}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"f", std::nullopt}}) == dbms::DBStatus::OK);
    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = table.tablename;
    ctx.selectCols = {"f", "g"};
    for (const auto& expression : {"abs(f)", "(f=0)", "g"}) {
        ctx.conds = {{"isnull", expression, ""}};
        auto result = dbms::QueryPlanner::executePlanChecked(
            dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx));
        assert(result.ok && result.structuredRowsAvailable && result.rows.size() == 1);
        assert((result.structuredNulls == std::vector<std::vector<bool>>{{true, true}}));
    }
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[EXPRESSION NULL PREDICATE] passed\n";
}
