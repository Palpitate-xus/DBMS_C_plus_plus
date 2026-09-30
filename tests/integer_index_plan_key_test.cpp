#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void expectRows(const std::string& database, const std::string& table,
                       std::vector<dbms::StorageEngine::Condition> conditions,
                       size_t expected) {
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = table;
    context.selectCols = {"id"};
    context.conds = std::move(conditions);
    auto result = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
    if (!result.ok || result.rows.size() != expected) {
        std::cerr << table << ": expected " << expected << " rows, got "
                  << result.rows.size() << '\n';
    }
    assert(result.ok && result.rows.size() == expected);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("integer_index_plan_key");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    for (const std::string family : {"btree", "hash", "bloom"}) {
        dbms::TableSchema table;
        table.tablename = family;
        table.append(dbms::makeIntColumn("id", false, 8, true));
        table.append(dbms::makeIntColumn("value", false, 8));
        // Cover explicit single-key encoding as well as inline encoding.
        if (family == "btree") table.pkColIndices = {0};
        assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
        for (const auto& values : std::vector<std::pair<std::string, std::string>>{
                 {"1", "7"}, {"-1", "-7"}, {"0", "0"},
                 {"9007199254740993", "9007199254740993"}}) {
            assert(g_engine.insert(database, family,
                {{"id", values.first}, {"value", values.second}}) == dbms::DBStatus::OK);
        }
        if (family == "btree") {
            assert(g_engine.createIndex(database, family, "value") == dbms::DBStatus::OK);
        } else if (family == "hash") {
            assert(g_engine.createHashIndex(database, family, "value") == dbms::DBStatus::OK);
        } else {
            assert(g_engine.createBloomIndex(database, family, "value") == dbms::DBStatus::OK);
        }
        for (const std::string key : {"0001", "+0001", "00000000000000000001"})
            expectRows(database, family, {{"=", "id", key}}, 1);
        expectRows(database, family, {{"=", "id", "-0001"}}, 1);
        expectRows(database, family, {{"=", "id", "-0"}}, 1);
        expectRows(database, family, {{"=", "id", "+9007199254740993"}}, 1);
        expectRows(database, family, {{"=", "id", "0001"}, {"=", "value", "+0007"}}, 1);
        expectRows(database, family, {{"=", "id", "+9007199254740993"},
                                     {"=", "value", "9007199254740993"}}, 1);
        if (family == "btree")
            expectRows(database, family, {{"=", "value", "+0007"}}, 1);
        dbms::PlanContext context;
        context.dbname = database;
        context.tablename = family;
        context.selectCols = {"id"};
        auto result = dbms::QueryPlanner::executePlanChecked(
            dbms::QueryPlanner::buildDisjunctiveSelectPlan(&g_engine, context,
                {{{"=", "id", "0001"}}, {{"=", "value", "-0007"}}}));
        assert(result.ok && result.rows.size() == 2);
    }
    dbms::TableSchema text;
    text.tablename = "text_keys";
    text.append(dbms::makeVarCharColumn("id", false, 20, true));
    assert(g_engine.createTable(database, text) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, text.tablename, {{"id", "0001"}}) == dbms::DBStatus::OK);
    expectRows(database, text.tablename, {{"=", "id", "0001"}}, 1);
    expectRows(database, text.tablename, {{"=", "id", "1"}}, 0);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[INTEGER INDEX PLAN KEY] passed\n";
}
