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
    if (!result.ok || result.rows.size() != expected)
        std::cerr << table << ": expected " << expected << " rows, got " << result.rows.size() << '\n';
    assert(result.ok && result.rows.size() == expected);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::StorageEngine::setMoneyLocale("C");
    const auto database = testDbPath("typed_index_plan_key");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    struct Case {
        dbms::Column column;
        std::string stored;
        std::string query;
    };
    const std::vector<Case> cases{
        {dbms::makeDoubleColumn("value", false), "1.5", "1.50"},
        {dbms::makeFloatColumn("value", false), "0.1", "0.1000000000"},
        {dbms::makeDoubleColumn("value", false), "-0", "0.0"},
        {dbms::makeDoubleColumn("value", false), "NaN", "nan"},
        {dbms::makeMoneyColumn("value", false), "$1.50", "1.500"},
        {dbms::makeUuidColumn("value", false), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}"},
        {dbms::makeStringColumn("value", false, 16), "a", "a   "},
    };
    for (const std::string family : {"btree", "hash", "bloom"}) {
        for (size_t i = 0; i < cases.size(); ++i) {
            const std::string name = family + std::to_string(i);
            dbms::TableSchema table;
            table.tablename = name;
            table.append(dbms::makeIntColumn("id", false, 2, true));
            table.append(cases[i].column);
            assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
            assert(g_engine.insert(database, name,
                {{"id", "1"}, {"value", cases[i].stored}}) == dbms::DBStatus::OK);
            if (family == "btree") {
                assert(g_engine.createIndex(database, name, "value") == dbms::DBStatus::OK);
            } else if (family == "hash") {
                assert(g_engine.createHashIndex(database, name, "value") == dbms::DBStatus::OK);
            } else {
                assert(g_engine.createBloomIndex(database, name, "value") == dbms::DBStatus::OK);
            }
            expectRows(database, name, {{"=", "value", cases[i].query}}, 1);
            expectRows(database, name, {{"=", "id", "1"}, {"=", "value", cases[i].query}}, 1);
            dbms::PlanContext context;
            context.dbname = database;
            context.tablename = name;
            context.selectCols = {"id"};
            auto result = dbms::QueryPlanner::executePlanChecked(
                dbms::QueryPlanner::buildDisjunctiveSelectPlan(&g_engine, context,
                    {{{"=", "id", "999"}}, {{"=", "value", cases[i].query}}}));
            assert(result.ok && result.rows.size() == 1);
        }
    }
    // Primary keys already convert their logical value in buildPKValue.
    // In particular, converting money's hexadecimal index key twice is not
    // safe: a digits-only encoded key can itself parse as a money literal.
    for (const bool explicitColumns : {false, true}) {
        dbms::TableSchema table;
        table.tablename = explicitColumns ? "money_explicit" : "money_inline";
        table.append(dbms::makeMoneyColumn("id", false, true));
        if (explicitColumns) table.pkColIndices = {0};
        assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
        assert(g_engine.insert(database, table.tablename, {{"id", "$0.00"}}) == dbms::DBStatus::OK);
        expectRows(database, table.tablename, {{"=", "id", "0.00"}}, 1);
    }
    dbms::TableSchema text;
    text.tablename = "varchar_space";
    text.append(dbms::makeIntColumn("id", false, 2, true));
    text.append(dbms::makeVarCharColumn("value", false, 16));
    assert(g_engine.createTable(database, text) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, text.tablename, {{"id", "1"}, {"value", "a"}}) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, text.tablename, "value") == dbms::DBStatus::OK);
    expectRows(database, text.tablename, {{"=", "value", "a   "}}, 0);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[TYPED INDEX PLAN KEY] passed\n";
}
