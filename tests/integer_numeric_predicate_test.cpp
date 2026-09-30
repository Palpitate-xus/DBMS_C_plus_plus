#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void expectRows(const std::string& database, const std::string& table,
                       dbms::StorageEngine::Condition condition, size_t expected) {
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = table;
    context.selectCols = {"id"};
    context.conds = {condition};
    const auto compact = condition.op + "id " + condition.value;
    const auto stored = g_engine.query(database, table, {compact}, {"id"});
    auto result = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
    if (stored.size() != expected || !result.ok || result.rows.size() != expected)
        std::cerr << table << ' ' << compact << ": expected " << expected
                  << ", storage=" << stored.size() << ", plan=" << result.rows.size() << '\n';
    assert(stored.size() == expected && result.ok && result.rows.size() == expected);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("integer_numeric_predicate");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    for (const bool indexed : {false, true}) {
        dbms::TableSchema table;
        table.tablename = indexed ? "indexed" : "heap";
        table.append(dbms::makeIntColumn("id", false, 8, indexed));
        table.append(dbms::makeIntColumn("value", false, 2));
        assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
        for (const std::string key : {"-1", "0", "1", "2", "9007199254740992", "9007199254740993"})
            assert(g_engine.insert(database, table.tablename, {{"id", key}, {"value", "7"}}) == dbms::DBStatus::OK);
        if (indexed)
            assert(g_engine.createIndex(database, table.tablename, "value") == dbms::DBStatus::OK);
        expectRows(database, table.tablename, {"=", "id", "1.0"}, 1);
        expectRows(database, table.tablename, {"=", "id", "1e0"}, 1);
        expectRows(database, table.tablename, {"=", "id", "1.1"}, 0);
        expectRows(database, table.tablename, {"<", "id", "1.1"}, 3);
        expectRows(database, table.tablename, {"<=", "id", "1.0"}, 3);
        expectRows(database, table.tablename, {">", "id", "1.1"}, 3);
        expectRows(database, table.tablename, {">=", "id", "2.0"}, 3);
        expectRows(database, table.tablename, {"!=", "id", "1.0"}, 5);
        expectRows(database, table.tablename, {"<>", "id", "1.0"}, 5);
        expectRows(database, table.tablename, {"=", "id", "9007199254740993.0"}, 1);
        expectRows(database, table.tablename, {">", "id", "9007199254740992.99999999"}, 1);
        expectRows(database, table.tablename, {"<", "id", "9007199254740993.00000001"}, 6);
        expectRows(database, table.tablename, {"=", "id", "1.00000000000000000000000000000000000001"}, 0);
        expectRows(database, table.tablename, {"=", "id", "9223372036854775808.0"}, 0);
        expectRows(database, table.tablename, {"between", "id", "9007199254740992.9 9007199254740993.1"}, 1);
        expectRows(database, table.tablename, {"notbetween", "id", "9007199254740992.9 9007199254740993.1"}, 5);
        if (indexed) {
            dbms::PlanContext context;
            context.dbname = database;
            context.tablename = table.tablename;
            context.selectCols = {"id"};
            context.conds = {{"=", "id", "9007199254740993.0"}, {"=", "value", "7.0"}};
            auto result = dbms::QueryPlanner::executePlanChecked(
                dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
            assert(result.ok && result.rows.size() == 1);
            result = dbms::QueryPlanner::executePlanChecked(
                dbms::QueryPlanner::buildDisjunctiveSelectPlan(&g_engine, context,
                    {{{"=", "id", "1.00001"}}, {{"=", "id", "2.0"}}}));
            assert(result.ok && result.rows.size() == 1);
        }
    }
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[INTEGER NUMERIC PREDICATE] passed\n";
}
