#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void matches(const std::string& db, const std::string& probe, size_t count) {
    const auto stored = g_engine.query(db, "secondary", {"=v " + probe}, {"id"});
    dbms::PlanContext context;
    context.dbname = db;
    context.tablename = "secondary";
    context.selectCols = {"id"};
    context.conds = {{"=", "v", probe}};
    const auto planned = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
    assert(stored.size() == count && planned.ok && planned.rows.size() == count);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("numeric_index_key_equality");
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "primary_values";
    table.append(dbms::makeDecimalColumn("v", false, 40, 10, true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (const auto& values : std::vector<std::pair<std::string, std::string>>{
            {"0.50", "0.500"}, {"-0.00", "0.000"},
            {"123456789012345678901234567890.100", "123456789012345678901234567890.1000"},
            {"NaN", "nan"}, {"Infinity", "+INFINITY"}}) {
        assert(g_engine.insert(db, table.tablename, {{"v", values.first}}) == dbms::DBStatus::OK);
        assert(g_engine.insert(db, table.tablename, {{"v", values.second}}) == dbms::DBStatus::DUPLICATE_KEY);
    }
    // Keep this exact-value test below BPTree's separately tracked 20-byte
    // key limit. Long common-prefix keys currently collide after truncation.
    assert(g_engine.insert(db, table.tablename, {{"v", "9007199254740992.1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"v", "9007199254740992.2"}}) == dbms::DBStatus::OK);
    table.tablename = "secondary";
    table.cols[0].isPrimaryKey = false;
    table.pkColIndices.clear();
    table.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    for (int i = 1; i <= 3; ++i)
        assert(g_engine.insert(db, table.tablename,
            {{"id", std::to_string(i)}, {"v", i == 1 ? "0.50" : i == 2 ? "0.500" : "2.00"}})
            == dbms::DBStatus::OK);
    assert(g_engine.createIndex(db, "secondary", "v") == dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(db, "secondary", "v") == dbms::DBStatus::OK);
    for (const auto& probe : {"0.5", "0.50", "0.5000", "5e-1"}) matches(db, probe, 2);
    assert(g_engine.update(db, "secondary", {{"v", "0.50000"}}, {"=id 1"}) == dbms::DBStatus::OK);
    matches(db, "0.5", 2);
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "secondary", {{"id", "4"}, {"v", "0.500000"}}) == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    matches(db, "0.5", 2);
    assert(g_engine.remove(db, "secondary", {"=id 1"}) == dbms::DBStatus::OK);
    matches(db, "0.5000", 1);
    assert(g_engine.update(db, "secondary", {{"v", "3.00"}}, {"=id 2"}) == dbms::DBStatus::OK);
    matches(db, "0.5", 0);
    matches(db, "3.000", 1);
    assert(g_engine.reindex(db, "secondary") == dbms::DBStatus::OK);
    matches(db, "3.0", 1);
    table.tablename = "unique_values";
    table.cols[0].isUnique = true;
    table.cols[0].isNull = true;
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"id", "1"}, {"v", "0.50"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"id", "2"}, {"v", "0.500"}}) == dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insertRow(db, table.tablename, {{"id", "3"}, {"v", std::nullopt}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(db, table.tablename, {{"id", "4"}, {"v", std::nullopt}}) == dbms::DBStatus::OK);
    assert(g_engine.dropDatabase(db) == dbms::DBStatus::OK);
    std::cout << "[NUMERIC INDEX KEY EQUALITY] passed\n";
}
