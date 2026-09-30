#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void expectPlan(const std::string& database, const std::string& table,
                       std::vector<dbms::StorageEngine::Condition> conditions,
                       size_t expected) {
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = table;
    context.conds = std::move(conditions);
    auto result = dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
    if (!result.ok || result.rows.size() != expected)
        std::cerr << table << ": expected " << expected << " rows, got " << result.rows.size() << '\n';
    assert(result.ok && result.rows.size() == expected);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    auto database = testDbPath("integer_index_write_key");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    for (const bool explicitColumns : {false, true}) {
        dbms::TableSchema table;
        table.tablename = explicitColumns ? "explicit_key" : "inline_key";
        table.append(dbms::makeIntColumn("id", false, 8, true));
        table.append(dbms::makeIntColumn("value", false, 8));
        if (explicitColumns) table.pkColIndices = {0};
        assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
        assert(g_engine.createIndex(database, table.tablename, "value") == dbms::DBStatus::OK);
        assert(g_engine.createHashIndex(database, table.tablename, "value") == dbms::DBStatus::OK);
        assert(g_engine.createBloomIndex(database, table.tablename, "value") == dbms::DBStatus::OK);
        assert(g_engine.insert(database, table.tablename,
            {{"id", "0001"}, {"value", "+0007"}}) == dbms::DBStatus::OK);
        expectPlan(database, table.tablename, {{"=", "id", "1"}}, 1);
        assert(g_engine.insert(database, table.tablename,
            {{"id", "1"}, {"value", "7"}}) == dbms::DBStatus::DUPLICATE_KEY);
        assert(g_engine.insert(database, table.tablename,
            {{"id", "-0"}, {"value", "0000"}}) == dbms::DBStatus::OK);
        assert(g_engine.insert(database, table.tablename,
            {{"id", "0"}, {"value", "0"}}) == dbms::DBStatus::DUPLICATE_KEY);
        expectPlan(database, table.tablename, {{"=", "value", "7"}}, 1);
        expectPlan(database, table.tablename, {{"=", "id", "1"}, {"=", "value", "7"}}, 1);
        assert(g_engine.getHashIndex(database, table.tablename, "value")->search("7").size() == 1);
        assert(g_engine.getBloomIndex(database, table.tablename, "value")->search("7").size() == 1);
        assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
        assert(g_engine.update(database, table.tablename,
            {{"value", "+0008"}}, {"=id 1"}) == dbms::DBStatus::OK);
        expectPlan(database, table.tablename, {{"=", "value", "8"}}, 1);
        assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
        expectPlan(database, table.tablename, {{"=", "value", "7"}}, 1);
        expectPlan(database, table.tablename, {{"=", "value", "8"}}, 0);
        assert(g_engine.reindex(database, table.tablename) == dbms::DBStatus::OK);
        expectPlan(database, table.tablename, {{"=", "id", "1"}}, 1);
    }
    dbms::TableSchema pair;
    pair.tablename = "pair";
    pair.append(dbms::makeIntColumn("a", false, 8, true));
    pair.append(dbms::makeIntColumn("b", false, 8, true));
    pair.pkColIndices = {0, 1};
    assert(g_engine.createTable(database, pair) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, pair.tablename, {{"a", "+0001"}, {"b", "0002"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, pair.tablename, {{"a", "1"}, {"b", "2"}}) == dbms::DBStatus::DUPLICATE_KEY);
    // Reproduce a pre-fix index mapping in our fixture, without treating a
    // successful empty legacy lookup as a supported behavior. Explicit
    // REINDEX must rebuild its key from the canonical heap datum.
    dbms::TableSchema legacy;
    legacy.tablename = "legacy";
    legacy.append(dbms::makeIntColumn("id", false, 8, true));
    assert(g_engine.createTable(database, legacy) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, legacy.tablename, {{"id", "1"}}) == dbms::DBStatus::OK);
    auto* oldIndex = g_engine.getPKIndex(database, legacy.tablename);
    int64_t legacyRid = 0;
    assert(oldIndex && oldIndex->search("1", legacyRid));
    assert(oldIndex->remove("1"));
    assert(oldIndex->insert("0001", legacyRid));
    assert(oldIndex->flush());
    assert(g_engine.reindex(database, legacy.tablename) == dbms::DBStatus::OK);
    expectPlan(database, legacy.tablename, {{"=", "id", "1"}}, 1);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    const auto renamed = database + "_reopened";
    assert(g_engine.renameDatabase(database, renamed) == dbms::DBStatus::OK);
    database = renamed;
    for (const std::string table : {"inline_key", "explicit_key"}) {
        expectPlan(database, table, {{"=", "id", "1"}}, 1);
        expectPlan(database, table, {{"=", "id", "1"}, {"=", "value", "7"}}, 1);
    }
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[INTEGER INDEX WRITE KEY] passed\n";
}
