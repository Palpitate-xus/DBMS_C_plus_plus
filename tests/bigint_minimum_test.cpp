#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <map>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "bigint_minimum";
    const std::string database = testDbPath(testName);
    const std::string minimum = "-9223372036854775808";
    const std::string maximum = "9223372036854775807";
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "nullable_values";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 2, true));
    table.append(dbms::makeIntColumn("v", true, 4));
    table.cols[1].dataType = "bigint";
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    assert(g_engine.insertRow(database, table.tablename,
        {{"id", "1"}, {"v", minimum}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, table.tablename,
        {{"id", "2"}, {"v", maximum}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, table.tablename,
        {{"id", "3"}, {"v", std::nullopt}}) == DBStatus::OK);
    std::map<std::string, dbms::StorageEngine::SqlCell> observed;
    assert(g_engine.forEachRow(database, table.tablename,
        [&](uint32_t page, uint16_t slot, const char* data, size_t length) {
            const std::string row(data, length);
            const auto id = g_engine.extractColumnValueStatic(row, table, 0);
            observed[id] = g_engine.isColumnNullByRid(database, table.tablename,
                dbms::StorageEngine::encodeRid(page, slot), 1)
                ? dbms::StorageEngine::SqlCell{} :
                  dbms::StorageEngine::SqlCell(g_engine.extractColumnValueStatic(row, table, 1));
        }));
    assert(observed.size() == 3);
    assert(observed.at("1") == minimum);
    assert(observed.at("2") == maximum);
    assert(!observed.at("3"));
    assert(g_engine.createIndex(database, table.tablename, "v") == DBStatus::OK);
    assert(g_engine.query(database, table.tablename, {"=v " + minimum}, {"id"}).size() == 1);
    assert(g_engine.query(database, table.tablename, {"isnull v"}, {"id"}).size() == 1);
    assert(g_engine.updateRows(database, table.tablename,
        {{"v", minimum}}, {"=id 2"}) == DBStatus::OK);
    assert(g_engine.query(database, table.tablename, {"=v " + minimum}, {"id"}).size() == 2);
    const auto minimumMatcher = [&](const dbms::StorageEngine::SqlRow& row) {
        return row.at("v") == minimum;
    };
    std::vector<dbms::StorageEngine::SqlRow> updated;
    assert(g_engine.updateRows(database, table.tablename,
        {{"v", minimum}}, {}, &updated, {}, minimumMatcher) == DBStatus::OK);
    assert(updated.size() == 2);
    for (const auto& row : updated) assert(row.at("v") == minimum);
    updated.clear();
    assert(g_engine.updateRows(database, table.tablename,
        {{"v", minimum}}, {"=id 3"}, &updated) == DBStatus::OK);
    assert(updated.size() == 1 && updated.front().at("v") == minimum);
    updated.clear();
    assert(g_engine.updateRows(database, table.tablename,
        {{"v", std::nullopt}}, {"=id 3"}, &updated) == DBStatus::OK);
    assert(updated.size() == 1 && !updated.front().at("v"));
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.savepoint("restore_minimum") == DBStatus::OK);
    assert(g_engine.updateRows(database, table.tablename,
        {{"v", minimum}}, {"=id 1"}) == DBStatus::OK);
    assert(g_engine.updateRows(database, table.tablename,
        {{"v", minimum}}, {"=id 3"}) == DBStatus::OK);
    assert(g_engine.rollbackToSavepoint("restore_minimum") == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    const auto minimumScan = [&] {
        return dbms::QueryPlanner::executePlanChecked(
            std::make_unique<dbms::IndexScanOp>(
                &g_engine, database, table.tablename, "v", minimum));
    };
    auto indexedMinimum = minimumScan();
    assert(indexedMinimum.ok && indexedMinimum.rows.size() == 2);
    std::vector<dbms::StorageEngine::SqlRow> deleted;
    assert(g_engine.removeRows(database, table.tablename,
        {}, &deleted, minimumMatcher) == DBStatus::OK);
    assert(deleted.size() == 2);
    for (const auto& row : deleted) assert(row.at("v") == minimum);
    indexedMinimum = minimumScan();
    assert(indexedMinimum.ok && indexedMinimum.rows.empty());
    assert(g_engine.query(database, table.tablename, {}, {"id"}).size() == 1);
    assert(g_engine.query(database, table.tablename, {"isnull v"}, {"id"}).size() == 1);
    dbms::TableSchema primary;
    primary.tablename = "minimum_primary";
    primary.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    primary.append(dbms::makeIntColumn("id", false, 4, true));
    primary.cols[0].dataType = "bigint";
    assert(g_engine.createTable(database, primary) == DBStatus::OK);
    assert(g_engine.insertRow(database, primary.tablename, {{"id", minimum}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, primary.tablename, {{"id", minimum}}) == DBStatus::DUPLICATE_KEY);
    const auto result = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(&g_engine, database, primary.tablename, "id", minimum));
    assert(result.ok && result.rows.size() == 1);
    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[BIGINT MINIMUM] passed\n";
}
