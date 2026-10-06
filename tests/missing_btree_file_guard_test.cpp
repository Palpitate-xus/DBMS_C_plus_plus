#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "storage/BufferPool.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <memory>

extern dbms::StorageEngine g_engine;

static bool rejectsIndexScan(const std::string& database,
                             const std::string& column,
                             const std::string& key) {
    try {
        const auto result = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<dbms::IndexScanOp>(
                &g_engine, database, "items", column, key));
        assert(!result.ok && result.errorSqlState == "XX001");
        assert(result.errorException && !result.errorMessage.empty());
        assert(result.rows.empty() && result.structuredRows.empty() &&
               result.structuredNulls.empty());
        result.throwIfFailed();
    } catch (const dbms::DbError& error) {
        return error.sqlState() == "XX001";
    }
    return false;
}

static bool rejectsBitmapScan(const std::string& database,
                              const std::string& primaryKey) {
    try {
        const auto result = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<dbms::BitmapHeapScanOp>(
                &g_engine, database, "items",
                std::vector<dbms::StorageEngine::Condition>{
                    {"=", "id", primaryKey}, {"=", "value", "7"}}));
        assert(!result.ok && result.errorSqlState == "XX001");
        assert(result.errorException && !result.errorMessage.empty());
        assert(result.rows.empty() && result.structuredRows.empty() &&
               result.structuredNulls.empty());
        result.throwIfFailed();
    } catch (const dbms::DbError& error) {
        return error.sqlState() == "XX001";
    }
    return false;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "missing_btree_guard";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    const auto missingPath = std::filesystem::path(database) / "uncreated.idx";
    {
        dbms::BufferPool pages(missingPath.string(), 4);
        assert(!pages.openExisting());
        assert(!std::filesystem::exists(missingPath));
        dbms::BPTree tree(missingPath);
        assert(!tree.openExisting());
        assert(!std::filesystem::exists(missingPath));
        assert(tree.open());
        assert(tree.insert("existing", 123));
        tree.close();
        assert(tree.openExisting());
        int64_t value = -1;
        assert(tree.search("existing", value) && value == 123);
    }
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("value", false, 4));
    table.pkColIndices = {0};
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "1"}, {"value", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "items", "value") == dbms::DBStatus::OK);
    assert(g_engine.createCompositeIndex(
               database, "items", {"id", "value"}, "both") == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddColumn(
               database, "items", dbms::makeIntColumn("extra", true, 4)) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableDropColumn(database, "items", "extra") ==
           dbms::DBStatus::OK);

    const std::string primaryKey = "1";
    const auto healthy = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(
            &g_engine, database, "items", "id", primaryKey));
    assert(healthy.ok && healthy.rows.size() == 1);

    const auto primaryPath = g_engine.getPKIndex(database, "items")->filePath();
    const auto secondaryPath =
        g_engine.getSecondaryIndex(database, "items", "value")->filePath();
    const auto compositePath =
        g_engine.getCompositeIndexTree(database, "items", "both")->filePath();
    assert(std::filesystem::remove(primaryPath));
    const bool primaryRejected = rejectsIndexScan(database, "id", primaryKey);
    assert(rejectsBitmapScan(database, primaryKey));
    const auto duplicate = g_engine.insert(
        database, "items", {{"id", "1"}, {"value", "8"}});
    const bool primaryStillMissing = !std::filesystem::exists(primaryPath);
    const auto heap = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::TableScanOp>(&g_engine, database, "items"));

    assert(std::filesystem::remove(secondaryPath));
    const bool secondaryRejected = rejectsIndexScan(database, "value", "7");
    const bool secondaryStillMissing = !std::filesystem::exists(secondaryPath);
    assert(std::filesystem::remove(compositePath));
    const bool compositeUnavailable =
        g_engine.getCompositeIndexTree(database, "items", "both") == nullptr;
    const bool compositeStillMissing = !std::filesystem::exists(compositePath);

    assert(primaryRejected && secondaryRejected && compositeUnavailable);
    assert(primaryStillMissing && secondaryStillMissing && compositeStillMissing);
    assert(duplicate != dbms::DBStatus::OK && heap.ok && heap.rows.size() == 1);
    assert(g_engine.insert(database, "items", {{"id", "2"}, {"value", "8"}}) ==
           dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());

    // Explicit maintenance must still be able to rebuild missing relations.
    assert(g_engine.reindex(database, "items") == dbms::DBStatus::OK);
    const auto primary = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(
            &g_engine, database, "items", "id", primaryKey));
    const auto secondary = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(
            &g_engine, database, "items", "value", "7"));
    assert(primary.ok && primary.rows.size() == 1);
    assert(secondary.ok && secondary.rows.size() == 1);
    assert(g_engine.getCompositeIndexTree(database, "items", "both"));

    // Check each secondary family independently so a missing primary cannot
    // hide missing-secondary failures during DML.
    for (const auto& path : {secondaryPath, compositePath}) {
        assert(std::filesystem::remove(path));
        if (path == secondaryPath) assert(rejectsBitmapScan(database, primaryKey));
        assert(g_engine.insert(
                   database, "items", {{"id", "2"}, {"value", "8"}}) ==
               dbms::DBStatus::IO_ERROR);
        assert(!std::filesystem::exists(path));
        const auto remaining = dbms::QueryPlanner::executePlanChecked(
            std::make_unique<dbms::TableScanOp>(&g_engine, database, "items"));
        assert(remaining.ok && remaining.rows.size() == 1);
        assert(g_engine.reindex(database, "items") == dbms::DBStatus::OK);
    }

    dbms::TableSchema toasted;
    toasted.tablename = "toasted";
    toasted.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    toasted.append(dbms::makeIntColumn("id", false, 4, true));
    toasted.append(dbms::makeTextColumn("payload", false));
    assert(g_engine.createTable(database, toasted) == dbms::DBStatus::OK);
    const auto toastId = g_engine.allocToastId(database, "toasted");
    assert(toastId != 0);
    assert(g_engine.writeToast(database, "toasted", toastId, "preserved chunks"));
    const auto toastPath = g_engine.toastIndexPath(database, "toasted");
    assert(std::filesystem::remove(toastPath));
    std::string toastData;
    assert(!g_engine.readToast(database, "toasted", toastId, 100, toastData));
    assert(!std::filesystem::exists(toastPath));

    dbms::TableSchema widened;
    widened.tablename = "widened";
    widened.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    widened.append(dbms::makeIntColumn("id", false, 4, true));
    widened.append(dbms::makeIntColumn("payload", false, 4));
    assert(g_engine.createTable(database, widened) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "widened", {{"id", "1"}, {"payload", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAlterColumnType(
               database, "widened", "payload",
               dbms::makeTextColumn("payload", false)) == dbms::DBStatus::OK);
    const auto widenedToastId = g_engine.allocToastId(database, "widened");
    assert(widenedToastId != 0);
    assert(g_engine.writeToast(database, "widened", widenedToastId, "new chunks"));
    assert(g_engine.readToast(database, "widened", widenedToastId, 100, toastData));
    assert(toastData == "new chunks");
    const auto widenedRows = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(
            &g_engine, database, "widened", "id", "1"));
    assert(widenedRows.ok && widenedRows.rows.size() == 1);
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[MISSING B-TREE FILE GUARD] passed" << std::endl;
}
