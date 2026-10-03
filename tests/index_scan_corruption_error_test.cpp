#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <vector>

extern dbms::StorageEngine g_engine;

static void installCorruption(dbms::BPTree* tree, bool invalidRoot = false) {
    assert(tree);
    const auto path = tree->filePath();
    tree->close();
    std::vector<char> bytes(dbms::BP_PAGE_SIZE * 3, 0);
    const uint32_t root = 1;
    const uint32_t nextFree = 3;
    const uint16_t order = 2;
    std::memcpy(bytes.data(), &root, sizeof(root));
    std::memcpy(bytes.data() + sizeof(root), &nextFree, sizeof(nextFree));
    std::memcpy(bytes.data() + sizeof(root) + sizeof(nextFree),
                &order, sizeof(order));
    for (uint32_t page = 1; page <= 2; ++page) {
        const uint32_t child = page == 1 ? 2 : 1;
        std::memcpy(bytes.data() + page * dbms::BP_PAGE_SIZE + 3,
                    &child, sizeof(child));
    }
    if (invalidRoot) bytes[dbms::BP_PAGE_SIZE] = 2;
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    assert(output);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    assert(output);
    assert(tree->open() == !invalidRoot);
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "index_scan_corruption_error";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeIntColumn("value", false, 4));
    schema.pkColIndices = {0};
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "1"}, {"value", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "items", "value") == dbms::DBStatus::OK);
    installCorruption(g_engine.getPKIndex(database, "items"));
    installCorruption(g_engine.getSecondaryIndex(database, "items", "value"));

    const auto heap = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::TableScanOp>(&g_engine, database, "items"));
    assert(heap.ok && heap.rows.size() == 1);
    unsigned rejected = 0;
    for (unsigned path = 0; path < 4; ++path) {
        const std::string column = path % 2 == 0 ? "id" : "value";
        const std::string key = path % 2 == 0 ? "1" : "7";
        dbms::OpPtr plan;
        if (path < 2) {
            plan = std::make_unique<dbms::IndexScanOp>(
                &g_engine, database, "items", column, key);
        } else {
            const std::string otherColumn = path % 2 == 0 ? "value" : "id";
            const std::string otherKey = path % 2 == 0 ? "7" : "1";
            plan = std::make_unique<dbms::BitmapHeapScanOp>(
                &g_engine, database, "items",
                std::vector<dbms::StorageEngine::Condition>{
                    {"=", column, key}, {"=", otherColumn, otherKey}});
        }
        try {
            (void)dbms::QueryPlanner::executePlanChecked(std::move(plan));
        } catch (const dbms::DbError& error) {
            assert(error.sqlState() == "XX001");
            ++rejected;
        }
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    }
    assert(rejected == 4);
    bool storageQueryRejected = false;
    try {
        (void)g_engine.query(database, "items", {"=id 1"}, {"id"}, {});
    } catch (const dbms::DbError& error) {
        assert(error.sqlState() == "XX001");
        storageQueryRejected = true;
    }
    assert(storageQueryRejected);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    installCorruption(g_engine.getPKIndex(database, "items"), true);
    installCorruption(g_engine.getSecondaryIndex(database, "items", "value"), true);
    for (const std::string column : {"id", "value"}) {
        bool failed = false;
        try {
            (void)dbms::QueryPlanner::executePlanChecked(
                std::make_unique<dbms::IndexScanOp>(
                    &g_engine, database, "items", column,
                    column == "id" ? "1" : "7"));
        } catch (const dbms::DbError& error) {
            assert(error.sqlState() == "XX001");
            failed = true;
        }
        assert(failed);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    }
    assert(g_engine.update(database, "items", {{"value", "9"}}, {}) ==
           dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    const auto afterFailure = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::TableScanOp>(&g_engine, database, "items"));
    assert(afterFailure.ok && afterFailure.rows == heap.rows);
    // The rejected write must not claim to repair a pre-existing corrupt
    // B-tree. Reindex is the explicit recovery operation; after it succeeds,
    // both access paths and writes should be usable again.
    assert(g_engine.reindex(database, "items") == dbms::DBStatus::OK);
    const auto repairedPrimary = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(
            &g_engine, database, "items", "id", "1"));
    assert(repairedPrimary.ok && repairedPrimary.rows == heap.rows);
    const auto repairedSecondary = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(
            &g_engine, database, "items", "value", "7"));
    assert(repairedSecondary.ok && repairedSecondary.rows == heap.rows);
    assert(g_engine.update(database, "items", {{"value", "9"}}, {}) ==
           dbms::DBStatus::OK);
    const auto updated = g_engine.query(
        database, "items", {"=id 1"}, {"value"});
    assert(updated == std::vector<std::string>{"9 "});
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[INDEX SCAN CORRUPTION ERROR] passed" << std::endl;
}
