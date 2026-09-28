#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <atomic>
#include <cassert>
#include <memory>
#include <thread>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "execution_plan_relation_lock";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "1"}}) == dbms::DBStatus::OK);

    auto& locks = g_engine.getLockManager();
    locks.setResourceNamespace(database);
    assert(locks.lockMetadata("items"));
    std::atomic<int> rejected{0};
    std::atomic<bool> leaked{false};
    std::thread reader([&] {
        locks.setLockTimeout(50);
        for (int path = 0; path < 6; ++path) {
            dbms::OpPtr plan;
            if (path == 0) {
                plan = std::make_unique<dbms::TableScanOp>(
                    &g_engine, database, "items");
            } else if (path == 1) {
                plan = std::make_unique<dbms::IndexScanOp>(
                    &g_engine, database, "items", "id", "1");
            } else if (path == 2) {
                plan = std::make_unique<dbms::ParallelTableScanOp>(
                    &g_engine, database, "items", 2);
            } else if (path == 3) {
                plan = std::make_unique<dbms::GiSTScanOp>(
                    &g_engine, database, "items",
                    std::vector<dbms::StorageEngine::Condition>{});
            } else if (path == 4) {
                plan = std::make_unique<dbms::BitmapHeapScanOp>(
                    &g_engine, database, "items",
                    std::vector<dbms::StorageEngine::Condition>{});
            } else {
                plan = std::make_unique<dbms::BitmapOrHeapScanOp>(
                    &g_engine, database, "items",
                    std::vector<std::vector<dbms::StorageEngine::Condition>>{{}, {}});
            }
            try {
                const auto result = dbms::QueryPlanner::executePlanChecked(std::move(plan));
                if (!result.ok && result.error.find("SQLSTATE 55P03") !=
                                      std::string::npos) ++rejected;
            } catch (const dbms::DbError& error) {
                if (error.sqlState() == "55P03") ++rejected;
            }
            if (!locks.captureCheckpoint().tableCounts.empty()) leaked = true;
        }
    });
    reader.join();
    locks.unlock("items");
    assert(rejected == 6);
    assert(!leaked);

    auto seq = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::TableScanOp>(&g_engine, database, "items"));
    auto index = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::IndexScanOp>(
            &g_engine, database, "items", "id", "1"));
    auto parallel = dbms::QueryPlanner::executePlanChecked(
        std::make_unique<dbms::ParallelTableScanOp>(
            &g_engine, database, "items", 2));
    assert(seq.ok && seq.rows.size() == 1);
    assert(index.ok && index.rows.size() == 1);
    assert(parallel.ok && parallel.rows.size() == 1);
    cleanupTestDb(testName);
}
