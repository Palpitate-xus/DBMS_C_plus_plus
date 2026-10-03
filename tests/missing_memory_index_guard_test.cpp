#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

static dbms::PlanExecutionResult bitmap(const std::string& database,
                                        const std::string& table) {
    dbms::PlanContext context;
    context.dbname = database;
    context.tablename = table;
    context.selectCols = {"*"};
    context.conds = {{"=", "value", "7"}, {"=", "id", "1"}};
    return dbms::QueryPlanner::executePlanChecked(
        dbms::QueryPlanner::buildSelectPlan(&g_engine, context));
}

static void expectRejected(const std::string& database) {
    for (const bool disjunction : {false, true}) {
        bool rejected = false;
        try {
            dbms::PlanExecutionResult result;
            if (disjunction) {
                auto plan = std::make_unique<dbms::BitmapOrHeapScanOp>(
                    &g_engine, database, "items",
                    std::vector<std::vector<dbms::StorageEngine::Condition>>{
                        {{"=", "value", "7"}}, {{"=", "id", "2"}}});
                result = dbms::QueryPlanner::executePlanChecked(std::move(plan));
            } else {
                result = bitmap(database, "items");
            }
            std::cerr << "invalid memory index incorrectly returned "
                      << result.rows.size() << " rows, ok=" << result.ok << '\n';
        } catch (const dbms::DbError& error) {
            assert(error.sqlState() == "XX001");
            rejected = true;
        }
        assert(rejected);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    }
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    for (const bool bloom : {false, true}) {
        auto database = testDbPath(bloom ? "missing_bloom_guard" : "missing_hash_guard");
        assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
        dbms::TableSchema table;
        table.tablename = "items";
        table.append(dbms::makeIntColumn("id", false, 4));
        table.append(dbms::makeIntColumn("value", false, 4));
        assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
        for (const std::string id : {"1", "2"}) {
            assert(g_engine.insert(database, table.tablename,
                                  {{"id", id}, {"value", "7"}}) == dbms::DBStatus::OK);
        }
        const auto create = [&]() {
            return bloom ? g_engine.createBloomIndex(database, "items", "value")
                         : g_engine.createHashIndex(database, "items", "value");
        };
        assert(create() == dbms::DBStatus::OK);
        assert(g_engine.createIndex(database, "items", "id") == dbms::DBStatus::OK);
        auto result = bitmap(database, "items");
        assert(result.ok && result.rows.size() == 1);
        const auto file = bloom
            ? g_engine.getBloomIndex(database, "items", "value")->filePath().filename()
            : g_engine.getHashIndex(database, "items", "value")->filePath().filename();

        // The public database rename closes cached mappings, so the first
        // subsequent scan must reload this known index from its missing file.
        const auto renamed = database + "_renamed";
        assert(g_engine.renameDatabase(database, renamed) == dbms::DBStatus::OK);
        database = renamed;
        const auto path = std::filesystem::path(database) / file;
        assert(std::filesystem::remove(path));
        // The runtime getter must not turn a missing persisted index into a
        // usable empty map. Bitmap guards alone are insufficient because the
        // DML and ordinary equality paths share these getters.
        if (bloom) {
            assert(g_engine.getBloomIndex(database, "items", "value") == nullptr);
        } else {
            assert(g_engine.getHashIndex(database, "items", "value") == nullptr);
        }
        expectRejected(database);
        assert(!std::filesystem::exists(path));
        assert(g_engine.query(database, "items", {}, {"*"}).size() == 2);
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());

        // Explicit DROP/CREATE scans the preserved heap and is allowed to
        // publish a fresh complete mapping. Runtime reads may not do that.
        assert((bloom ? g_engine.dropBloomIndex(database, "items", "value")
                      : g_engine.dropHashIndex(database, "items", "value")) ==
               dbms::DBStatus::OK);
        assert(create() == dbms::DBStatus::OK);
        result = bitmap(database, "items");
        assert(result.ok && result.rows.size() == 1);
        // A cached complete RID set is not authority to ignore deletion or
        // atomic replacement of its backing relation.
        assert(std::filesystem::remove(path));
        if (bloom) {
            assert(g_engine.getBloomIndex(database, "items", "value") == nullptr);
        } else {
            assert(g_engine.getHashIndex(database, "items", "value") == nullptr);
        }
        expectRejected(database);
        assert(!std::filesystem::exists(path));
        assert(g_engine.query(database, "items", {}, {"*"}).size() == 2);

        // Rebuild explicitly, warm the cache, then atomically replace the
        // sidecar with a corrupt generation. The old in-memory RID map must
        // not survive the rename.
        assert((bloom ? g_engine.dropBloomIndex(database, "items", "value")
                      : g_engine.dropHashIndex(database, "items", "value")) ==
               dbms::DBStatus::OK);
        assert(create() == dbms::DBStatus::OK);
        result = bitmap(database, "items");
        assert(result.ok && result.rows.size() == 1);
        auto replacement = path;
        replacement += ".replacement";
        {
            std::ofstream out(replacement, std::ios::binary | std::ios::trunc);
            out << "corrupt replacement generation";
            assert(out.good());
        }
        std::filesystem::rename(replacement, path);
        if (bloom) {
            assert(g_engine.getBloomIndex(database, "items", "value") == nullptr);
        } else {
            assert(g_engine.getHashIndex(database, "items", "value") == nullptr);
        }
        expectRejected(database);
        assert(std::filesystem::exists(path));
        assert(g_engine.query(database, "items", {}, {"*"}).size() == 2);

        assert((bloom ? g_engine.dropBloomIndex(database, "items", "value")
                      : g_engine.dropHashIndex(database, "items", "value")) ==
               dbms::DBStatus::OK);
        assert(create() == dbms::DBStatus::OK);
        result = bitmap(database, "items");
        assert(result.ok && result.rows.size() == 1);
        {
            // Change the same inode in place. Size and nanosecond timestamps
            // are part of the cached generation, not just inode identity.
            std::ofstream out(path, std::ios::binary | std::ios::trunc);
            out << "truncated in-place generation";
            assert(out.good());
        }
        if (bloom) {
            assert(g_engine.getBloomIndex(database, "items", "value") == nullptr);
        } else {
            assert(g_engine.getHashIndex(database, "items", "value") == nullptr);
        }
        expectRejected(database);
        assert(std::filesystem::exists(path));
        assert(g_engine.query(database, "items", {}, {"*"}).size() == 2);
        assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    }
    std::cout << "[MISSING MEMORY INDEX GUARD] passed\n";
}
