#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <atomic>
#include <cassert>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "subquery_relation_lock";
    cleanupTestDb(testName);
    const std::string database = testDbPath(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    for (const std::string name : {"outer_rows", "inner_rows"}) {
        dbms::TableSchema schema;
        schema.tablename = name;
        schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        schema.append(dbms::makeIntColumn("id", false, 4, true));
        assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
        assert(g_engine.insert(database, name, {{"id", "1"}}) == dbms::DBStatus::OK);
    }

    auto& locks = g_engine.getLockManager();
    for (const std::string kind : {"exists_sub", "subquery"}) {
        dbms::StorageEngine::SelectExpr expression;
        expression.displayName = "subquery_value";
        expression.isScalar = true;
        expression.funcName = kind;
        expression.funcArgs = {"select id from inner_rows where id = 1"};

        // A DDL holder owns the inner relation before the reader starts.
        // A page lock alone is insufficient to honor this metadata lock.
        locks.setResourceNamespace(database);
        assert(locks.lockMetadata("inner_rows"));
        std::atomic<int> rejected{0};
        std::atomic<bool> released{true};
        std::thread reader([&] {
            locks.setLockTimeout(50);
            for (bool transactional : {false, true}) {
                if (transactional) {
                    assert(g_engine.beginTransaction(database, false) == dbms::DBStatus::OK);
                }
                try {
                    (void)g_engine.queryExpr(database, "outer_rows", {}, {expression});
                } catch (const std::runtime_error& error) {
                    if (std::string(error.what()).find("SQLSTATE 55P03") !=
                        std::string::npos) ++rejected;
                }
                if (!locks.captureCheckpoint().tableCounts.empty()) released = false;
                if (transactional) {
                    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
                }
            }
        });
        reader.join();
        locks.unlock("inner_rows");
        assert(rejected.load() == 2);
        assert(released.load());

        const auto rows = g_engine.queryExpr(database, "outer_rows", {}, {expression});
        assert((rows == std::vector<std::string>{kind == "exists_sub" ? "t " : "1 "}));
        assert(locks.captureCheckpoint().tableCounts.empty());

        expression.funcArgs = {
            "select id from inner_rows where unavailable_function(id) = 1"};
        bool invalidRejected = false;
        try {
            (void)g_engine.queryExpr(database, "outer_rows", {}, {expression});
        } catch (const std::runtime_error&) {
            invalidRejected = true;
        }
        assert(invalidRejected);
        assert(locks.captureCheckpoint().tableCounts.empty());
    }

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SUBQUERY RELATION LOCK] passed\n";
}
