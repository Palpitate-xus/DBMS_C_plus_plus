#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "query_native_error_unlock", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    TableSchema table;
    table.tablename = "lock_source";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(db, table) == DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"id", "1"}}) == DBStatus::OK);
    auto& locks = g_engine.getLockManager();
    const auto exercise = [&] {
        const auto before = locks.captureCheckpoint().tableCounts;
        for (const auto& [condition, state] : std::vector<std::pair<std::string, std::string>>{
            {"typedexpr missing=1", "42703"},
            {"typedexpr CAST('bad' AS integer)=1", "22P02"},
            {"typedexpr id/0=1", "22012"}
        }) {
            bool precise = false;
            try {
                (void)g_engine.query(db, table.tablename, {condition}, {"id"});
            } catch (const std::exception& error) {
                precise = std::string(error.what()).find("SQLSTATE " + state) != std::string::npos;
            }
            assert(precise);
            const auto after = locks.captureCheckpoint().tableCounts;
            std::cout << "query lock counts before=" << before.size() << " after=" << after.size() << std::endl;
            assert(after == before);
            assert(g_engine.query(db, table.tablename, {}, {"id"}).size() == 1);
            assert(locks.captureCheckpoint().tableCounts == before);
        }
    };
    exercise();
    // The guard releases this query's one acquisition, never its caller's
    // already-owned table lock (and never all locks in the transaction).
    assert(locks.lockShared(table.tablename));
    exercise();
    locks.unlock(table.tablename);
    assert(locks.captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[NATIVE QUERY ERROR UNLOCK] exact errors and inherited lock counts preserved\n";
}
