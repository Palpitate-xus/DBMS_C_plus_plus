#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "for_update_no_gap_lock";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "1"}}) == DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "10"}}) == DBStatus::OK);
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    const auto before = g_engine.getLockManager().captureCheckpoint();
    assert(before.gapCounts.empty() && before.rowLocks.empty());
    assert(g_engine.query(database, "items", {"=id 1"}, {"id"}, {}, true) ==
           std::vector<std::string>{"1 "});
    const auto after = g_engine.getLockManager().captureCheckpoint();
    assert(after.gapCounts.empty());
    assert(after.rowLocks.size() == 1 && after.rowModes.size() == 1);
    assert(after.rowModes.begin()->second == dbms::LockManager::LockMode::Exclusive);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(g_engine.getLockManager().captureCheckpoint().rowLocks.empty());
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[FOR UPDATE NO GAP LOCK] passed\n";
}
