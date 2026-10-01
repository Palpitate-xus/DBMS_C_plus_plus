#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    using dbms::IsolationLevel;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "isolation_idempotent";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "isolation_values";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    assert(g_engine.insertRow(database, table.tablename, {{"id", "0"}}) == DBStatus::OK);
    for (const auto level : {IsolationLevel::READ_UNCOMMITTED, IsolationLevel::READ_COMMITTED,
                             IsolationLevel::REPEATABLE_READ, IsolationLevel::SERIALIZABLE}) {
        const auto other = level == IsolationLevel::READ_COMMITTED
            ? IsolationLevel::SERIALIZABLE : IsolationLevel::READ_COMMITTED;
        assert(g_engine.setIsolationLevel(level));
        assert(g_engine.beginTransaction(database) == DBStatus::OK);
        size_t count = 0;
        assert(g_engine.forEachRow(database, table.tablename,
            [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
        assert(count == 1);
        assert(g_engine.setIsolationLevel(level));
        assert(g_engine.getIsolationLevel() == level);
        assert(g_engine.savepoint("same_level") == DBStatus::OK);
        assert(g_engine.setIsolationLevel(level));
        assert(!g_engine.setIsolationLevel(other));
        assert(g_engine.rollbackToSavepoint("same_level") == DBStatus::OK);
        assert(g_engine.getIsolationLevel() == level);
        assert(g_engine.rollbackTransaction() == DBStatus::OK);

        assert(g_engine.setIsolationLevel(level));
        assert(g_engine.beginTransaction(database) == DBStatus::OK);
        assert(g_engine.savepoint("child") == DBStatus::OK);
        assert(!g_engine.setIsolationLevel(other));
        assert(g_engine.setIsolationLevel(level));
        assert(g_engine.releaseSavepoint("child") == DBStatus::OK);
        assert(g_engine.setIsolationLevel(other));
        assert(g_engine.getIsolationLevel() == other);
        assert(g_engine.rollbackTransaction() == DBStatus::OK);
    }
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[ISOLATION IDEMPOTENT] passed\n";
}
