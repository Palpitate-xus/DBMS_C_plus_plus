#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "savepoint_read_only";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(!g_engine.isReadOnly());
    assert(g_engine.savepoint("outer") == DBStatus::OK);
    assert(g_engine.setReadOnly(true));
    assert(!g_engine.setReadOnly(false));
    assert(g_engine.savepoint("inner") == DBStatus::OK);
    assert(g_engine.rollbackToSavepoint("inner") == DBStatus::OK);
    assert(g_engine.isReadOnly());
    assert(g_engine.releaseSavepoint("inner") == DBStatus::OK);
    assert(g_engine.isReadOnly());
    assert(g_engine.rollbackToSavepoint("outer") == DBStatus::OK);
    assert(!g_engine.isReadOnly());
    assert(g_engine.setReadOnly(true));
    assert(g_engine.releaseSavepoint("outer") == DBStatus::OK);
    assert(!g_engine.isReadOnly());

    assert(g_engine.savepoint("duplicate") == DBStatus::OK);
    assert(g_engine.setReadOnly(true));
    assert(g_engine.savepoint("duplicate") == DBStatus::OK);
    assert(g_engine.releaseSavepoint("duplicate") == DBStatus::OK);
    assert(g_engine.isReadOnly());
    assert(g_engine.releaseSavepoint("missing") == DBStatus::INVALID_VALUE);
    assert(g_engine.isReadOnly());
    assert(g_engine.rollbackToSavepoint("duplicate") == DBStatus::OK);
    assert(!g_engine.isReadOnly());
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(!g_engine.isReadOnly());

    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.setReadOnly(true));
    assert(g_engine.savepoint("readonly") == DBStatus::OK);
    assert(!g_engine.setReadOnly(false));
    assert(g_engine.rollbackToSavepoint("readonly") == DBStatus::OK);
    assert(g_engine.isReadOnly());
    assert(g_engine.releaseSavepoint("readonly") == DBStatus::OK);
    assert(g_engine.isReadOnly());
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[SAVEPOINT READ ONLY] passed\n";
}
