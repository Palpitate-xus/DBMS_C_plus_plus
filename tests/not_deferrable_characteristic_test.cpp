#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "not_deferrable_characteristic";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    for (const bool rollbackChild : {false, true}) {
        assert(g_engine.canSetTransactionDeferrable());
        assert(g_engine.beginTransaction(database) == DBStatus::OK);
        assert(g_engine.canSetTransactionDeferrable());
        assert(g_engine.savepoint("child") == DBStatus::OK);
        assert(!g_engine.canSetTransactionDeferrable());
        if (rollbackChild) assert(g_engine.rollbackToSavepoint("child") == DBStatus::OK);
        assert(!g_engine.canSetTransactionDeferrable());
        assert(g_engine.releaseSavepoint("child") == DBStatus::OK);
        assert(g_engine.canSetTransactionDeferrable());
        assert(g_engine.savepoint("child") == DBStatus::OK);
        g_engine.noteQuerySnapshot();
        if (rollbackChild) assert(g_engine.rollbackToSavepoint("child") == DBStatus::OK);
        assert(g_engine.releaseSavepoint("child") == DBStatus::OK);
        assert(!g_engine.canSetTransactionDeferrable());
        assert(g_engine.rollbackTransaction() == DBStatus::OK);
        assert(g_engine.canSetTransactionDeferrable());
    }
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[NOT DEFERRABLE CHARACTERISTIC] passed\n";
}
