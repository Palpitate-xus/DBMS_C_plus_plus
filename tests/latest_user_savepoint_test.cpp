#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "latest_user_savepoint";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    assert(!g_engine.latestUserSavepoint());
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    std::string internal = "internal";
    assert(g_engine.createStatementSavepoint(internal) == DBStatus::OK);
    assert(!g_engine.latestUserSavepoint());
    assert(g_engine.savepoint("__dbms_dml_statement_0") == DBStatus::OK);
    assert(g_engine.latestUserSavepoint() == "__dbms_dml_statement_0");
    std::string collision = "__dbms_dml_statement_0";
    assert(g_engine.createStatementSavepoint(collision) == DBStatus::OK);
    assert(collision != "__dbms_dml_statement_0");
    assert(g_engine.latestUserSavepoint() == "__dbms_dml_statement_0");
    assert(g_engine.savepoint("nested") == DBStatus::OK);
    assert(g_engine.latestUserSavepoint() == "nested");
    assert(g_engine.releaseSavepoint("nested") == DBStatus::OK);
    assert(g_engine.latestUserSavepoint() == "__dbms_dml_statement_0");
    assert(g_engine.rollbackToSavepoint("__dbms_dml_statement_0") == DBStatus::OK);
    assert(g_engine.latestUserSavepoint() == "__dbms_dml_statement_0");
    assert(g_engine.releaseSavepoint("__dbms_dml_statement_0") == DBStatus::OK);
    assert(!g_engine.latestUserSavepoint());
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(!g_engine.latestUserSavepoint());
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[LATEST USER SAVEPOINT] passed\n";
}
