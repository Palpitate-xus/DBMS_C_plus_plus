#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "statement_savepoint_namespace";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);

    std::string internal = "__dbms_dml_statement_0";
    assert(g_engine.createStatementSavepoint(internal) == DBStatus::INVALID_VALUE);
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    // Internal-looking names are still legal user savepoints. An internal
    // boundary must never hide one, even when a caller requests that name.
    assert(g_engine.savepoint(internal) == DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "1"}}) == DBStatus::OK);
    assert(g_engine.createStatementSavepoint(internal) == DBStatus::OK);
    assert(internal != "__dbms_dml_statement_0");
    assert(g_engine.insert(database, "items", {{"id", "2"}}) == DBStatus::OK);
    assert(g_engine.rollbackToSavepoint(internal) == DBStatus::OK);
    assert(g_engine.query(database, "items", {}, {"id"}).size() == 1);
    assert(g_engine.releaseSavepoint(internal) == DBStatus::OK);
    assert(g_engine.rollbackToSavepoint("__dbms_dml_statement_0") == DBStatus::OK);
    assert(g_engine.query(database, "items", {}, {"id"}).empty());

    std::string repeated = "boundary";
    assert(g_engine.createStatementSavepoint(repeated) == DBStatus::OK);
    std::string nested = repeated;
    assert(g_engine.createStatementSavepoint(nested) == DBStatus::OK);
    assert(nested != repeated);
    assert(g_engine.releaseSavepoint(nested) == DBStatus::OK);
    assert(g_engine.releaseSavepoint(repeated) == DBStatus::OK);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[STATEMENT SAVEPOINT NAMESPACE] passed\n";
}
