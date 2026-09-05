#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void beginCleanTransaction(const std::string& database) {
    if (g_engine.inTransaction()) {
        assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    }
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
}

void test_rollback_discards_later_savepoints(const std::string& database) {
    beginCleanTransaction(database);
    assert(g_engine.savepoint("first") == dbms::DBStatus::OK);
    assert(g_engine.savepoint("second") == dbms::DBStatus::OK);

    // Savepoint order is independent of whether any rows were changed between
    // the two declarations.
    assert(g_engine.rollbackToSavepoint("first") == dbms::DBStatus::OK);
    assert(g_engine.rollbackToSavepoint("second") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.releaseSavepoint("first") == dbms::DBStatus::OK);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
}

void test_release_discards_target_and_later(const std::string& database) {
    beginCleanTransaction(database);
    assert(g_engine.savepoint("first") == dbms::DBStatus::OK);
    assert(g_engine.savepoint("second") == dbms::DBStatus::OK);

    assert(g_engine.releaseSavepoint("first") == dbms::DBStatus::OK);
    assert(g_engine.rollbackToSavepoint("first") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.rollbackToSavepoint("second") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
}

void test_duplicate_names_form_a_stack(const std::string& database) {
    beginCleanTransaction(database);
    assert(g_engine.savepoint("same") == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.savepoint("same") == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "items", {{"id", "2"}}) ==
           dbms::DBStatus::OK);

    // Releasing the newest same-named savepoint reveals the older one.
    assert(g_engine.releaseSavepoint("same") == dbms::DBStatus::OK);
    assert(g_engine.rollbackToSavepoint("same") == dbms::DBStatus::OK);
    assert(g_engine.query(database, "items", {}, {"id"}).empty());
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "savepoint_stack";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    test_rollback_discards_later_savepoints(database);
    test_release_discards_target_and_later(database);
    test_duplicate_names_form_a_stack(database);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SAVEPOINT STACK] ordering and duplicate names OK"
              << std::endl;
    return 0;
}
