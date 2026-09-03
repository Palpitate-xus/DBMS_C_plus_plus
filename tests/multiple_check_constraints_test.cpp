#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    cleanupAllTestData();

    const std::string testName = "multiple_check_constraints";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE checked_values ("
        "id INT PRIMARY KEY, "
        "value INT CHECK (value > 0) CHECK (value < 1000), "
        "CONSTRAINT checked_values_floor CHECK (value >= 10), "
        "CONSTRAINT checked_values_ceiling CHECK (value <= 900))",
        session));

    assert(g_engine.insert(database, "checked_values",
                           {{"id", "1"}, {"value", "100"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "2"}, {"value", "0"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "3"}, {"value", "1001"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "4"}, {"value", "5"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "5"}, {"value", "950"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.update(database, "checked_values", {{"value", "950"}},
                           {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);

    // Reload from disk before exercising constraint identity operations.
    {
        dbms::StorageEngine reloaded;
        assert(reloaded.insert(database, "checked_values",
                               {{"id", "6"}, {"value", "950"}}) ==
               dbms::DBStatus::INVALID_VALUE);
    }

    assert(!ddl.executeSql(
        "CREATE TABLE checked_copy "
        "(LIKE checked_values INCLUDING CONSTRAINTS)",
        session));
    assert(g_engine.insert(database, "checked_copy",
                           {{"id", "1"}, {"value", "950"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "checked_copy",
                           {{"id", "2"}, {"value", "1001"}}) ==
           dbms::DBStatus::INVALID_VALUE);

    assert(!ddl.executeSql(
        "ALTER TABLE checked_values DROP CONSTRAINT checked_values_floor",
        session));
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "7"}, {"value", "5"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "8"}, {"value", "950"}}) ==
           dbms::DBStatus::INVALID_VALUE);

    assert(!ddl.executeSql(
        "ALTER TABLE checked_values RENAME CONSTRAINT "
        "checked_values_ceiling TO checked_values_cap",
        session));
    assert(!ddl.executeSql(
        "ALTER TABLE checked_values DROP CONSTRAINT checked_values_cap",
        session));
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "9"}, {"value", "950"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "10"}, {"value", "1001"}}) ==
           dbms::DBStatus::INVALID_VALUE);

    // Adding another CHECK to an already constrained column must append it,
    // not overwrite one of the existing expressions.
    assert(!ddl.executeSql(
        "ALTER TABLE checked_values ADD CONSTRAINT checked_values_not_500 "
        "CHECK (value <> 500)",
        session));
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "11"}, {"value", "500"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "12"}, {"value", "1001"}}) ==
           dbms::DBStatus::INVALID_VALUE);

    assert(!ddl.executeSql(
        "ALTER TABLE checked_values ALTER CONSTRAINT checked_values_not_500 "
        "DEFERRABLE INITIALLY DEFERRED",
        session));
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "16"}, {"value", "500"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.update(database, "checked_values", {{"value", "500"}},
                           {"=id 1"}) == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "13"}, {"value", "500"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::INVALID_VALUE);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "checked_values", {{"value", "500"}},
                           {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::INVALID_VALUE);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "15"}, {"value", "500"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("multiple_check_invalid_prepare") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(!g_engine.inTransaction());

    assert(!ddl.executeSql(
        "ALTER TABLE checked_values DROP CONSTRAINT checked_values_not_500",
        session));
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "14"}, {"value", "500"}}) ==
           dbms::DBStatus::OK);

    // Deferrability declared directly on ALTER TABLE ADD CONSTRAINT must be
    // applied to the stored CHECK, not only to side metadata.
    assert(!ddl.executeSql(
        "ALTER TABLE checked_values ADD CONSTRAINT checked_values_not_600 "
        "CHECK (value <> 600) DEFERRABLE INITIALLY DEFERRED",
        session));
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "17"}, {"value", "600"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "checked_values",
                           {{"id", "18"}, {"value", "600"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::INVALID_VALUE);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[MULTIPLE CHECK] all constraints retained independently OK"
              << std::endl;
    return 0;
}
