#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void cleanup(const std::string& database) {
    if (std::filesystem::exists(database)) {
        std::filesystem::remove_all(database);
    }
}

Session makeSession(const std::string& database) {
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    return session;
}

void testRenameMovesDeferrabilityMetadata() {
    const std::string database = testDbPath("constraint_rename_metadata");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE deferred_values ("
        "id INT PRIMARY KEY, tag VARCHAR(20), "
        "CONSTRAINT old_tag_key UNIQUE (tag) "
        "DEFERRABLE INITIALLY DEFERRED)",
        session));
    assert(g_engine.isConstraintCurrentlyDeferred(
        database, "deferred_values", "old_tag_key"));

    assert(g_engine.alterTableRenameConstraint(
               database, "deferred_values", "old_tag_key", "new_tag_key") ==
           dbms::DBStatus::OK);

    const auto params = g_engine.getStorageParams(database, "deferred_values");
    assert(params.count("constraint.old_tag_key.validated") == 0);
    assert(params.count("constraint.old_tag_key.not_valid") == 0);
    assert(params.count("constraint.old_tag_key.deferrable") == 0);
    assert(params.count("constraint.old_tag_key.initially_deferred") == 0);
    assert(params.at("constraint.new_tag_key.validated") == "1");
    assert(params.at("constraint.new_tag_key.not_valid") == "0");
    assert(params.at("constraint.new_tag_key.deferrable") == "1");
    assert(params.at("constraint.new_tag_key.initially_deferred") == "1");
    assert(g_engine.isConstraintCurrentlyDeferred(
        database, "deferred_values", "new_tag_key"));
    assert(!g_engine.isConstraintCurrentlyDeferred(
        database, "deferred_values", "old_tag_key"));

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "deferred_values",
                           {{"id", "1"}, {"tag", "duplicate"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "deferred_values",
                           {{"id", "2"}, {"tag", "duplicate"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::INVALID_VALUE);
    cleanup(database);
}

void testPrimaryKeyRenameUsesStoredName() {
    const std::string database = testDbPath("constraint_rename_primary");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE parent (id INT PRIMARY KEY)", session));
    assert(g_engine.alterTableRenameConstraint(
               database, "parent", "parent_pkey", "parent_identity") ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableDropConstraint(
               database, "parent", "parent_pkey") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.alterTableDropConstraint(
               database, "parent", "parent_identity") ==
           dbms::DBStatus::OK);
    cleanup(database);
}

void testRenameRejectsPrimaryKeyNameCollisionAndMissingNoop() {
    const std::string database = testDbPath("constraint_rename_collision");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE items (id INT PRIMARY KEY, qty INT)",
                           session));
    assert(g_engine.alterTableAddCheckConstraint(
               database, "items", "positive_qty", "qty > 0") ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableRenameConstraint(
               database, "items", "positive_qty", "items_pkey") ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(g_engine.alterTableRenameConstraint(
               database, "items", "missing_name", "missing_name") ==
           dbms::DBStatus::INVALID_VALUE);
    cleanup(database);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    testRenameMovesDeferrabilityMetadata();
    testPrimaryKeyRenameUsesStoredName();
    testRenameRejectsPrimaryKeyNameCollisionAndMissingNoop();
    std::cout << "[CONSTRAINT RENAME METADATA] all passed" << std::endl;
    return 0;
}
