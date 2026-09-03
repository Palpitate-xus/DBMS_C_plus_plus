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

bool hasPrimaryKey(const dbms::TableSchema& table) {
    if (!table.pkColIndices.empty()) return true;
    for (size_t column = 0; column < table.len; ++column) {
        if (table.cols[column].isPrimaryKey) return true;
    }
    return false;
}

void testUnknownNameCannotDropDefaultPrimaryKey() {
    const std::string database = testDbPath("pk_constraint_default");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(20))",
        session));
    assert(g_engine.alterTableDropConstraint(
               database, "accounts", "missing_constraint") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(hasPrimaryKey(g_engine.getTableSchema(database, "accounts")));

    assert(g_engine.alterTableDropConstraint(
               database, "accounts", "accounts_pkey") ==
           dbms::DBStatus::OK);
    assert(!hasPrimaryKey(g_engine.getTableSchema(database, "accounts")));
    cleanup(database);
}

void testExplicitCreateNameMustMatch() {
    const std::string database = testDbPath("pk_constraint_create_name");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE inventory (id INT, "
        "CONSTRAINT inventory_identity PRIMARY KEY (id))",
        session));
    assert(g_engine.alterTableDropConstraint(
               database, "inventory", "inventory_pkey") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(hasPrimaryKey(g_engine.getTableSchema(database, "inventory")));
    assert(g_engine.alterTableDropConstraint(
               database, "inventory", "inventory_identity") ==
           dbms::DBStatus::OK);
    cleanup(database);
}

void testExplicitAlterNameMustMatch() {
    const std::string database = testDbPath("pk_constraint_alter_name");
    cleanup(database);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session session = makeSession(database);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE metrics (id INT, value INT)", session));
    assert(g_engine.alterTableAddPrimaryKey(
               database, "metrics", "metrics_identity", {"id"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableDropConstraint(
               database, "metrics", "not_metrics_identity") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(hasPrimaryKey(g_engine.getTableSchema(database, "metrics")));
    assert(g_engine.alterTableDropConstraint(
               database, "metrics", "metrics_identity") ==
           dbms::DBStatus::OK);
    cleanup(database);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    testUnknownNameCannotDropDefaultPrimaryKey();
    testExplicitCreateNameMustMatch();
    testExplicitAlterNameMustMatch();
    std::cout << "[PK CONSTRAINT NAME] all passed" << std::endl;
    return 0;
}
