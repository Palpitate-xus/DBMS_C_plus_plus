#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

std::string trimRight(const std::string& value) {
    const size_t end = value.find_last_not_of(" \t\n\r");
    return end == std::string::npos ? "" : value.substr(0, end + 1);
}

std::string childParentId(const std::string& database,
                          const std::string& table) {
    const auto rows = g_engine.query(database, table, {"=id 1"}, {"pid"});
    assert(rows.size() == 1);
    return trimRight(rows.front());
}

void createFixture(const std::string& database) {
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema parent;
    parent.tablename = "parent";
    parent.formatVersion = 2;
    parent.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, parent) == dbms::DBStatus::OK);

    dbms::TableSchema nullableChild;
    nullableChild.tablename = "a_nullable_child";
    nullableChild.formatVersion = 2;
    nullableChild.append(dbms::makeIntColumn("id", false, 4, true));
    nullableChild.append(dbms::makeIntColumn("pid", true, 4, false));
    assert(g_engine.createTable(database, nullableChild) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "a_nullable_child", "a_nullable_child_parent_fk",
               {"pid"}, "parent", {"id"}, "setnull", "restrict") ==
           dbms::DBStatus::OK);

    dbms::TableSchema requiredChild;
    requiredChild.tablename = "z_required_child";
    requiredChild.formatVersion = 2;
    requiredChild.append(dbms::makeIntColumn("id", false, 4, true));
    requiredChild.append(dbms::makeIntColumn("pid", false, 4, false));
    assert(g_engine.createTable(database, requiredChild) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "z_required_child", "z_required_child_parent_fk",
               {"pid"}, "parent", {"id"}, "setnull", "restrict") ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "parent", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "a_nullable_child", {{"id", "1"}, {"pid", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "z_required_child", {{"id", "1"}, {"pid", "1"}}) ==
           dbms::DBStatus::OK);
}

void assertFixtureUnchanged(const std::string& database) {
    assert(g_engine.query(database, "parent", {"=id 1"}, {"id"}).size() == 1);
    assert(childParentId(database, "a_nullable_child") == "1");
    assert(childParentId(database, "z_required_child") == "1");
}

void test_autocommit_delete_is_atomic() {
    const std::string database = testDbPath("delete_atomicity");
    cleanupTestDb("delete_atomicity");
    createFixture(database);

    assert(g_engine.remove(database, "parent", {"=id 1"}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);
    assert(!g_engine.inTransaction());
    assertFixtureUnchanged(database);

    cleanupTestDb("delete_atomicity");
    std::cout << "[DELETE ATOMICITY] failed referential actions rolled back OK"
              << std::endl;
}

void test_transactional_delete_is_statement_atomic() {
    const std::string database = testDbPath("delete_txn_atomicity");
    cleanupTestDb("delete_txn_atomicity");
    createFixture(database);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "parent", {{"id", "2"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.remove(database, "parent", {"=id 1"}) ==
           dbms::DBStatus::NULL_NOT_ALLOWED);
    assert(g_engine.inTransaction());
    assertFixtureUnchanged(database);
    assert(g_engine.query(database, "parent", {"=id 2"}, {"id"}).size() == 1);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(g_engine.query(database, "parent", {"=id 2"}, {"id"}).size() == 1);

    cleanupTestDb("delete_txn_atomicity");
    std::cout << "[DELETE ATOMICITY] failed transactional delete rolled back OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_autocommit_delete_is_atomic();
    test_transactional_delete_is_statement_atomic();
    return 0;
}
