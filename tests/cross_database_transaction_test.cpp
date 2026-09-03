#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void createItemsTable(const std::string& database) {
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = 2;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
}

bool hasId(const std::string& database, const std::string& id) {
    return g_engine.query(database, "items", {"=id " + id}, {"id"})
               .size() == 1;
}

void test_dml_cannot_escape_transaction_database() {
    const std::string firstName = "cross_database_transaction_a";
    const std::string secondName = "cross_database_transaction_b";
    const std::string first = testDbPath(firstName);
    const std::string second = testDbPath(secondName);
    cleanupTestDb(firstName);
    cleanupTestDb(secondName);
    assert(g_engine.createDatabase(first, "utf8") == dbms::DBStatus::OK);
    assert(g_engine.createDatabase(second, "utf8") == dbms::DBStatus::OK);
    createItemsTable(first);
    createItemsTable(second);
    assert(g_engine.insert(second, "items", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(second, "items", {{"id", "2"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(first) == dbms::DBStatus::OK);
    assert(g_engine.insert(first, "items", {{"id", "7"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.insert(second, "items", {{"id", "3"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.inTransaction());
    assert(!hasId(second, "3"));

    assert(g_engine.update(
               second, "items", {{"id", "10"}}, {"=id 1"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.inTransaction());
    assert(hasId(second, "1"));
    assert(!hasId(second, "10"));

    assert(g_engine.remove(second, "items", {"=id 2"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.inTransaction());
    assert(hasId(second, "2"));

    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(hasId(first, "7"));
    assert(hasId(second, "1"));
    assert(hasId(second, "2"));
    assert(!hasId(second, "3"));

    // Normal writes to the second database remain available after the first
    // database's transaction has ended.
    assert(g_engine.insert(second, "items", {{"id", "3"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(
               second, "items", {{"id", "4"}}, {"=id 3"}) ==
           dbms::DBStatus::OK);
    assert(hasId(second, "4"));
    assert(g_engine.remove(second, "items", {"=id 4"}) ==
           dbms::DBStatus::OK);
    assert(!hasId(second, "4"));

    cleanupTestDb(firstName);
    cleanupTestDb(secondName);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_dml_cannot_escape_transaction_database();
    finalCleanupTestData();
    std::cout << "[CROSS DATABASE TRANSACTION] DML stayed in transaction database OK"
              << std::endl;
    return 0;
}
