#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <map>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

void createFixture(const std::string& database) {
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("value", false, 40));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.createTrigger(database, {
               "fail_before_insert", "before", "insert", "t",
               "select fail", "", true, true, {}}) == dbms::DBStatus::OK);
}

void test_autocommit_before_failure_writes_nothing() {
    const std::string database = testDbPath("insert_before_failure");
    cleanupTestDb("insert_before_failure");
    createFixture(database);

    int calls = 0;
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++calls;
        return true;
    });
    std::vector<std::map<std::string, std::string>> returned{
        {{"sentinel", "keep"}}};
    const dbms::DBStatus status = g_engine.insert(
        database, "t", {{"id", "1"}, {"value", "new"}}, &returned);
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(returned.size() == 1);
    assert(returned.front().at("sentinel") == "keep");
    assert(!g_engine.inTransaction());
    assert(g_engine.query(database, "t", {}, {"id"}).empty());

    cleanupTestDb("insert_before_failure");
    std::cout << "[INSERT TRIGGER FAILURE] autocommit BEFORE error aborted OK"
              << std::endl;
}

void test_explicit_transaction_survives_before_failure() {
    const std::string database = testDbPath("insert_before_failure_txn");
    cleanupTestDb("insert_before_failure_txn");
    createFixture(database);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t", {{"id", "2"}, {"value", "survivor"}}) ==
           dbms::DBStatus::OK);
    int calls = 0;
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++calls;
        return true;
    });
    const dbms::DBStatus status = g_engine.insert(
        database, "t", {{"id", "1"}, {"value", "new"}});
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(g_engine.inTransaction());
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).empty());
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).size() == 1);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).size() == 1);

    cleanupTestDb("insert_before_failure_txn");
    std::cout << "[INSERT TRIGGER FAILURE] explicit transaction survived OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_autocommit_before_failure_writes_nothing();
    test_explicit_transaction_survives_before_failure();
    finalCleanupTestData();
    return 0;
}
