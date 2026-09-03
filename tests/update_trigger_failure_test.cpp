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
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"value", "original"}}) ==
           dbms::DBStatus::OK);
}

void assertOriginalRow(const std::string& database) {
    assert(g_engine.query(database, "t", {"=value original"}, {"id"})
               .size() == 1);
    assert(g_engine.query(database, "t", {"=value changed"}, {"id"})
               .empty());
}

void test_before_trigger_failure_aborts_update() {
    const std::string database = testDbPath("update_before_trigger_failure");
    cleanupTestDb("update_before_trigger_failure");
    createFixture(database);
    assert(g_engine.createTrigger(database, {
               "fail_before", "before", "update", "t", "select fail", "",
               true, true, {}}) == dbms::DBStatus::OK);

    int calls = 0;
    // TriggerExecutor mirrors main::execute: true is the error result.
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++calls;
        return true;
    });
    const dbms::DBStatus status = g_engine.update(
        database, "t", {{"value", "changed"}}, {"=id 1"});
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(!g_engine.inTransaction());
    assertOriginalRow(database);

    cleanupTestDb("update_before_trigger_failure");
    std::cout << "[UPDATE TRIGGER FAILURE] BEFORE error aborted statement OK"
              << std::endl;
}

void test_after_row_trigger_failure_rolls_back_to_savepoint() {
    const std::string database = testDbPath("update_after_row_failure");
    cleanupTestDb("update_after_row_failure");
    createFixture(database);
    assert(g_engine.createTrigger(database, {
               "fail_after_row", "after", "update", "t", "select fail", "",
               true, true, {}}) == dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t", {{"id", "2"}, {"value", "survivor"}}) ==
           dbms::DBStatus::OK);
    int calls = 0;
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++calls;
        return true;
    });
    std::vector<std::map<std::string, std::string>> returned{
        {{"sentinel", "keep"}}};
    const dbms::DBStatus status = g_engine.update(
        database, "t", {{"value", "changed"}}, {"=id 1"}, &returned);
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(returned.size() == 1);
    assert(returned.front().at("sentinel") == "keep");
    assert(g_engine.inTransaction());
    assertOriginalRow(database);
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).size() == 1);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assertOriginalRow(database);
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).size() == 1);

    cleanupTestDb("update_after_row_failure");
    std::cout << "[UPDATE TRIGGER FAILURE] AFTER ROW error rolled back OK"
              << std::endl;
}

void test_after_statement_trigger_failure_rolls_back() {
    const std::string database = testDbPath("update_after_statement_failure");
    cleanupTestDb("update_after_statement_failure");
    createFixture(database);
    assert(g_engine.createTrigger(database, {
               "fail_after_statement", "after", "update", "t",
               "select fail", "", false, true, {}}) == dbms::DBStatus::OK);

    int calls = 0;
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++calls;
        return true;
    });
    const dbms::DBStatus status = g_engine.update(
        database, "t", {{"value", "changed"}}, {"=id 1"});
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(!g_engine.inTransaction());
    assertOriginalRow(database);

    cleanupTestDb("update_after_statement_failure");
    std::cout << "[UPDATE TRIGGER FAILURE] AFTER STATEMENT error rolled back OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_before_trigger_failure_aborts_update();
    test_after_row_trigger_failure_rolls_back_to_savepoint();
    test_after_statement_trigger_failure_rolls_back();
    finalCleanupTestData();
    return 0;
}
