#include "access/BPTree.h"
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

int64_t createFixture(const std::string& database) {
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
    int64_t rid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", rid));
    return rid;
}

void assertOriginalRow(const std::string& database, int64_t rid) {
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "t", {"=value original"}, {"id"})
               .size() == 1);
    int64_t indexedRid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", indexedRid));
    assert(indexedRid == rid);
}

void runAutocommitFailure(const std::string& testName,
                          const std::string& timing, bool forEachRow) {
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    const int64_t rid = createFixture(database);
    assert(g_engine.createTrigger(database, {
               "fail_delete", timing, "delete", "t", "select fail", "",
               forEachRow, true, {}}) == dbms::DBStatus::OK);

    int calls = 0;
    // TriggerExecutor mirrors main::execute: true is the error result.
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++calls;
        return true;
    });
    std::vector<std::map<std::string, std::string>> returned{
        {{"sentinel", "keep"}}};
    const dbms::DBStatus status =
        g_engine.remove(database, "t", {"=id 1"}, &returned);
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(returned.size() == 1);
    assert(returned.front().at("sentinel") == "keep");
    assert(!g_engine.inTransaction());
    assertOriginalRow(database, rid);

    cleanupTestDb(testName);
}

void test_all_delete_trigger_kinds_abort() {
    runAutocommitFailure(
        "delete_before_row_trigger_failure", "before", true);
    runAutocommitFailure(
        "delete_before_statement_trigger_failure", "before", false);
    runAutocommitFailure(
        "delete_after_row_trigger_failure", "after", true);
    runAutocommitFailure(
        "delete_after_statement_trigger_failure", "after", false);
    std::cout << "[DELETE TRIGGER FAILURE] all trigger kinds abort OK"
              << std::endl;
}

void test_after_failure_rolls_back_to_statement_savepoint() {
    const std::string testName = "delete_trigger_savepoint_failure";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    const int64_t rid = createFixture(database);
    assert(g_engine.createTrigger(database, {
               "fail_delete", "after", "delete", "t", "select fail", "",
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
    const dbms::DBStatus status =
        g_engine.remove(database, "t", {"=id 1"});
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(g_engine.inTransaction());
    assertOriginalRow(database, rid);
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).size() == 1);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assertOriginalRow(database, rid);
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).size() == 1);

    cleanupTestDb(testName);
    std::cout << "[DELETE TRIGGER FAILURE] statement savepoint rollback OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_all_delete_trigger_kinds_abort();
    test_after_failure_rolls_back_to_statement_savepoint();
    finalCleanupTestData();
    return 0;
}
