#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
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

void createFixture(const std::string& database, bool forEachRow) {
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("value", false, 40));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "t", "value") ==
           dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(database, "t", "value") ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(database, "t", "value") ==
           dbms::DBStatus::OK);
    assert(g_engine.createTrigger(database, {
               "fail_after_insert", "after", "insert", "t", "select fail",
               "", forEachRow, true, {}}) == dbms::DBStatus::OK);
}

void assertMissing(const std::string& database, const std::string& id,
                   const std::string& value) {
    int64_t ignored = -1;
    assert(!g_engine.getPKIndex(database, "t")->search(id, ignored));
    assert(g_engine.getSecondaryIndex(database, "t", "value")
               ->searchMulti(value)
               .empty());
    assert(g_engine.getHashIndex(database, "t", "value")
               ->search(value)
               .empty());
    assert(g_engine.getBloomIndex(database, "t", "value")
               ->search(value)
               .empty());
    assert(g_engine.query(database, "t", {"=value " + value}, {"id"})
               .empty());
}

void assertExactlyOne(const std::string& database, const std::string& id,
                      const std::string& value) {
    int64_t rid = -1;
    assert(g_engine.getPKIndex(database, "t")->search(id, rid));
    const auto secondary = g_engine.getSecondaryIndex(database, "t", "value")
                               ->searchMulti(value);
    const auto hash =
        g_engine.getHashIndex(database, "t", "value")->search(value);
    const auto bloom =
        g_engine.getBloomIndex(database, "t", "value")->search(value);
    assert(secondary.size() == 1 && secondary.front() == rid);
    assert(hash.size() == 1 && hash.front() == rid);
    assert(bloom.size() == 1 && bloom.front() == rid);
    assert(g_engine.query(database, "t", {"=value " + value}, {"id"})
               .size() == 1);
}

void runAutocommitFailure(const std::string& testName, bool forEachRow) {
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    createFixture(database, forEachRow);

    int calls = 0;
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++calls;
        return true;
    });
    std::vector<std::map<std::string, std::string>> returned{
        {{"sentinel", "keep"}}};
    const dbms::DBStatus status = g_engine.insert(
        database, "t", {{"id", "1"}, {"value", "failed"}}, &returned);
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(returned.size() == 1);
    assert(returned.front().at("sentinel") == "keep");
    assert(!g_engine.inTransaction());
    assertMissing(database, "1", "failed");

    // A retry may reuse the exact heap slot. Every access method must still
    // contain one mapping rather than a stale+new duplicate.
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"value", "failed"}}) ==
           dbms::DBStatus::OK);
    assertExactlyOne(database, "1", "failed");

    cleanupTestDb(testName);
}

void test_autocommit_row_and_statement_failures() {
    runAutocommitFailure("insert_after_row_failure", true);
    runAutocommitFailure("insert_after_statement_failure", false);
    std::cout << "[INSERT TRIGGER FAILURE] autocommit AFTER kinds cleaned OK"
              << std::endl;
}

void test_after_failure_rolls_back_to_statement_savepoint() {
    const std::string testName = "insert_after_failure_savepoint";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    createFixture(database, true);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    // No executor is installed yet, so the earlier statement is unaffected.
    assert(g_engine.insert(
               database, "t", {{"id", "2"}, {"value", "survivor"}}) ==
           dbms::DBStatus::OK);
    int calls = 0;
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++calls;
        return true;
    });
    const dbms::DBStatus status = g_engine.insert(
        database, "t", {{"id", "1"}, {"value", "failed"}});
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::IO_ERROR);
    assert(calls == 1);
    assert(g_engine.inTransaction());
    assertMissing(database, "1", "failed");
    assertExactlyOne(database, "2", "survivor");
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assertMissing(database, "1", "failed");
    assertExactlyOne(database, "2", "survivor");

    cleanupTestDb(testName);
    std::cout << "[INSERT TRIGGER FAILURE] transaction savepoint rollback OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_autocommit_row_and_statement_failures();
    test_after_failure_rolls_back_to_statement_savepoint();
    finalCleanupTestData();
    return 0;
}
