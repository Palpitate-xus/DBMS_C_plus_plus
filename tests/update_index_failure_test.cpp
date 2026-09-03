#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

void assertOneRid(const std::vector<int64_t>& values, int64_t rid) {
    assert(values.size() == 1);
    assert(values.front() == rid);
}

int64_t createBtreeFixture(const std::string& database) {
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("a", false, 30));
    schema.append(dbms::makeVarCharColumn("b", false, 30));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t",
               {{"id", "1"}, {"a", "alpha"}, {"b", "beta"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "t", "a") == dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "t", "b") == dbms::DBStatus::OK);
    assert(g_engine.createCompositeIndex(
               database, "t", {"a", "b"}, "ab_idx") ==
           dbms::DBStatus::OK);
    int64_t rid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", rid));
    return rid;
}

void removeFirstSecondaryEntry(const std::string& database, int64_t rid) {
    const auto metadata = g_engine.getIndexMetadata(database, "t");
    assert(metadata.size() == 2);
    const std::string& removedName = metadata.front().name;
    const std::string removedKey = removedName == "a" ? "alpha" : "beta";
    dbms::BPTree* index =
        g_engine.getSecondaryIndex(database, "t", removedName);
    assert(index != nullptr);
    assert(index->removeMulti(removedKey, rid));
}

void assertBtreeFixtureRestored(const std::string& database, int64_t rid) {
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "t", {"=a alpha"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "t", {"=a new-alpha"}, {"id"}).empty());

    int64_t primaryRid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", primaryRid));
    assert(primaryRid == rid);
    assertOneRid(
        g_engine.getSecondaryIndex(database, "t", "a")->searchMulti("alpha"),
        rid);
    assert(g_engine.getSecondaryIndex(database, "t", "a")
               ->searchMulti("new-alpha")
               .empty());
    assertOneRid(
        g_engine.getSecondaryIndex(database, "t", "b")->searchMulti("beta"),
        rid);
    assertOneRid(
        g_engine.getCompositeIndexTree(database, "t", "ab_idx")
            ->searchMulti(std::string("alpha\x01") + "beta"),
        rid);
    assert(g_engine.getCompositeIndexTree(database, "t", "ab_idx")
               ->searchMulti(std::string("new-alpha\x01") + "beta")
               .empty());
}

void test_primary_failure_rolls_back() {
    const std::string database = testDbPath("update_primary_failure");
    cleanupTestDb("update_primary_failure");
    const int64_t rid = createBtreeFixture(database);
    assert(g_engine.getPKIndex(database, "t")->remove("1"));

    assert(g_engine.update(database, "t", {{"a", "new-alpha"}}, {}) ==
           dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assertBtreeFixtureRestored(database, rid);

    cleanupTestDb("update_primary_failure");
    std::cout << "[UPDATE INDEX FAILURE] primary rollback repaired indexes OK"
              << std::endl;
}

void test_autocommit_secondary_failure_rolls_back() {
    const std::string database = testDbPath("update_secondary_failure");
    cleanupTestDb("update_secondary_failure");
    const int64_t rid = createBtreeFixture(database);
    removeFirstSecondaryEntry(database, rid);

    assert(g_engine.update(
               database, "t", {{"a", "new-alpha"}}, {"=id 1"}) ==
           dbms::DBStatus::IO_ERROR);
    assert(!g_engine.inTransaction());
    assertBtreeFixtureRestored(database, rid);

    cleanupTestDb("update_secondary_failure");
    std::cout << "[UPDATE INDEX FAILURE] autocommit rollback repaired indexes OK"
              << std::endl;
}

void test_savepoint_secondary_failure_rolls_back_statement() {
    const std::string database = testDbPath("update_secondary_savepoint");
    cleanupTestDb("update_secondary_savepoint");
    const int64_t rid = createBtreeFixture(database);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t",
               {{"id", "2"}, {"a", "second"}, {"b", "survivor"}}) ==
           dbms::DBStatus::OK);
    removeFirstSecondaryEntry(database, rid);

    assert(g_engine.update(
               database, "t", {{"a", "new-alpha"}}, {"=id 1"}) ==
           dbms::DBStatus::IO_ERROR);
    assert(g_engine.inTransaction());
    assertBtreeFixtureRestored(database, rid);
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).size() == 1);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assertBtreeFixtureRestored(database, rid);
    assert(g_engine.query(database, "t", {"=id 2"}, {"id"}).size() == 1);

    cleanupTestDb("update_secondary_savepoint");
    std::cout << "[UPDATE INDEX FAILURE] savepoint rollback stayed atomic OK"
              << std::endl;
}

void test_composite_failure_rolls_back() {
    const std::string database = testDbPath("update_composite_failure");
    cleanupTestDb("update_composite_failure");
    const int64_t rid = createBtreeFixture(database);
    dbms::BPTree* composite =
        g_engine.getCompositeIndexTree(database, "t", "ab_idx");
    assert(composite != nullptr);
    assert(composite->removeMulti(
        std::string("alpha\x01") + "beta", rid));

    assert(g_engine.update(
               database, "t", {{"a", "new-alpha"}}, {"=id 1"}) ==
           dbms::DBStatus::IO_ERROR);
    assertBtreeFixtureRestored(database, rid);

    cleanupTestDb("update_composite_failure");
    std::cout << "[UPDATE INDEX FAILURE] composite rollback repaired indexes OK"
              << std::endl;
}

int64_t createHashBloomFixture(const std::string& database) {
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("h", false, 30));
    schema.append(dbms::makeVarCharColumn("b", false, 30));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t",
               {{"id", "1"}, {"h", "hash-key"}, {"b", "bloom-key"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(database, "t", "h") ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(database, "t", "b") ==
           dbms::DBStatus::OK);
    int64_t rid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", rid));
    return rid;
}

void assertHashBloomFixtureRestored(const std::string& database, int64_t rid) {
    assert(g_engine.query(database, "t", {"=id 1"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "t", {"=h hash-key"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "t", {"=h new-hash"}, {"id"}).empty());
    assert(g_engine.query(database, "t", {"=b bloom-key"}, {"id"}).size() == 1);
    assert(g_engine.query(database, "t", {"=b new-bloom"}, {"id"}).empty());

    int64_t primaryRid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", primaryRid));
    assert(primaryRid == rid);
    assertOneRid(
        g_engine.getHashIndex(database, "t", "h")->search("hash-key"), rid);
    assert(g_engine.getHashIndex(database, "t", "h")
               ->search("new-hash")
               .empty());
    assertOneRid(
        g_engine.getBloomIndex(database, "t", "b")->search("bloom-key"),
        rid);
    assert(g_engine.getBloomIndex(database, "t", "b")
               ->search("new-bloom")
               .empty());
}

void test_hash_failure_rolls_back() {
    const std::string database = testDbPath("update_hash_failure");
    cleanupTestDb("update_hash_failure");
    const int64_t rid = createHashBloomFixture(database);
    assert(g_engine.getHashIndex(database, "t", "h")
               ->remove("hash-key", rid));

    assert(g_engine.update(
               database, "t", {{"h", "new-hash"}}, {"=id 1"}) ==
           dbms::DBStatus::IO_ERROR);
    assertHashBloomFixtureRestored(database, rid);

    cleanupTestDb("update_hash_failure");
    std::cout << "[UPDATE INDEX FAILURE] hash rollback repaired indexes OK"
              << std::endl;
}

void test_bloom_failure_rolls_back() {
    const std::string database = testDbPath("update_bloom_failure");
    cleanupTestDb("update_bloom_failure");
    const int64_t rid = createHashBloomFixture(database);
    assert(g_engine.getBloomIndex(database, "t", "b")
               ->remove("bloom-key", rid));

    assert(g_engine.update(
               database, "t", {{"b", "new-bloom"}}, {"=id 1"}) ==
           dbms::DBStatus::IO_ERROR);
    assertHashBloomFixtureRestored(database, rid);

    cleanupTestDb("update_bloom_failure");
    std::cout << "[UPDATE INDEX FAILURE] bloom rollback repaired indexes OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_primary_failure_rolls_back();
    test_autocommit_secondary_failure_rolls_back();
    test_savepoint_secondary_failure_rolls_back_statement();
    test_composite_failure_rolls_back();
    test_hash_failure_rolls_back();
    test_bloom_failure_rolls_back();
    finalCleanupTestData();
    return 0;
}
