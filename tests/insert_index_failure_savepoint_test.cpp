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

const std::map<std::string, std::string> kSurvivor{
    {"id", "2"}, {"a", "survivor-a"}, {"b", "survivor-b"},
    {"h", "survivor-h"}, {"bl", "survivor-bl"}};
const std::map<std::string, std::string> kFailed{
    {"id", "1"}, {"a", "failed-a"}, {"b", "failed-b"},
    {"h", "failed-h"}, {"bl", "failed-bl"}};

std::string compositeKey(
    const std::map<std::string, std::string>& values) {
    return values.at("a") + std::string("\x01") + values.at("b");
}

void assertExactlyOne(const std::string& database,
                      const std::map<std::string, std::string>& values) {
    int64_t rid = -1;
    assert(g_engine.getPKIndex(database, "t")->search(values.at("id"), rid));
    for (const std::string column : {"a", "b"}) {
        const auto rids = g_engine.getSecondaryIndex(database, "t", column)
                              ->searchMulti(values.at(column));
        assert(rids.size() == 1 && rids.front() == rid);
    }
    const auto composite =
        g_engine.getCompositeIndexTree(database, "t", "ab_idx")
            ->searchMulti(compositeKey(values));
    const auto hash =
        g_engine.getHashIndex(database, "t", "h")->search(values.at("h"));
    const auto bloom = g_engine.getBloomIndex(database, "t", "bl")
                           ->search(values.at("bl"));
    assert(composite.size() == 1 && composite.front() == rid);
    assert(hash.size() == 1 && hash.front() == rid);
    assert(bloom.size() == 1 && bloom.front() == rid);
    assert(g_engine.query(
               database, "t", {"=id " + values.at("id")}, {"id"})
               .size() == 1);
}

void assertMissing(const std::string& database,
                   const std::map<std::string, std::string>& values) {
    int64_t ignored = -1;
    assert(!g_engine.getPKIndex(database, "t")
                ->search(values.at("id"), ignored));
    for (const std::string column : {"a", "b"}) {
        assert(g_engine.getSecondaryIndex(database, "t", column)
                   ->searchMulti(values.at(column))
                   .empty());
    }
    assert(g_engine.getCompositeIndexTree(database, "t", "ab_idx")
               ->searchMulti(compositeKey(values))
               .empty());
    assert(g_engine.getHashIndex(database, "t", "h")
               ->search(values.at("h"))
               .empty());
    assert(g_engine.getBloomIndex(database, "t", "bl")
               ->search(values.at("bl"))
               .empty());
    assert(g_engine.query(
               database, "t", {"=id " + values.at("id")}, {"id"})
               .empty());
}

void test_partial_index_failure_uses_statement_savepoint() {
    const std::string testName = "insert_index_failure_savepoint";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("a", false, 40));
    schema.append(dbms::makeVarCharColumn("b", false, 40));
    schema.append(dbms::makeVarCharColumn("h", false, 40));
    schema.append(dbms::makeVarCharColumn("bl", false, 40));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "t", "a") == dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "t", "b") == dbms::DBStatus::OK);
    assert(g_engine.createCompositeIndex(
               database, "t", {"a", "b"}, "ab_idx") ==
           dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(database, "t", "h") ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(database, "t", "bl") ==
           dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", kSurvivor) == dbms::DBStatus::OK);

    // INSERT writes the primary key and ordinary indexes before composite,
    // hash, and Bloom indexes. Closing the second ordinary index forces a
    // failure after a prefix has already been written.
    const auto metadata = g_engine.getIndexMetadata(database, "t");
    assert(metadata.size() == 2);
    dbms::BPTree* failedIndex = g_engine.getSecondaryIndex(
        database, "t", metadata[1].name);
    assert(failedIndex != nullptr);
    failedIndex->close();
    assert(!failedIndex->isOpen());

    assert(g_engine.insert(database, "t", kFailed) ==
           dbms::DBStatus::IO_ERROR);
    assert(g_engine.inTransaction());
    assert(failedIndex->isOpen());
    assertMissing(database, kFailed);
    assertExactlyOne(database, kSurvivor);

    // The failed statement did not poison the surrounding transaction, and
    // reusing its heap slot creates one exact mapping in every access method.
    assert(g_engine.insert(database, "t", kFailed) == dbms::DBStatus::OK);
    assertExactlyOne(database, kFailed);
    assertExactlyOne(database, kSurvivor);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    assertExactlyOne(database, kFailed);
    assertExactlyOne(database, kSurvivor);

    cleanupTestDb(testName);
    std::cout << "[INSERT INDEX FAILURE] statement savepoint stayed atomic OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_partial_index_failure_uses_statement_savepoint();
    finalCleanupTestData();
    return 0;
}
