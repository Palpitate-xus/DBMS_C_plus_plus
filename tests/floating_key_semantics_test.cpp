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

void assertOneRid(const std::vector<int64_t>& values) {
    assert(values.size() == 1);
}

void test_unique_and_secondary_keys(const std::string& database) {
    dbms::TableSchema schema;
    schema.tablename = "unique_values";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column single = dbms::makeFloatColumn("single_value", false);
    single.isUnique = true;
    schema.append(single);
    dbms::Column precise = dbms::makeDoubleColumn("double_value", false);
    precise.isUnique = true;
    schema.append(precise);
    schema.append(dbms::makeDoubleColumn("indexed_value", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "unique_values", "indexed_value") ==
           dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(
               database, "unique_values", "indexed_value") ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(
               database, "unique_values", "indexed_value") ==
           dbms::DBStatus::OK);

    assert(g_engine.insert(database, "unique_values",
                           {{"id", "1"}, {"single_value", "1"},
                            {"double_value", "-0"},
                            {"indexed_value", "-0"}}) ==
           dbms::DBStatus::OK);

    // Index keys use SQL floating equality, while the heap value keeps the
    // sign bit so SELECT can still render negative zero faithfully.
    assertOneRid(g_engine.getSecondaryIndex(
                      database, "unique_values", "double_value")
                     ->searchMulti("0"));
    assertOneRid(g_engine.getSecondaryIndex(
                      database, "unique_values", "indexed_value")
                     ->searchMulti("0"));
    assertOneRid(g_engine.getHashIndex(
                      database, "unique_values", "indexed_value")
                     ->search("0"));
    assertOneRid(g_engine.getBloomIndex(
                      database, "unique_values", "indexed_value")
                     ->search("0"));
    assert(g_engine.query(database, "unique_values", {"=indexed_value 0"},
                          {"id"}) == std::vector<std::string>{"1 "});
    assert(g_engine.query(database, "unique_values", {"=id 1"},
                          {"double_value", "indexed_value"}) ==
           std::vector<std::string>{"-0 -0 "});

    // Textually different spellings of the same binary value are one key.
    assert(g_engine.insert(database, "unique_values",
                           {{"id", "2"}, {"single_value", "1.0"},
                            {"double_value", "2"},
                            {"indexed_value", "2"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(database, "unique_values",
                           {{"id", "3"}, {"single_value", "3"},
                            {"double_value", "0"},
                            {"indexed_value", "3"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    // PostgreSQL-style NaN equality also applies to uniqueness.
    assert(g_engine.insert(database, "unique_values",
                           {{"id", "4"}, {"single_value", "NaN"},
                            {"double_value", "4"},
                            {"indexed_value", "4"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_values",
                           {{"id", "5"}, {"single_value", "nan"},
                            {"double_value", "5"},
                            {"indexed_value", "5"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    assert(g_engine.insert(database, "unique_values",
                           {{"id", "6"}, {"single_value", "2"},
                            {"double_value", "6"},
                            {"indexed_value", "6"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "unique_values",
                           {{"single_value", "1e0"}}, {"=id 6"}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.query(database, "unique_values", {"=id 6"},
                          {"single_value"}) ==
           std::vector<std::string>{"2 "});

    // Rollback must use the same canonical keys when it reverses UPDATE,
    // INSERT, and DELETE index maintenance.
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "unique_values",
                           {{"indexed_value", "7"}}, {"=id 1"}) ==
           dbms::DBStatus::OK);
    assertOneRid(g_engine.getHashIndex(
                      database, "unique_values", "indexed_value")
                     ->search("7"));
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assertOneRid(g_engine.getHashIndex(
                      database, "unique_values", "indexed_value")
                     ->search("0"));
    assert(g_engine.getHashIndex(database, "unique_values", "indexed_value")
               ->search("7")
               .empty());

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_values",
                           {{"id", "7"}, {"single_value", "7"},
                            {"double_value", "7"},
                            {"indexed_value", "-0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.getBloomIndex(database, "unique_values", "indexed_value")
               ->search("0")
               .size() == 2);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assertOneRid(g_engine.getBloomIndex(
                      database, "unique_values", "indexed_value")
                     ->search("0"));

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.remove(database, "unique_values", {"=id 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.getSecondaryIndex(
                      database, "unique_values", "indexed_value")
               ->searchMulti("0")
               .empty());
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assertOneRid(g_engine.getSecondaryIndex(
                      database, "unique_values", "indexed_value")
                     ->searchMulti("0"));
}

void test_primary_and_composite_keys(const std::string& database) {
    dbms::TableSchema primary;
    primary.tablename = "primary_values";
    primary.formatVersion = 2;
    primary.append(dbms::makeDoubleColumn("key_value", false, true));
    primary.append(dbms::makeVarCharColumn("label", false, 16));
    assert(g_engine.createTable(database, primary) == dbms::DBStatus::OK);

    assert(g_engine.insert(database, "primary_values",
                           {{"key_value", "1"}, {"label", "one"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "primary_values",
                           {{"key_value", "1.0"}, {"label", "duplicate"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(database, "primary_values",
                           {{"key_value", "-0"}, {"label", "minus"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "primary_values",
                           {{"key_value", "0"}, {"label", "plus"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.insert(database, "primary_values",
                           {{"key_value", "NaN"}, {"label", "nan"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "primary_values",
                           {{"key_value", "nan"}, {"label", "duplicate"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    int64_t ignored = -1;
    assert(g_engine.getPKIndex(database, "primary_values")
               ->search("0", ignored));
    assert(g_engine.query(database, "primary_values", {"=key_value 0"},
                          {"label"}) == std::vector<std::string>{"minus "});
    assert(g_engine.query(database, "primary_values", {"=label minus"},
                          {"key_value"}) == std::vector<std::string>{"-0 "});

    dbms::TableSchema composite;
    composite.tablename = "composite_values";
    composite.formatVersion = 2;
    composite.append(dbms::makeIntColumn("id", false, 4, true));
    composite.append(dbms::makeDoubleColumn("left_value", false));
    composite.append(dbms::makeFloatColumn("right_value", false));
    composite.uniqueConstraints.push_back({1, 2});
    composite.uniqueConstraintNames.push_back("composite_values_pair_key");
    assert(g_engine.createTable(database, composite) == dbms::DBStatus::OK);
    assert(g_engine.createCompositeIndex(
               database, "composite_values", {"left_value", "right_value"},
               "floating_pair_idx") == dbms::DBStatus::OK);

    assert(g_engine.insert(database, "composite_values",
                           {{"id", "1"}, {"left_value", "1"},
                            {"right_value", "-0"}}) ==
           dbms::DBStatus::OK);
    const std::string canonicalPair = std::string("1") + '\x01' + "0";
    assertOneRid(g_engine.getCompositeIndexTree(
                      database, "composite_values", "floating_pair_idx")
                     ->searchMulti(canonicalPair));
    assert(g_engine.insert(database, "composite_values",
                           {{"id", "2"}, {"left_value", "1.0"},
                            {"right_value", "0"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    // Creating indexes over existing heap data must produce the same keys as
    // incremental maintenance.
    dbms::TableSchema late;
    late.tablename = "late_indexes";
    late.formatVersion = 2;
    late.append(dbms::makeIntColumn("id", false, 4, true));
    late.append(dbms::makeDoubleColumn("value", false));
    assert(g_engine.createTable(database, late) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "late_indexes",
                           {{"id", "1"}, {"value", "-0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "late_indexes", "value") ==
           dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(database, "late_indexes", "value") ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(database, "late_indexes", "value") ==
           dbms::DBStatus::OK);
    assertOneRid(g_engine.getSecondaryIndex(database, "late_indexes", "value")
                     ->searchMulti("0"));
    assertOneRid(g_engine.getHashIndex(database, "late_indexes", "value")
                     ->search("0"));
    assertOneRid(g_engine.getBloomIndex(database, "late_indexes", "value")
                     ->search("0"));
}

void test_deferred_unique_key(const std::string& database) {
    dbms::TableSchema deferred;
    deferred.tablename = "deferred_values";
    deferred.formatVersion = 2;
    deferred.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column value = dbms::makeDoubleColumn("value", false);
    value.isUnique = true;
    deferred.append(value);
    assert(g_engine.createTable(database, deferred) == dbms::DBStatus::OK);
    assert(g_engine.updateStorageParams(
               database, "deferred_values",
               {{"constraint.deferred_values_value_key.deferrable", "1"},
                {"constraint.deferred_values_value_key.initially_deferred",
                 "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.isConstraintCurrentlyDeferred(
        database, "deferred_values", "deferred_values_value_key"));

    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "deferred_values",
                           {{"id", "1"}, {"value", "-0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "deferred_values",
                           {{"id", "2"}, {"value", "0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.query(database, "deferred_values", {}, {"id"}).empty());
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "floating_key_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    test_unique_and_secondary_keys(database);
    test_primary_and_composite_keys(database);
    test_deferred_unique_key(database);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FLOATING KEYS] equality/index semantics OK" << std::endl;
    return 0;
}
