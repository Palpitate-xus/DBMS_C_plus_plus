#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

dbms::TableSchema makeTable(const std::string& name) {
    dbms::TableSchema table;
    table.tablename = name;
    table.formatVersion = 2;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeVarCharColumn("left_value", false, 40));
    table.append(dbms::makeVarCharColumn("right_value", false, 40));
    return table;
}

void test_single_column_insert_failure(const std::string& database) {
    using dbms::DBStatus;

    assert(g_engine.createTable(database, makeTable("single_failure")) ==
           DBStatus::OK);
    assert(g_engine.insert(
               database, "single_failure",
               {{"id", "1"}, {"left_value", "alpha"},
                {"right_value", "one"}}) == DBStatus::OK);

    dbms::BPTree* index = g_engine.getSecondaryIndex(
        database, "single_failure", "left_value");
    assert(index != nullptr);
    const std::filesystem::path indexPath = index->filePath();
    index->close();

    assert(g_engine.createIndex(
               database, "single_failure", "left_value") ==
           DBStatus::IO_ERROR);
    assert(g_engine.getIndexMetadata(
               database, "single_failure").empty());
    assert(!std::filesystem::exists(indexPath));

    // Failure cleanup must evict the closed cache entry and release the
    // metadata lock so an ordinary retry can build a complete index.
    assert(g_engine.createIndex(
               database, "single_failure", "left_value") == DBStatus::OK);
    const auto rids = g_engine.getSecondaryIndex(
        database, "single_failure", "left_value")->searchMulti("alpha");
    assert(rids.size() == 1);
}

void test_single_column_flush_failure(const std::string& database) {
    using dbms::DBStatus;

    assert(g_engine.createTable(database, makeTable("flush_failure")) ==
           DBStatus::OK);
    dbms::BPTree* index = g_engine.getSecondaryIndex(
        database, "flush_failure", "left_value");
    assert(index != nullptr);
    const std::filesystem::path indexPath = index->filePath();
    index->close();

    // With no rows there is no insert to fail. CREATE must still detect that
    // the finished tree cannot be flushed before metadata is published.
    assert(g_engine.createIndex(
               database, "flush_failure", "left_value") ==
           DBStatus::IO_ERROR);
    assert(g_engine.getIndexMetadata(
               database, "flush_failure").empty());
    assert(!std::filesystem::exists(indexPath));
    assert(g_engine.createIndex(
               database, "flush_failure", "left_value") == DBStatus::OK);
}

void test_composite_insert_failure(const std::string& database) {
    using dbms::DBStatus;

    assert(g_engine.createTable(database, makeTable("composite_failure")) ==
           DBStatus::OK);
    assert(g_engine.insert(
               database, "composite_failure",
               {{"id", "1"}, {"left_value", "alpha"},
                {"right_value", "one"}}) == DBStatus::OK);

    dbms::BPTree* index = g_engine.getCompositeIndexTree(
        database, "composite_failure", "pair_idx");
    assert(index != nullptr);
    const std::filesystem::path indexPath = index->filePath();
    index->close();

    assert(g_engine.createCompositeIndex(
               database, "composite_failure",
               {"left_value", "right_value"}, "pair_idx") ==
           DBStatus::IO_ERROR);
    assert(g_engine.getCompositeIndexes(
               database, "composite_failure").empty());
    assert(!std::filesystem::exists(indexPath));

    assert(g_engine.createCompositeIndex(
               database, "composite_failure",
               {"left_value", "right_value"}, "pair_idx") ==
           DBStatus::OK);
    const auto rids = g_engine.getCompositeIndexTree(
        database, "composite_failure", "pair_idx")
                          ->searchMulti("alpha\x01one");
    assert(rids.size() == 1);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "index_build_failure";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    test_single_column_insert_failure(database);
    test_single_column_flush_failure(database);
    test_composite_insert_failure(database);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[INDEX BUILD FAILURE] failed trees were not published OK"
              << std::endl;
    return 0;
}
