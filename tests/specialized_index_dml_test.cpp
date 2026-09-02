#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "catalog/type_registry.h"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <vector>

dbms::Config g_config;

using namespace dbms;
namespace fs = std::filesystem;

namespace {

constexpr const char* kDatabase = "specialized_index_dml_db";
constexpr const char* kTable = "items";

void cleanup() {
    std::error_code error;
    fs::remove_all(kDatabase, error);
    error.clear();
    fs::remove_all("info/.prepared", error);
    error.clear();
    fs::remove(".txnid", error);
}

bool containsRid(const std::vector<int64_t>& values, int64_t rid) {
    return std::find(values.begin(), values.end(), rid) != values.end();
}

int64_t ridFor(StorageEngine& engine, const std::string& id) {
    BPTree* index = engine.getPKIndex(kDatabase, kTable);
    assert(index != nullptr);
    int64_t rid = -1;
    assert(index->search(id, rid));
    return rid;
}

void assertIndexed(StorageEngine& engine, int64_t rid,
                   const std::string& token,
                   const std::string& point,
                   const std::string& score) {
    assert(containsRid(engine.fullTextSearch(
                           kDatabase, kTable, "body", token),
                       rid));
    assert(containsRid(engine.ginSearch(
                           kDatabase, kTable, "body", token),
                       rid));
    assert(containsRid(engine.giSTSearchOverlap(
                           kDatabase, kTable, "score", score, score),
                       rid));
    assert(containsRid(engine.spGiSTSearch(
                           kDatabase, kTable, "location", "=", point),
                       rid));
    assert(!engine.brinSearchRange(
                kDatabase, kTable, "score", "=", score).empty());
}

void assertTokenAbsent(StorageEngine& engine, const std::string& token) {
    assert(engine.fullTextSearch(
               kDatabase, kTable, "body", token).empty());
    assert(engine.ginSearch(
               kDatabase, kTable, "body", token).empty());
}

bool gistSidecarContains(const std::string& value) {
    std::ifstream input(
        fs::path(kDatabase) / "items_score.gist");
    int64_t rid = -1;
    std::string low;
    std::string high;
    while (input >> rid >> low >> high) {
        if (low == value || high == value) return true;
    }
    assert(input.eof());
    return false;
}

void createFixture(StorageEngine& engine) {
    assert(engine.createDatabase(kDatabase) == DBStatus::OK);
    TableSchema table;
    table.tablename = kTable;
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("body", false, 512, false));
    table.append(makePointColumn("location", false, false));
    table.append(makeIntColumn("score", false, 4, false));
    assert(engine.createTable(kDatabase, table) == DBStatus::OK);
    assert(engine.insert(
               kDatabase, kTable,
               {{"id", "1"}, {"body", "alpha legacy"},
                {"location", "1,1"}, {"score", "10"}}) ==
           DBStatus::OK);

    assert(engine.createFullTextIndex(
               kDatabase, kTable, "body") == DBStatus::OK);
    assert(engine.createGinIndex(
               kDatabase, kTable, "body") == DBStatus::OK);
    assert(engine.createGiSTIndex(
               kDatabase, kTable, "score") == DBStatus::OK);
    assert(engine.createSPGiSTIndex(
               kDatabase, kTable, "location") == DBStatus::OK);
    assert(engine.createBrinIndex(
               kDatabase, kTable, "score", 1) == DBStatus::OK);
    assertIndexed(engine, ridFor(engine, "1"), "alpha", "1,1", "10");
}

void testRenameLifecycle(StorageEngine& engine) {
    const fs::path root(kDatabase);
    const int64_t rid1 = ridFor(engine, "1");

    // A column rename must move SP-GiST just like the other specialized
    // families and invalidate its in-memory quadtree cache.
    assert(engine.alterTableRenameColumn(
               kDatabase, kTable, "location", "position") ==
           DBStatus::OK);
    assert(!fs::exists(root / "items_location.spgist"));
    assert(fs::exists(root / "items_position.spgist"));
    assert(containsRid(engine.spGiSTSearch(
                           kDatabase, kTable, "position", "=", "1,1"),
                       rid1));
    assert(engine.alterTableRenameColumn(
               kDatabase, kTable, "position", "location") ==
           DBStatus::OK);

    // An interrupted-generation marker belongs to the relation, not its SQL
    // spelling. Renaming the table must move both the sidecar and marker.
    {
        std::ofstream dirty(
            root / "items.specialized_index_dirty",
            std::ios::binary | std::ios::trunc);
        dirty << "DBMS_SPECIALIZED_INDEX_DIRTY_V1\n";
        assert(dirty.good());
    }
    constexpr const char* renamedTable = "renamed_items";
    assert(engine.alterTableRenameTable(
               kDatabase, kTable, renamedTable) == DBStatus::OK);
    assert(!fs::exists(root / "items_location.spgist"));
    assert(!fs::exists(root / "items.specialized_index_dirty"));
    assert(fs::exists(root / "renamed_items_location.spgist"));
    assert(fs::exists(root / "renamed_items.specialized_index_dirty"));
    assert(containsRid(engine.spGiSTSearch(
                           kDatabase, renamedTable, "location", "=", "1,1"),
                       rid1));
    assert(engine.alterTableRenameTable(
               kDatabase, renamedTable, kTable) == DBStatus::OK);

    // A successful autocommit mutation publishes a complete generation and
    // removes the marker after the heap and sidecars agree.
    assert(engine.update(
               kDatabase, kTable, {{"score", "10"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(!fs::exists(root / "items.specialized_index_dirty"));
    assertIndexed(engine, ridFor(engine, "1"), "alpha", "1,1", "10");
    std::cout << "[SPECIALIZED INDEX] rename lifecycle maintenance OK\n";
}

void testPartitionInsertVisibility(StorageEngine& engine) {
    constexpr const char* partitionedTable = "partitioned_items";
    TableSchema table;
    table.tablename = partitionedTable;
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("body", false, 256, false));
    table.partitionType = TableSchema::PartitionType::Hash;
    table.partitionKey = "id";
    table.hashPartitions = 2;
    assert(engine.createTable(kDatabase, table) == DBStatus::OK);
    assert(engine.createFullTextIndex(
               kDatabase, partitionedTable, "body") == DBStatus::OK);
    assert(engine.createGinIndex(
               kDatabase, partitionedTable, "body") == DBStatus::OK);

    // Partition DML uses a short-lived allocator. Its dirty page must be
    // flushed before a whole-table builder reopens the partition file.
    assert(engine.insert(
               kDatabase, partitionedTable,
               {{"id", "101"}, {"body", "partition visible"}}) ==
           DBStatus::OK);
    assert(!engine.fullTextSearch(
                kDatabase, partitionedTable, "body", "partition").empty());
    assert(!engine.ginSearch(
                kDatabase, partitionedTable, "body", "partition").empty());
    std::cout << "[SPECIALIZED INDEX] partition insert visibility OK\n";
}

void testDmlAndTransactionBoundaries(StorageEngine& engine) {
    assert(engine.insert(
               kDatabase, kTable,
               {{"id", "2"}, {"body", "beta fresh"},
                {"location", "2,2"}, {"score", "20"}}) ==
           DBStatus::OK);
    const int64_t rid2 = ridFor(engine, "2");
    assertIndexed(engine, rid2, "beta", "2,2", "20");

    assert(engine.update(
               kDatabase, kTable,
               {{"body", "gamma changed"}, {"location", "3,3"},
                {"score", "30"}},
               {"=id 1"}) == DBStatus::OK);
    const int64_t rid1 = ridFor(engine, "1");
    assertTokenAbsent(engine, "alpha");
    assertIndexed(engine, rid1, "gamma", "3,3", "30");
    assert(!gistSidecarContains("10"));
    assert(engine.spGiSTSearch(
               kDatabase, kTable, "location", "=", "1,1").empty());
    assert(engine.brinSearchRange(
               kDatabase, kTable, "score", "=", "10").empty());

    assert(engine.remove(kDatabase, kTable, {"=id 2"}) == DBStatus::OK);
    assertTokenAbsent(engine, "beta");
    assert(!gistSidecarContains("20"));
    assert(engine.spGiSTSearch(
               kDatabase, kTable, "location", "=", "2,2").empty());
    assert(engine.brinSearchRange(
               kDatabase, kTable, "score", "=", "20").empty());

    assert(engine.beginTransaction(kDatabase) == DBStatus::OK);
    assert(engine.insert(
               kDatabase, kTable,
               {{"id", "3"}, {"body", "delta pending"},
                {"location", "4,4"}, {"score", "40"}}) ==
           DBStatus::OK);
    // SQL must still see the writer's row while sidecar replacement is
    // deferred; filterRows deliberately uses the heap during an active xid.
    assert(engine.query(
               kDatabase, kTable, {"=id 3"}, {"body"}).size() == 1);
    assert(containsRid(engine.fullTextSearch(
                           kDatabase, kTable, "body", "delta"),
                       ridFor(engine, "3")));
    assert(engine.commitTransaction() == DBStatus::OK);
    const int64_t rid3 = ridFor(engine, "3");
    assertIndexed(engine, rid3, "delta", "4,4", "40");

    assert(engine.beginTransaction(kDatabase) == DBStatus::OK);
    assert(engine.update(
               kDatabase, kTable,
               {{"body", "epsilon rolledback"}, {"location", "5,5"},
                {"score", "50"}},
               {"=id 3"}) == DBStatus::OK);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertTokenAbsent(engine, "epsilon");
    assertIndexed(engine, ridFor(engine, "3"), "delta", "4,4", "40");
    assert(!gistSidecarContains("50"));
    assert(engine.spGiSTSearch(
               kDatabase, kTable, "location", "=", "5,5").empty());
    assert(engine.brinSearchRange(
               kDatabase, kTable, "score", "=", "50").empty());

    assert(engine.beginTransaction(kDatabase) == DBStatus::OK);
    assert(engine.remove(kDatabase, kTable, {"=id 3"}) == DBStatus::OK);
    assert(engine.rollbackTransaction() == DBStatus::OK);
    assertIndexed(engine, ridFor(engine, "3"), "delta", "4,4", "40");

    std::cout
        << "[SPECIALIZED INDEX] autocommit/commit/rollback maintenance OK\n";
}

void testPreparedBoundaries(StorageEngine& engine) {
    assert(engine.beginTransaction(kDatabase) == DBStatus::OK);
    assert(engine.insert(
               kDatabase, kTable,
               {{"id", "4"}, {"body", "zeta preparedcommit"},
                {"location", "6,6"}, {"score", "60"}}) ==
           DBStatus::OK);
    engine.getLockManager().setResourceNamespace(kDatabase);
    assert(engine.getLockManager().lockExclusive(kTable));
    assert(engine.prepareTransaction("specialized_commit") == DBStatus::OK);
    assertTokenAbsent(engine, "zeta");
    assert(engine.commitPrepared("specialized_commit") == DBStatus::OK);
    assertIndexed(engine, ridFor(engine, "4"), "zeta", "6,6", "60");

    assert(engine.beginTransaction(kDatabase) == DBStatus::OK);
    assert(engine.insert(
               kDatabase, kTable,
               {{"id", "5"}, {"body", "eta preparedrollback"},
                {"location", "7,7"}, {"score", "70"}}) ==
           DBStatus::OK);
    engine.getLockManager().setResourceNamespace(kDatabase);
    assert(engine.getLockManager().lockExclusive(kTable));
    assert(engine.prepareTransaction("specialized_rollback") == DBStatus::OK);
    assertTokenAbsent(engine, "eta");
    assert(engine.rollbackPrepared("specialized_rollback") == DBStatus::OK);
    assert(engine.query(
               kDatabase, kTable, {"=id 5"}, {"id"}).empty());
    assertTokenAbsent(engine, "eta");
    assert(!gistSidecarContains("70"));
    assert(engine.spGiSTSearch(
               kDatabase, kTable, "location", "=", "7,7").empty());
    assert(engine.brinSearchRange(
               kDatabase, kTable, "score", "=", "70").empty());

    std::cout << "[SPECIALIZED INDEX] prepared commit/rollback maintenance OK\n";
}

void damageSidecarsAndAssertHeapFallback(StorageEngine& engine) {
    const fs::path root(kDatabase);
    const std::vector<fs::path> sidecars = {
        root / "items_body.fti", root / "items_body.gin",
        root / "items_score.gist", root / "items_location.spgist",
        root / "items_score.brin"};
    for (const auto& path : sidecars) {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output << "interrupted replacement";
        assert(output.good());
    }
    {
        std::ofstream dirty(
            root / "items.specialized_index_dirty",
            std::ios::binary | std::ios::trunc);
        dirty << "DBMS_SPECIALIZED_INDEX_DIRTY_V1\n";
        assert(dirty.good());
    }

    const int64_t rid1 = ridFor(engine, "1");
    // Dirty or snapshot-incomplete sidecars are bypassed by the public
    // access-method searches themselves, so both direct and planned callers
    // receive snapshot-correct heap-derived candidates.
    assert(containsRid(engine.fullTextSearch(
                           kDatabase, kTable, "body", "gamma"),
                       rid1));
    assert(containsRid(engine.ginSearch(
                           kDatabase, kTable, "body", "gamma"),
                       rid1));
    assert(containsRid(engine.giSTSearchOverlap(
                           kDatabase, kTable, "score", "30", "30"),
                       rid1));
    assert(containsRid(engine.spGiSTSearch(
                           kDatabase, kTable, "location", "=", "3,3"),
                       rid1));
    assert(!engine.brinSearchRange(
                kDatabase, kTable, "score", "=", "30").empty());
    assert(engine.query(
               kDatabase, kTable, {"containsbody gamma"}, {"id"}).size() == 1);
}

void assertRecoveredSidecars(StorageEngine& engine) {
    assert(!fs::exists(
        fs::path(kDatabase) / "items.specialized_index_dirty"));
    assertIndexed(engine, ridFor(engine, "1"), "gamma", "3,3", "30");
    assertIndexed(engine, ridFor(engine, "3"), "delta", "4,4", "40");
    assertIndexed(engine, ridFor(engine, "4"), "zeta", "6,6", "60");
    assertTokenAbsent(engine, "eta");
    std::cout << "[SPECIALIZED INDEX] startup reconstruction/dirty fallback OK\n";
}

}  // namespace

int main() {
    cleanup();
    TypeRegistry::instance().bootstrap();
    {
        StorageEngine engine;
        createFixture(engine);
        testRenameLifecycle(engine);
        testPartitionInsertVisibility(engine);
        testDmlAndTransactionBoundaries(engine);
        testPreparedBoundaries(engine);
        damageSidecarsAndAssertHeapFallback(engine);
    }
    {
        StorageEngine recovered;
        assertRecoveredSidecars(recovered);
    }
    cleanup();
    std::cout << "[SPECIALIZED INDEX] all tests passed\n";
    return 0;
}
