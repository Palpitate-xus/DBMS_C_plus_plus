#include "access/BPTree.h"
#include "access/BloomIndex.h"
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

void test_failed_insert_removes_earlier_bloom_entries() {
    const std::string database = testDbPath("insert_bloom_cleanup");
    cleanupTestDb("insert_bloom_cleanup");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("b1", false, 30));
    schema.append(dbms::makeVarCharColumn("b2", false, 30));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(database, "t", "b1") ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(database, "t", "b2") ==
           dbms::DBStatus::OK);

    dbms::BloomIndex* first = g_engine.getBloomIndex(database, "t", "b1");
    dbms::BloomIndex* second = g_engine.getBloomIndex(database, "t", "b2");
    assert(first != nullptr && second != nullptr);
    assert(second->close());

    // b1 is inserted first. The closed b2 then forces abortIndexUpdate after
    // b1 already contains the new RID.
    assert(g_engine.insert(
               database, "t",
               {{"id", "1"}, {"b1", "first-key"}, {"b2", "second-key"}}) ==
           dbms::DBStatus::IO_ERROR);
    assert(g_engine.query(database, "t", {}, {"id"}).empty());
    assert(first->search("first-key").empty());

    // Reusing the slot must create exactly one mapping, not expose the stale
    // Bloom RID left by the failed attempt plus the successful retry.
    assert(second->open());
    assert(g_engine.insert(
               database, "t",
               {{"id", "1"}, {"b1", "first-key"}, {"b2", "second-key"}}) ==
           dbms::DBStatus::OK);
    int64_t rid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", rid));
    assertOneRid(first->search("first-key"), rid);
    assertOneRid(second->search("second-key"), rid);

    cleanupTestDb("insert_bloom_cleanup");
    std::cout << "[INSERT BLOOM CLEANUP] failed insert left no stale RID OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_failed_insert_removes_earlier_bloom_entries();
    finalCleanupTestData();
    return 0;
}
