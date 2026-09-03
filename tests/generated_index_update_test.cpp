#include "Session.h"
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
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

void test_generated_hash_and_bloom_keys_follow_logical_update() {
    const std::string database = testDbPath("generated_index_update");
    cleanupTestDb("generated_index_update");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, "
        "c INT GENERATED ALWAYS AS (a + b) STORED)",
        session));
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"a", "3"}, {"b", "4"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(database, "t", "c") ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(database, "t", "c") ==
           dbms::DBStatus::OK);

    int64_t oldRid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", oldRid));
    assertOneRid(
        g_engine.getHashIndex(database, "t", "c")->search("7"), oldRid);
    assertOneRid(
        g_engine.getBloomIndex(database, "t", "c")->search("7"), oldRid);

    // c is not in the SET list, but its stored value and both access-method
    // keys must move from 7 to 14 along with the new heap version.
    assert(g_engine.update(database, "t", {{"a", "10"}}, {"=id 1"}) ==
           dbms::DBStatus::OK);

    int64_t newRid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", newRid));
    assert(newRid != oldRid);
    assert(g_engine.getHashIndex(database, "t", "c")->search("7").empty());
    assertOneRid(
        g_engine.getHashIndex(database, "t", "c")->search("14"), newRid);
    assert(g_engine.getBloomIndex(database, "t", "c")->search("7").empty());
    assertOneRid(
        g_engine.getBloomIndex(database, "t", "c")->search("14"), newRid);
    assert(g_engine.query(database, "t", {"=c 7"}, {"id"}).empty());
    assert(g_engine.query(database, "t", {"=c 14"}, {"id"}).size() == 1);

    cleanupTestDb("generated_index_update");
    std::cout << "[GENERATED INDEX UPDATE] hash/bloom logical keys moved OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_generated_hash_and_bloom_keys_follow_logical_update();
    finalCleanupTestData();
    return 0;
}
