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

void test_before_insert_precedes_stored_generation() {
    const std::string testName = "generated_insert_trigger";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeIntColumn("a", false, 4));
    schema.append(dbms::makeIntColumn("b", false, 4));
    dbms::Column generated = dbms::makeIntColumn("c", true, 4);
    generated.generatedExpr = "a + b";
    generated.generatedKind = 's';
    generated.checkExpr = "c > 10";
    schema.append(generated);
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.createIndex(database, "t", "c") == dbms::DBStatus::OK);
    assert(g_engine.createHashIndex(database, "t", "c") ==
           dbms::DBStatus::OK);
    assert(g_engine.createBloomIndex(database, "t", "c") ==
           dbms::DBStatus::OK);
    assert(g_engine.createTrigger(database, {
               "rewrite_a", "before", "insert", "t", "set a = 10", "",
               true, true, {}}) == dbms::DBStatus::OK);

    int triggerCalls = 0;
    g_engine.setTriggerExecutor([&](const std::string&) {
        ++triggerCalls;
        return false;
    });
    std::vector<std::map<std::string, std::string>> returned;
    const dbms::DBStatus status = g_engine.insert(
        database, "t", {{"id", "1"}, {"a", "3"}, {"b", "4"}},
        &returned);
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::OK);
    assert(triggerCalls == 1);
    assert(returned.size() == 1);
    assert(returned.front().at("a") == "10");
    assert(returned.front().at("c") == "14");
    const auto rows = g_engine.query(database, "t", {"=id 1"}, {"a", "c"});
    assert(rows.size() == 1);
    assert(rows.front().find("10") != std::string::npos);
    assert(rows.front().find("14") != std::string::npos);

    int64_t rid = -1;
    assert(g_engine.getPKIndex(database, "t")->search("1", rid));
    const auto secondary =
        g_engine.getSecondaryIndex(database, "t", "c")->searchMulti("14");
    const auto hash =
        g_engine.getHashIndex(database, "t", "c")->search("14");
    const auto bloom =
        g_engine.getBloomIndex(database, "t", "c")->search("14");
    assert(secondary.size() == 1 && secondary.front() == rid);
    assert(hash.size() == 1 && hash.front() == rid);
    assert(bloom.size() == 1 && bloom.front() == rid);
    assert(g_engine.getSecondaryIndex(database, "t", "c")
               ->searchMulti("7")
               .empty());
    assert(g_engine.getHashIndex(database, "t", "c")->search("7").empty());
    assert(g_engine.getBloomIndex(database, "t", "c")
               ->search("7")
               .empty());

    cleanupTestDb(testName);
    std::cout << "[GENERATED INSERT] BEFORE trigger row image indexed OK"
              << std::endl;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_before_insert_precedes_stored_generation();
    finalCleanupTestData();
    return 0;
}
