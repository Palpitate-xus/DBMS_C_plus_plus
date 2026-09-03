#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

dbms::TableSchema uniqueSchema(const std::string& tableName) {
    dbms::TableSchema schema;
    schema.tablename = tableName;
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column code = dbms::makeVarCharColumn("code", false, 40);
    code.isUnique = true;
    schema.append(code);
    return schema;
}

void useSuccessfulTriggerExecutor() {
    g_engine.setTriggerExecutor([](const std::string&) { return false; });
}

void test_trigger_created_unique_conflict_is_rejected() {
    const std::string testName = "insert_trigger_unique_reject";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    assert(g_engine.createTable(database, uniqueSchema("t")) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"code", "taken"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createTrigger(database, {
               "duplicate_code", "before", "insert", "t",
               "set code = 'taken'", "", true, true, {}}) ==
           dbms::DBStatus::OK);
    useSuccessfulTriggerExecutor();

    assert(g_engine.insert(
               database, "t", {{"id", "2"}, {"code", "fresh"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    g_engine.setTriggerExecutor({});
    assert(g_engine.query(database, "t", {}, {"id"}).size() == 1);
    const auto rids = g_engine.getSecondaryIndex(database, "t", "code")
                          ->searchMulti("taken");
    assert(rids.size() == 1);
    assert(g_engine.getSecondaryIndex(database, "t", "code")
               ->searchMulti("fresh")
               .empty());

    cleanupTestDb(testName);
}

void test_trigger_can_resolve_input_conflicts() {
    const std::string testName = "insert_trigger_unique_resolve";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    assert(g_engine.createTable(database, uniqueSchema("t")) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"code", "taken"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.createTrigger(database, {
               "rewrite_keys", "before", "insert", "t",
               "set id = 2; set code = 'free'", "", true, true, {}}) ==
           dbms::DBStatus::OK);
    useSuccessfulTriggerExecutor();

    assert(g_engine.insert(
               database, "t", {{"id", "1"}, {"code", "taken"}}) ==
           dbms::DBStatus::OK);
    g_engine.setTriggerExecutor({});
    assert(g_engine.query(database, "t", {"=id 1"}, {"code"}).size() == 1);
    assert(g_engine.query(database, "t", {"=id 2"}, {"code"}).size() == 1);
    assert(g_engine.getSecondaryIndex(database, "t", "code")
               ->searchMulti("taken")
               .size() == 1);
    assert(g_engine.getSecondaryIndex(database, "t", "code")
               ->searchMulti("free")
               .size() == 1);

    cleanupTestDb(testName);
}

void test_generated_unique_value_is_checked() {
    const std::string testName = "insert_generated_unique";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeIntColumn("a", false, 4));
    dbms::Column generated = dbms::makeIntColumn("bucket", true, 4);
    generated.generatedExpr = "a % 2";
    generated.generatedKind = 's';
    generated.isUnique = true;
    schema.append(generated);
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    assert(g_engine.insert(database, "t", {{"id", "1"}, {"a", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id", "2"}, {"a", "3"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.query(database, "t", {}, {"id"}).size() == 1);
    assert(g_engine.getSecondaryIndex(database, "t", "bucket")
               ->searchMulti("1")
               .size() == 1);

    cleanupTestDb(testName);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_trigger_created_unique_conflict_is_rejected();
    test_trigger_can_resolve_input_conflicts();
    test_generated_unique_value_is_checked();
    finalCleanupTestData();
    std::cout << "[INSERT FINAL UNIQUE] trigger/generated keys validated OK"
              << std::endl;
    return 0;
}
