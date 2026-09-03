#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void addPrimaryKey(dbms::TableSchema& schema) {
    schema.append(dbms::makeIntColumn("id", false, 4, true));
}

void addRewriteTrigger(const std::string& database,
                       const std::string& table,
                       const std::string& name,
                       const std::string& action) {
    assert(g_engine.createTrigger(database, {
               name, "before", "insert", table, action, "", true, true,
               {}}) == dbms::DBStatus::OK);
}

void test_final_trigger_values_are_validated() {
    const std::string testName = "insert_final_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema filled;
    filled.tablename = "filled";
    filled.formatVersion = 2;
    addPrimaryKey(filled);
    filled.append(dbms::makeVarCharColumn("required", false, 20));
    assert(g_engine.createTable(database, filled) == dbms::DBStatus::OK);
    addRewriteTrigger(
        database, "filled", "fill_required", "set required = 'ready'");

    dbms::TableSchema invalidInteger;
    invalidInteger.tablename = "invalid_integer";
    invalidInteger.formatVersion = 2;
    addPrimaryKey(invalidInteger);
    invalidInteger.append(dbms::makeIntColumn("quantity", false, 4));
    assert(g_engine.createTable(database, invalidInteger) ==
           dbms::DBStatus::OK);
    addRewriteTrigger(
        database, "invalid_integer", "break_integer",
        "set quantity = 'not-a-number'");

    dbms::TableSchema invalidEnum;
    invalidEnum.tablename = "invalid_enum";
    invalidEnum.formatVersion = 2;
    addPrimaryKey(invalidEnum);
    dbms::Column color = dbms::makeVarCharColumn("color", false, 10);
    color.enumValues = {"red", "blue"};
    invalidEnum.append(color);
    assert(g_engine.createTable(database, invalidEnum) == dbms::DBStatus::OK);
    addRewriteTrigger(
        database, "invalid_enum", "break_enum", "set color = 'green'");

    g_engine.setTriggerExecutor([](const std::string&) { return false; });
    assert(g_engine.insert(database, "filled", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    const auto filledRows = g_engine.query(
        database, "filled", {"=id 1"}, {"required"});
    assert(filledRows.size() == 1);
    assert(filledRows.front().find("ready") != std::string::npos);

    assert(g_engine.insert(
               database, "invalid_integer",
               {{"id", "1"}, {"quantity", "5"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.query(database, "invalid_integer", {}, {"id"}).empty());

    assert(g_engine.insert(
               database, "invalid_enum", {{"id", "1"}, {"color", "red"}}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.query(database, "invalid_enum", {}, {"id"}).empty());
    g_engine.setTriggerExecutor({});

    cleanupTestDb(testName);
}

void test_nonnull_stored_generated_value_is_checked_after_generation() {
    const std::string testName = "insert_generated_not_null";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    addPrimaryKey(schema);
    schema.append(dbms::makeIntColumn("base", false, 4));
    dbms::Column generated = dbms::makeIntColumn("derived", false, 4);
    generated.generatedExpr = "base + 1";
    generated.generatedKind = 's';
    schema.append(generated);
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t", {{"id", "1"}, {"base", "4"}}) ==
           dbms::DBStatus::OK);
    const auto rows = g_engine.query(database, "t", {"=id 1"}, {"derived"});
    assert(rows.size() == 1 && rows.front().find("5") != std::string::npos);

    cleanupTestDb(testName);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_final_trigger_values_are_validated();
    test_nonnull_stored_generated_value_is_checked_after_generation();
    finalCleanupTestData();
    std::cout << "[INSERT FINAL VALIDATION] trigger/generated rows checked OK"
              << std::endl;
    return 0;
}
