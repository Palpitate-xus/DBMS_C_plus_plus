#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

void expectRejectedTriggerValue(const std::string& testName,
                                dbms::Column targetColumn,
                                const std::string& initialValue,
                                const std::string& triggerValue) {
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "t";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    targetColumn.dataName = "target";
    schema.append(targetColumn);
    schema.append(dbms::makeVarCharColumn("note", false, 32));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "t",
                           {{"id", "1"}, {"target", initialValue},
                            {"note", "before"}}) == dbms::DBStatus::OK);

    assert(g_engine.createTrigger(
               database,
               {"rewrite_target", "before", "update", "t",
                "set target = '" + triggerValue + "'", "", true, true,
                {}}) == dbms::DBStatus::OK);
    g_engine.setTriggerExecutor([](const std::string&) { return false; });
    const dbms::DBStatus status = g_engine.update(
        database, "t", {{"note", "after"}}, {"=id 1"});
    g_engine.setTriggerExecutor({});

    assert(status == dbms::DBStatus::INVALID_VALUE);
    assert(!g_engine.inTransaction());
    const auto targetRows =
        g_engine.query(database, "t", {"=id 1"}, {"target"});
    const auto noteRows =
        g_engine.query(database, "t", {"=id 1"}, {"note"});
    assert(targetRows.size() == 1 &&
           targetRows.front() == initialValue + " ");
    assert(noteRows.size() == 1 && noteRows.front() == "before ");
    cleanupTestDb(testName);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    expectRejectedTriggerValue(
        "update_final_integer", dbms::makeIntColumn("target", false, 4),
        "7", "not-an-integer");
    expectRejectedTriggerValue(
        "update_final_float", dbms::makeFloatColumn("target", false),
        "1.5", "not-a-float");
    expectRejectedTriggerValue(
        "update_final_json", dbms::makeJsonColumn("target", false),
        "{\"valid\":true}", "{broken}");

    dbms::Column enumColumn =
        dbms::makeVarCharColumn("target", false, 16);
    enumColumn.enumValues = {"red", "blue"};
    expectRejectedTriggerValue(
        "update_final_enum", enumColumn, "red", "green");

    finalCleanupTestData();
    std::cout << "[UPDATE FINAL VALIDATION] trigger values checked OK"
              << std::endl;
    return 0;
}
