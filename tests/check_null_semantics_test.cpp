#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

dbms::TableSchema checkedTable(const std::string& name, bool deferred = false) {
    dbms::TableSchema table;
    table.tablename = name;
    table.formatVersion = 2;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column value = dbms::makeIntColumn("value", true, 4, false);
    value.checkExpr = "value > 0";
    value.checkConstraintName = name + "_value_check";
    value.deferrable = deferred;
    value.initiallyDeferred = deferred;
    table.append(value);
    return table;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "check_null_semantics";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    assert(g_engine.createTable(database, checkedTable("immediate_check")) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "immediate_check",
                           {{"id", "1"}, {"value", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "immediate_check",
                           {{"id", "2"}, {"value", "2"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "immediate_check", {{"value", "NULL"}},
                           {"=id 2"}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "immediate_check",
                           {{"id", "3"}, {"value", "-1"}}) ==
           dbms::DBStatus::CHECK_VIOLATION);

    dbms::TableSchema altered;
    altered.tablename = "altered_check";
    altered.formatVersion = 2;
    altered.append(dbms::makeIntColumn("id", false, 4, true));
    altered.append(dbms::makeIntColumn("value", true, 4, false));
    assert(g_engine.createTable(database, altered) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "altered_check",
                           {{"id", "1"}, {"value", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddCheckConstraint(
               database, "altered_check", "altered_value_check",
               "value > 0") == dbms::DBStatus::OK);

    assert(g_engine.createTable(database, checkedTable("deferred_check", true)) ==
           dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "deferred_check",
                           {{"id", "1"}, {"value", "NULL"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[CHECK NULL] UNKNOWN satisfies immediate/deferred/ALTER checks OK"
              << std::endl;
    return 0;
}
