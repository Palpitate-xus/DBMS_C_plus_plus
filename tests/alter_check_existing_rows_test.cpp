#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "alter_check_existing_rows";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "measurements";
    table.formatVersion = 2;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("value", false, 4, false));
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "measurements",
                           {{"id", "1"}, {"value", "-1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "measurements",
                           {{"id", "2"}, {"value", "2"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.alterTableAddCheckConstraint(
               database, "measurements", "measurements_value_check",
               "value > 0") == dbms::DBStatus::CHECK_VIOLATION);
    const dbms::TableSchema rejected =
        g_engine.getTableSchema(database, "measurements");
    assert(rejected.cols[1].checkExpr.empty());
    assert(rejected.cols[1].checkConstraintName.empty());

    assert(g_engine.update(database, "measurements", {{"value", "1"}},
                           {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddCheckConstraint(
               database, "measurements", "measurements_value_check",
               "value > 0") == dbms::DBStatus::OK);
    assert(g_engine.update(database, "measurements", {{"value", "0"}},
                           {"=id 2"}) == dbms::DBStatus::CHECK_VIOLATION);
    assert(g_engine.query(database, "measurements", {"=value 2"}, {"id"}) ==
           std::vector<std::string>{"2 "});

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[ALTER CHECK] existing rows validated before publication OK"
              << std::endl;
    return 0;
}
