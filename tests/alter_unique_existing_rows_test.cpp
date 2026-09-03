#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

dbms::TableSchema baseTable(const std::string& name) {
    dbms::TableSchema table;
    table.tablename = name;
    table.formatVersion = 2;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    return table;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "alter_unique_existing_rows";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema single = baseTable("single_value");
    single.append(dbms::makeIntColumn("value", false, 4, false));
    assert(g_engine.createTable(database, single) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "single_value",
                           {{"id", "1"}, {"value", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "single_value",
                           {{"id", "2"}, {"value", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddUniqueConstraint(
               database, "single_value", "single_value_key", {"value"}) ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.getTableSchema(database, "single_value")
               .uniqueConstraints.empty());

    assert(g_engine.update(database, "single_value", {{"value", "8"}},
                           {"=id 2"}) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddUniqueConstraint(
               database, "single_value", "single_value_key", {"value"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "single_value",
                           {{"id", "3"}, {"value", "7"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    dbms::TableSchema composite = baseTable("composite_value");
    composite.append(dbms::makeIntColumn("left_value", false, 4, false));
    composite.append(dbms::makeIntColumn("right_value", false, 4, false));
    assert(g_engine.createTable(database, composite) == dbms::DBStatus::OK);
    for (const char* id : {"1", "2"}) {
        assert(g_engine.insert(
                   database, "composite_value",
                   {{"id", id}, {"left_value", "10"},
                    {"right_value", "20"}}) == dbms::DBStatus::OK);
    }
    assert(g_engine.alterTableAddUniqueConstraint(
               database, "composite_value", "composite_value_key",
               {"left_value", "right_value"}) ==
           dbms::DBStatus::INVALID_VALUE);

    dbms::TableSchema nullable = baseTable("nullable_value");
    nullable.append(dbms::makeIntColumn("value", true, 4, false));
    assert(g_engine.createTable(database, nullable) == dbms::DBStatus::OK);
    for (const char* id : {"1", "2"}) {
        assert(g_engine.insert(database, "nullable_value",
                               {{"id", id}, {"value", "NULL"}}) ==
               dbms::DBStatus::OK);
    }
    assert(g_engine.alterTableAddUniqueConstraint(
               database, "nullable_value", "nullable_value_key", {"value"}) ==
           dbms::DBStatus::OK);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[ALTER UNIQUE] existing rows validated before publication OK"
              << std::endl;
    return 0;
}
