#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "numeric_predicate_validation";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "numbers";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeFloatColumn("single_value", false));
    schema.append(dbms::makeDoubleColumn("double_value", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "numbers",
                           {{"id", "1"}, {"single_value", "1.25"},
                            {"double_value", "2.5"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "numbers",
                           {{"id", "2"}, {"single_value", "3.5"},
                            {"double_value", "4.5"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.query(database, "numbers", {"=single_value 1.25"},
                          {"id"}) == std::vector<std::string>{"1 "});
    assert(g_engine.query(database, "numbers", {"=double_value 2.5"},
                          {"id"}) == std::vector<std::string>{"1 "});

    // std::stof/std::stod accept a valid prefix.  A malformed predicate must
    // evaluate as invalid/UNKNOWN, not silently compare against that prefix.
    assert(g_engine.query(database, "numbers",
                          {"=single_value 1.25junk"}, {"id"}).empty());
    assert(g_engine.query(database, "numbers",
                          {"=double_value 2.5junk"}, {"id"}).empty());
    assert(g_engine.query(database, "numbers",
                          {"betweensingle_value 1junk 2"}, {"id"}).empty());
    assert(g_engine.query(database, "numbers",
                          {"betweendouble_value 2 3junk"}, {"id"}).empty());
    assert(g_engine.query(database, "numbers",
                          {"notbetweendouble_value 2junk 3"}, {"id"}).empty());

    // Integer BETWEEN accepts decimal bounds, but those bounds still need to
    // be consumed completely before numeric comparison is allowed.
    assert(g_engine.query(database, "numbers", {"betweenid 1junk 2"},
                          {"id"}).empty());

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[NUMERIC PREDICATE] strict literal consumption OK"
              << std::endl;
    return 0;
}
