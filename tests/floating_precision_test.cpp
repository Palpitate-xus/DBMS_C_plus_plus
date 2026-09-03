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
    const std::string testName = "floating_precision";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "measurements";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeVarCharColumn("note", false, 32));
    schema.append(dbms::makeFloatColumn("single_value", false));
    schema.append(dbms::makeDoubleColumn("double_value", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    const std::string singleValue = "1.234567";
    const std::string doubleValue = "1.2345678901234567";
    assert(g_engine.insert(database, "measurements",
                           {{"id", "1"}, {"note", "before"},
                            {"single_value", singleValue},
                            {"double_value", doubleValue}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.query(database, "measurements", {"=id 1"},
                          {"single_value", "double_value"}) ==
           std::vector<std::string>{singleValue + " " + doubleValue + " "});
    assert(g_engine.query(database, "measurements",
                          {"=single_value " + singleValue}, {"id"}) ==
           std::vector<std::string>{"1 "});
    assert(g_engine.query(database, "measurements",
                          {"=double_value " + doubleValue}, {"id"}) ==
           std::vector<std::string>{"1 "});

    // UPDATE reconstructs the complete tuple.  Reading an unchanged float
    // through a six-digit formatter must not silently round it before the
    // new tuple version is written.
    assert(g_engine.update(database, "measurements", {{"note", "after"}},
                           {"=id 1"}) == dbms::DBStatus::OK);
    assert(g_engine.query(database, "measurements", {"=id 1"},
                          {"single_value", "double_value"}) ==
           std::vector<std::string>{singleValue + " " + doubleValue + " "});

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FLOATING PRECISION] shortest round trip OK" << std::endl;
    return 0;
}
