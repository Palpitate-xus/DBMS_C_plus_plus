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
    const std::string testName = "floating_special_values";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "special_values";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeFloatColumn("single_value", false));
    schema.append(dbms::makeDoubleColumn("double_value", false));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    const std::vector<std::vector<std::string>> values = {
        {"1", "1.5", "1.5"},
        {"2", "Infinity", "Infinity"},
        {"3", "-Infinity", "-Infinity"},
        {"4", "NaN", "NaN"},
    };
    for (const auto& value : values) {
        assert(g_engine.insert(database, "special_values",
                               {{"id", value[0]},
                                {"single_value", value[1]},
                                {"double_value", value[2]}}) ==
               dbms::DBStatus::OK);
    }

    assert(g_engine.query(database, "special_values", {"=id 2"},
                          {"single_value", "double_value"}) ==
           std::vector<std::string>{"Infinity Infinity "});
    assert(g_engine.query(database, "special_values", {"=id 3"},
                          {"single_value", "double_value"}) ==
           std::vector<std::string>{"-Infinity -Infinity "});
    assert(g_engine.query(database, "special_values", {"=id 4"},
                          {"single_value", "double_value"}) ==
           std::vector<std::string>{"NaN NaN "});

    // PostgreSQL defines NaN as equal to itself and greater than every
    // non-NaN floating value so B-tree comparisons remain total.
    assert(g_engine.query(database, "special_values", {"=single_value NaN"},
                          {"id"}) == std::vector<std::string>{"4 "});
    assert(g_engine.query(database, "special_values", {"=double_value NaN"},
                          {"id"}) == std::vector<std::string>{"4 "});
    assert(g_engine.query(database, "special_values", {">double_value Infinity"},
                          {"id"}) == std::vector<std::string>{"4 "});
    assert(g_engine.query(database, "special_values", {"<=double_value Infinity"},
                          {"id"}) ==
           (std::vector<std::string>{"1 ", "2 ", "3 "}));
    assert(g_engine.query(database, "special_values", {"!=double_value NaN"},
                          {"id"}) ==
           (std::vector<std::string>{"1 ", "2 ", "3 "}));
    assert(g_engine.query(database, "special_values",
                          {"betweendouble_value -Infinity NaN"}, {"id"}) ==
           (std::vector<std::string>{"1 ", "2 ", "3 ", "4 "}));

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FLOATING SPECIAL] PostgreSQL ordering/output OK"
              << std::endl;
    return 0;
}
