#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <map>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "floating_zero";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema fixedSchema;
    fixedSchema.tablename = "fixed_values";
    fixedSchema.formatVersion = 2;
    fixedSchema.append(dbms::makeIntColumn("id", false, 4, true));
    fixedSchema.append(dbms::makeFloatColumn("float_value", true));
    fixedSchema.append(dbms::makeDoubleColumn("double_value", true));
    fixedSchema.uniqueConstraints.push_back({1, 2});
    fixedSchema.uniqueConstraintNames.push_back("fixed_values_pair_key");
    assert(g_engine.createTable(database, fixedSchema) == dbms::DBStatus::OK);

    assert(g_engine.insert(database, "fixed_values",
                           {{"id", "1"}, {"float_value", "0"},
                            {"double_value", "0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.query(database, "fixed_values", {"=id 1"},
                          {"float_value", "double_value"}) ==
           std::vector<std::string>{"0 0 "});

    // A decoded zero must participate in uniqueness checks as a value, not
    // disappear as though the column were NULL or omitted.
    assert(g_engine.insert(database, "fixed_values",
                           {{"id", "2"}, {"float_value", "0"},
                            {"double_value", "0"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);

    // The v2 null bitmap, rather than the floating-point payload, carries
    // SQL NULL.  NULL therefore remains distinct from a real zero.
    std::vector<std::map<std::string, std::string>> insertedRows;
    assert(g_engine.insert(database, "fixed_values",
                           {{"id", "3"}, {"float_value", "NULL"},
                            {"double_value", "NULL"}},
                           &insertedRows) ==
           dbms::DBStatus::OK);
    assert(insertedRows.size() == 1);
    assert(insertedRows.front().at("float_value").empty());
    assert(insertedRows.front().at("double_value").empty());
    int64_t nullRid = -1;
    assert(g_engine.getPKIndex(database, "fixed_values")->search("3", nullRid));
    assert(g_engine.isColumnNullByRid(database, "fixed_values", nullRid, 1));
    assert(g_engine.isColumnNullByRid(database, "fixed_values", nullRid, 2));
    const auto allRows = g_engine.query(database, "fixed_values", {},
                                        {"float_value", "double_value"});
    assert(allRows == (std::vector<std::string>{"0 0 ", "NULL NULL "}));

    assert(g_engine.insert(database, "fixed_values",
                           {{"id", "4"}, {"float_value", "1"},
                            {"double_value", "1"}}) ==
           dbms::DBStatus::OK);
    std::vector<std::map<std::string, std::string>> updatedRows;
    assert(g_engine.update(database, "fixed_values",
                           {{"float_value", "NULL"},
                            {"double_value", "NULL"}},
                           {"=id 4"}, &updatedRows) == dbms::DBStatus::OK);
    assert(updatedRows.size() == 1);
    assert(updatedRows.front().at("float_value").empty());
    assert(updatedRows.front().at("double_value").empty());

    // DISTINCT must keep a real zero separate from the NULL group.
    assert(g_engine.query(database, "fixed_values", {}, {"id"}, {}, false,
                          false, false, 0, {"float_value"}) ==
           (std::vector<std::string>{"1 ", "3 "}));

    std::vector<std::map<std::string, std::string>> deletedRows;
    assert(g_engine.remove(database, "fixed_values", {"=id 3"},
                           &deletedRows) == dbms::DBStatus::OK);
    assert(deletedRows.size() == 1);
    assert(deletedRows.front().at("float_value").empty());
    assert(deletedRows.front().at("double_value").empty());

    // Tables containing variable-length columns use a separate row builder;
    // their fixed floating-point fields must preserve zero as well.
    dbms::TableSchema variableSchema;
    variableSchema.tablename = "variable_values";
    variableSchema.formatVersion = 2;
    variableSchema.append(dbms::makeIntColumn("id", false, 4, true));
    variableSchema.append(dbms::makeVarCharColumn("note", false, 32));
    variableSchema.append(dbms::makeFloatColumn("float_value", false));
    variableSchema.append(dbms::makeDoubleColumn("double_value", false));
    assert(g_engine.createTable(database, variableSchema) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "variable_values",
                           {{"id", "1"}, {"note", "kept"},
                            {"float_value", "0"},
                            {"double_value", "0"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.query(database, "variable_values", {"=id 1"},
                          {"note", "float_value", "double_value"}) ==
           std::vector<std::string>{"kept 0 0 "});

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FLOATING ZERO] value/null distinction OK" << std::endl;
    return 0;
}
