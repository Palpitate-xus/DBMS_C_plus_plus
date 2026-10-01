#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

struct RowValue {
    bool found = false;
    bool isNull = false;
    std::string value;
};

RowValue valueById(const std::string& database, const std::string& tableName,
                   const std::string& id, const std::string& columnName) {
    const dbms::TableSchema table =
        g_engine.getTableSchema(database, tableName);
    size_t valueColumn = table.len;
    for (size_t column = 0; column < table.len; ++column) {
        if (table.cols[column].dataName == columnName) {
            valueColumn = column;
            break;
        }
    }
    assert(valueColumn < table.len);

    RowValue result;
    assert(g_engine.forEachRow(
        database, tableName,
        [&](uint32_t pageId, uint16_t slotId, const char* data,
            size_t length) {
            const std::string row(data, length);
            if (g_engine.extractColumnValue(
                    row, table, 0, database, true) != id) {
                return;
            }
            result.found = true;
            const int64_t rid =
                dbms::StorageEngine::encodeRid(pageId, slotId);
            result.isNull = table.cols[valueColumn].isNull &&
                g_engine.isColumnNullByRid(
                    database, tableName, rid, valueColumn);
            result.value = g_engine.extractColumnValue(
                row, table, valueColumn, database, true);
        }));
    return result;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "foreign_key_referenced_column";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema parent;
    parent.tablename = "parent";
    parent.formatVersion = 2;
    parent.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column code = dbms::makeIntColumn("code", false, 4, false);
    code.isUnique = true;
    parent.append(code);
    assert(g_engine.createTable(database, parent) == dbms::DBStatus::OK);

    dbms::TableSchema child;
    child.tablename = "child";
    child.formatVersion = 2;
    child.append(dbms::makeIntColumn("id", false, 4, true));
    child.append(dbms::makeIntColumn("parent_code", false, 4, false));
    assert(g_engine.createTable(database, child) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "child", "child_parent_code_fk", {"parent_code"},
               "parent", {"code"}) == dbms::DBStatus::OK);

    // The PK value and referenced UNIQUE value deliberately differ. Looking
    // up parent_code in parent.id reverses both expected outcomes below.
    assert(g_engine.insert(database, "parent", {{"id", "7"}, {"code", "9"}}) ==
           dbms::DBStatus::OK);
    const dbms::DBStatus validReference = g_engine.insert(
        database, "child", {{"id", "1"}, {"parent_code", "9"}});
    const dbms::DBStatus missingReference = g_engine.insert(
        database, "child", {{"id", "2"}, {"parent_code", "7"}});
    assert(validReference == dbms::DBStatus::OK);
    assert(missingReference == dbms::DBStatus::FOREIGN_KEY_VIOLATION);

    // Referential actions must compare the declared UNIQUE target (code),
    // not the unrelated primary key (id).
    assert(g_engine.update(database, "parent", {{"code", "10"}},
                           {"=id 7"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    const RowValue restrictedParent =
        valueById(database, "parent", "7", "code");
    assert(restrictedParent.found && restrictedParent.value == "9");
    assert(g_engine.remove(database, "parent", {"=id 7"}) ==
           dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(valueById(database, "parent", "7", "code").found);

    dbms::TableSchema cascadeParent;
    cascadeParent.tablename = "cascade_parent";
    cascadeParent.formatVersion = 2;
    cascadeParent.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column cascadeCode =
        dbms::makeIntColumn("code", false, 4, false);
    cascadeCode.isUnique = true;
    cascadeParent.append(cascadeCode);
    assert(g_engine.createTable(database, cascadeParent) ==
           dbms::DBStatus::OK);

    dbms::TableSchema cascadeChild;
    cascadeChild.tablename = "cascade_child";
    cascadeChild.formatVersion = 2;
    cascadeChild.append(dbms::makeIntColumn("id", false, 4, true));
    cascadeChild.append(
        dbms::makeIntColumn("parent_code", false, 4, false));
    assert(g_engine.createTable(database, cascadeChild) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "cascade_child", "cascade_code_fk",
               {"parent_code"}, "cascade_parent", {"code"}, "cascade",
               "cascade") == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "cascade_parent",
                           {{"id", "20"}, {"code", "200"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "cascade_child",
                           {{"id", "21"}, {"parent_code", "200"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "cascade_parent", {{"code", "201"}},
                           {"=id 20"}) == dbms::DBStatus::OK);
    const RowValue transactionalCascade =
        valueById(database, "cascade_child", "21", "parent_code");
    assert(transactionalCascade.found && !transactionalCascade.isNull &&
           transactionalCascade.value == "201");
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(valueById(database, "cascade_parent", "20", "code").value ==
           "200");
    assert(valueById(database, "cascade_child", "21", "parent_code").value ==
           "200");
    assert(g_engine.update(database, "cascade_parent", {{"code", "201"}},
                           {"=id 20"}) == dbms::DBStatus::OK);
    const RowValue cascadedValue =
        valueById(database, "cascade_child", "21", "parent_code");
    assert(cascadedValue.found && !cascadedValue.isNull &&
           cascadedValue.value == "201");
    assert(g_engine.remove(database, "cascade_parent", {"=id 20"}) ==
           dbms::DBStatus::OK);
    assert(!valueById(database, "cascade_child", "21", "parent_code")
                .found);

    dbms::TableSchema nullParent;
    nullParent.tablename = "null_parent";
    nullParent.formatVersion = 2;
    nullParent.append(dbms::makeIntColumn("id", false, 4, true));
    dbms::Column nullCode = dbms::makeIntColumn("code", false, 4, false);
    nullCode.isUnique = true;
    nullParent.append(nullCode);
    assert(g_engine.createTable(database, nullParent) ==
           dbms::DBStatus::OK);

    dbms::TableSchema nullChild;
    nullChild.tablename = "null_child";
    nullChild.formatVersion = 2;
    nullChild.append(dbms::makeIntColumn("id", false, 4, true));
    nullChild.append(dbms::makeIntColumn("parent_code", true, 4, false));
    assert(g_engine.createTable(database, nullChild) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "null_child", "null_code_fk", {"parent_code"},
               "null_parent", {"code"}, "setnull", "setnull") ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "null_parent",
                           {{"id", "30"}, {"code", "300"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "null_parent",
                           {{"id", "40"}, {"code", "400"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "null_child",
                           {{"id", "31"}, {"parent_code", "300"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "null_child",
                           {{"id", "41"}, {"parent_code", "400"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "null_parent", {{"code", "301"}},
                           {"=id 30"}) == dbms::DBStatus::OK);
    const RowValue updateSetNull =
        valueById(database, "null_child", "31", "parent_code");
    assert(updateSetNull.found && updateSetNull.isNull);
    assert(g_engine.remove(database, "null_parent", {"=id 40"}) ==
           dbms::DBStatus::OK);
    const RowValue deleteSetNull =
        valueById(database, "null_child", "41", "parent_code");
    assert(deleteSetNull.found && deleteSetNull.isNull);

    dbms::TableSchema compositeUniqueParent;
    compositeUniqueParent.tablename = "composite_unique_parent";
    compositeUniqueParent.formatVersion = 2;
    compositeUniqueParent.append(
        dbms::makeIntColumn("id", false, 4, true));
    compositeUniqueParent.append(
        dbms::makeIntColumn("region", false, 4, false));
    compositeUniqueParent.append(
        dbms::makeIntColumn("code", false, 4, false));
    compositeUniqueParent.uniqueConstraints.push_back({1, 2});
    compositeUniqueParent.uniqueConstraintNames.push_back(
        "composite_region_code_key");
    assert(g_engine.createTable(database, compositeUniqueParent) ==
           dbms::DBStatus::OK);

    dbms::TableSchema compositeUniqueChild;
    compositeUniqueChild.tablename = "composite_unique_child";
    compositeUniqueChild.formatVersion = 2;
    compositeUniqueChild.append(
        dbms::makeIntColumn("id", false, 4, true));
    compositeUniqueChild.append(
        dbms::makeIntColumn("parent_region", false, 4, false));
    compositeUniqueChild.append(
        dbms::makeIntColumn("parent_code", false, 4, false));
    assert(g_engine.createTable(database, compositeUniqueChild) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "composite_unique_child", "composite_unique_fk",
               {"parent_region", "parent_code"},
               "composite_unique_parent", {"region", "code"}, "cascade",
               "cascade") == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "composite_unique_parent",
                           {{"id", "50"}, {"region", "5"},
                            {"code", "500"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "composite_unique_child",
                           {{"id", "51"}, {"parent_region", "5"},
                            {"parent_code", "500"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "composite_unique_parent",
                           {{"region", "6"}, {"code", "501"}},
                           {"=id 50"}) == dbms::DBStatus::OK);
    const RowValue cascadedRegion = valueById(
        database, "composite_unique_child", "51", "parent_region");
    const RowValue cascadedCode = valueById(
        database, "composite_unique_child", "51", "parent_code");
    assert(cascadedRegion.found && cascadedRegion.value == "6");
    assert(cascadedCode.found && cascadedCode.value == "501");
    assert(g_engine.remove(database, "composite_unique_parent", {"=id 50"}) ==
           dbms::DBStatus::OK);
    assert(!valueById(database, "composite_unique_child", "51",
                      "parent_code").found);

    // A referenced relation is not required to have a primary key when the
    // declared target itself is UNIQUE.
    dbms::TableSchema uniqueOnlyParent;
    uniqueOnlyParent.tablename = "unique_only_parent";
    uniqueOnlyParent.formatVersion = 2;
    dbms::Column uniqueOnlyCode =
        dbms::makeIntColumn("code", false, 4, false);
    uniqueOnlyCode.isUnique = true;
    uniqueOnlyParent.append(uniqueOnlyCode);
    assert(g_engine.createTable(database, uniqueOnlyParent) ==
           dbms::DBStatus::OK);

    dbms::TableSchema uniqueOnlyChild;
    uniqueOnlyChild.tablename = "unique_only_child";
    uniqueOnlyChild.formatVersion = 2;
    uniqueOnlyChild.append(dbms::makeIntColumn("id", false, 4, true));
    uniqueOnlyChild.append(
        dbms::makeIntColumn("parent_code", false, 4, false));
    assert(g_engine.createTable(database, uniqueOnlyChild) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "unique_only_child", "unique_only_code_fk",
               {"parent_code"}, "unique_only_parent", {"code"}, "cascade",
               "cascade") == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_only_parent", {{"code", "700"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "unique_only_child",
                           {{"id", "71"}, {"parent_code", "700"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "unique_only_parent", {{"code", "701"}},
                           {"=code 700"}) == dbms::DBStatus::OK);
    assert(valueById(database, "unique_only_child", "71", "parent_code")
               .value == "701");
    assert(g_engine.remove(database, "unique_only_parent", {"=code 701"}) ==
           dbms::DBStatus::OK);
    assert(!valueById(database, "unique_only_child", "71", "parent_code")
                .found);

    dbms::TableSchema tablePrimaryParent;
    tablePrimaryParent.tablename = "table_primary_parent";
    tablePrimaryParent.formatVersion = 2;
    tablePrimaryParent.append(dbms::makeIntColumn("id", false, 4, true));
    tablePrimaryParent.pkColIndices = {0};
    assert(g_engine.createTable(database, tablePrimaryParent) ==
           dbms::DBStatus::OK);

    dbms::TableSchema tablePrimaryChild;
    tablePrimaryChild.tablename = "table_primary_child";
    tablePrimaryChild.formatVersion = 2;
    tablePrimaryChild.append(dbms::makeIntColumn("id", false, 4, true));
    tablePrimaryChild.append(dbms::makeIntColumn("parent_id", false, 4, false));
    assert(g_engine.createTable(database, tablePrimaryChild) ==
           dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, "table_primary_child", "table_primary_child_fk",
               {"parent_id"}, "table_primary_parent", {"id"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "table_primary_parent", {{"id", "42"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "table_primary_child",
                           {{"id", "10"}, {"parent_id", "42"}}) ==
           dbms::DBStatus::OK);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FOREIGN KEY REFERENCED COLUMN] non-PK UNIQUE actions OK"
              << std::endl;
    return 0;
}
