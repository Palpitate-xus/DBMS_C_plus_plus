#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <string>

extern dbms::StorageEngine g_engine;

namespace {

struct NodeRow {
    bool found = false;
    bool parentIsNull = false;
    std::string parentId;
};

NodeRow nodeById(const std::string& database, const std::string& tableName,
                 const std::string& id) {
    const dbms::TableSchema table =
        g_engine.getTableSchema(database, tableName);
    assert(table.len == 2);
    NodeRow result;
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
            result.parentIsNull = g_engine.isColumnNullByRid(
                database, tableName, rid, 1);
            result.parentId = g_engine.extractColumnValue(
                row, table, 1, database, true);
        }));
    return result;
}

void createNodeTable(const std::string& database,
                     const std::string& tableName,
                     const std::string& onDelete,
                     const std::string& onUpdate) {
    dbms::TableSchema table;
    table.tablename = tableName;
    table.formatVersion = 2;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeIntColumn("parent_id", true, 4, false));
    assert(g_engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(g_engine.alterTableAddFKConstraint(
               database, tableName, tableName + "_parent_fk", {"parent_id"},
               tableName, {"id"}, onDelete, onUpdate) ==
           dbms::DBStatus::OK);
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "self_referencing_foreign_key";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    createNodeTable(database, "restrict_nodes", "restrict", "restrict");
    assert(g_engine.insert(database, "restrict_nodes", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "restrict_nodes",
                           {{"id", "2"}, {"parent_id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "restrict_nodes", {{"id", "10"}},
                           {"=id 1"}) == dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.remove(database, "restrict_nodes", {"=id 1"}) ==
           dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(nodeById(database, "restrict_nodes", "1").found);
    assert(nodeById(database, "restrict_nodes", "2").parentId == "1");

    createNodeTable(database, "cascade_nodes", "cascade", "cascade");
    assert(g_engine.insert(database, "cascade_nodes", {{"id", "100"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "cascade_nodes",
                           {{"id", "101"}, {"parent_id", "100"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "cascade_nodes",
                           {{"id", "102"}, {"parent_id", "101"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "cascade_nodes", {{"id", "110"}},
                           {"=id 100"}) == dbms::DBStatus::OK);
    assert(!nodeById(database, "cascade_nodes", "100").found);
    assert(nodeById(database, "cascade_nodes", "101").parentId == "110");
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(nodeById(database, "cascade_nodes", "100").found);
    assert(nodeById(database, "cascade_nodes", "101").parentId == "100");
    assert(g_engine.update(database, "cascade_nodes", {{"id", "110"}},
                           {"=id 100"}) == dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.remove(database, "cascade_nodes", {"=id 110"}) ==
           dbms::DBStatus::OK);
    assert(!nodeById(database, "cascade_nodes", "110").found);
    assert(!nodeById(database, "cascade_nodes", "101").found);
    assert(!nodeById(database, "cascade_nodes", "102").found);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(nodeById(database, "cascade_nodes", "110").found);
    assert(nodeById(database, "cascade_nodes", "101").parentId == "110");
    assert(nodeById(database, "cascade_nodes", "102").parentId == "101");
    assert(g_engine.remove(database, "cascade_nodes", {"=id 110"}) ==
           dbms::DBStatus::OK);
    assert(!nodeById(database, "cascade_nodes", "110").found);
    assert(!nodeById(database, "cascade_nodes", "101").found);
    assert(!nodeById(database, "cascade_nodes", "102").found);

    createNodeTable(database, "set_null_nodes", "setnull", "setnull");
    assert(g_engine.insert(database, "set_null_nodes", {{"id", "200"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "set_null_nodes",
                           {{"id", "201"}, {"parent_id", "200"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "set_null_nodes", {{"id", "210"}},
                           {"=id 200"}) == dbms::DBStatus::OK);
    const NodeRow updateSetNull =
        nodeById(database, "set_null_nodes", "201");
    assert(updateSetNull.found && updateSetNull.parentIsNull);
    assert(g_engine.insert(database, "set_null_nodes", {{"id", "220"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "set_null_nodes",
                           {{"id", "221"}, {"parent_id", "220"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.remove(database, "set_null_nodes", {"=id 220"}) ==
           dbms::DBStatus::OK);
    const NodeRow deleteSetNull =
        nodeById(database, "set_null_nodes", "221");
    assert(deleteSetNull.found && deleteSetNull.parentIsNull);

    createNodeTable(database, "self_loop_cascade", "cascade", "cascade");
    assert(g_engine.insert(database, "self_loop_cascade", {{"id", "300"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "self_loop_cascade",
                           {{"parent_id", "300"}}, {"=id 300"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.update(database, "self_loop_cascade", {{"id", "301"}},
                           {"=id 300"}) == dbms::DBStatus::OK);
    const NodeRow transactionalLoop =
        nodeById(database, "self_loop_cascade", "301");
    assert(transactionalLoop.found && transactionalLoop.parentId == "301");
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(nodeById(database, "self_loop_cascade", "300").parentId ==
           "300");
    assert(g_engine.update(database, "self_loop_cascade", {{"id", "301"}},
                           {"=id 300"}) == dbms::DBStatus::OK);
    const NodeRow cascadedLoop =
        nodeById(database, "self_loop_cascade", "301");
    assert(cascadedLoop.found && cascadedLoop.parentId == "301");
    assert(g_engine.remove(database, "self_loop_cascade", {"=id 301"}) ==
           dbms::DBStatus::OK);
    assert(!nodeById(database, "self_loop_cascade", "301").found);

    createNodeTable(database, "self_loop_set_null", "setnull", "setnull");
    assert(g_engine.insert(database, "self_loop_set_null", {{"id", "350"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "self_loop_set_null",
                           {{"parent_id", "350"}}, {"=id 350"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "self_loop_set_null", {{"id", "351"}},
                           {"=id 350"}) == dbms::DBStatus::OK);
    const NodeRow nulledLoop =
        nodeById(database, "self_loop_set_null", "351");
    assert(nulledLoop.found && nulledLoop.parentIsNull);

    createNodeTable(database, "self_loop_restrict", "restrict", "restrict");
    assert(g_engine.insert(database, "self_loop_restrict", {{"id", "400"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "self_loop_restrict",
                           {{"parent_id", "400"}}, {"=id 400"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "self_loop_restrict", {{"id", "401"}},
                           {"=id 400"}) ==
           dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(g_engine.update(database, "self_loop_restrict",
                           {{"id", "401"}, {"parent_id", "401"}},
                           {"=id 400"}) == dbms::DBStatus::OK);
    assert(nodeById(database, "self_loop_restrict", "401").parentId ==
           "401");
    assert(g_engine.remove(database, "self_loop_restrict", {"=id 401"}) ==
           dbms::DBStatus::OK);

    createNodeTable(database, "cycle_nodes", "cascade", "cascade");
    assert(g_engine.insert(database, "cycle_nodes", {{"id", "500"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(database, "cycle_nodes",
                           {{"id", "501"}, {"parent_id", "500"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.update(database, "cycle_nodes", {{"parent_id", "501"}},
                           {"=id 500"}) == dbms::DBStatus::OK);
    assert(g_engine.remove(database, "cycle_nodes", {"=id 500"}) ==
           dbms::DBStatus::OK);
    assert(!nodeById(database, "cycle_nodes", "500").found);
    assert(!nodeById(database, "cycle_nodes", "501").found);

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[SELF FOREIGN KEY] actions preserve referential integrity OK"
              << std::endl;
    return 0;
}
