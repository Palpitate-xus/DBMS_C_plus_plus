#include "Config.h"
#include "TableManage.h"
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

dbms::Config g_config;

using namespace dbms;
namespace fs = std::filesystem;

namespace {

constexpr const char* kDatabase = "foreign_key_action_dml_db";
constexpr const char* kRenameDatabase = "foreign_key_rename_db";
const std::string kPayload(5000, 'p');

void cleanup() {
    std::error_code error;
    fs::remove_all(kDatabase, error);
    error.clear();
    fs::remove_all(kRenameDatabase, error);
    error.clear();
    fs::remove_all("info/.prepared", error);
    error.clear();
    fs::remove(".txnid", error);
}

TableSchema keyedTable(const std::string& name) {
    TableSchema table;
    table.tablename = name;
    table.formatVersion = 2;
    table.append(makeIntColumn("id", false, 4, true));
    return table;
}

void createFixture(StorageEngine& engine) {
    assert(engine.createDatabase(kDatabase) == DBStatus::OK);

    TableSchema parent = keyedTable("parent");
    assert(engine.createTable(kDatabase, parent) == DBStatus::OK);

    TableSchema cascadeChild = keyedTable("cascade_child");
    cascadeChild.append(makeIntColumn("parent_id", false, 4, false));
    assert(engine.createTable(kDatabase, cascadeChild) == DBStatus::OK);
    assert(engine.alterTableAddFKConstraint(
               kDatabase, "cascade_child", "cascade_child_parent_fk",
               {"parent_id"}, "parent", {"id"}, "cascade", "cascade") ==
           DBStatus::OK);

    TableSchema grandchild = keyedTable("grandchild");
    grandchild.append(makeIntColumn("child_id", false, 4, false));
    assert(engine.createTable(kDatabase, grandchild) == DBStatus::OK);
    assert(engine.alterTableAddFKConstraint(
               kDatabase, "grandchild", "grandchild_child_fk",
               {"child_id"}, "cascade_child", {"id"}, "cascade",
               "cascade") == DBStatus::OK);

    TableSchema nullableChild = keyedTable("nullable_child");
    nullableChild.append(makeIntColumn("parent_id", true, 4, false));
    nullableChild.append(makeVarCharColumn("payload", false, 12000, false));
    assert(engine.createTable(kDatabase, nullableChild) == DBStatus::OK);
    assert(engine.alterTableAddFKConstraint(
               kDatabase, "nullable_child", "nullable_child_parent_fk",
               {"parent_id"}, "parent", {"id"}, "setnull", "cascade") ==
           DBStatus::OK);

    for (const std::string table : {"cascade_child", "nullable_child"}) {
        assert(engine.createIndex(kDatabase, table, "parent_id") ==
               DBStatus::OK);
        assert(engine.createHashIndex(kDatabase, table, "parent_id") ==
               DBStatus::OK);
        assert(engine.createBloomIndex(kDatabase, table, "parent_id") ==
               DBStatus::OK);
        assert(engine.createCompositeIndex(
                   kDatabase, table, {"parent_id", "id"},
                   "parent_id_id_idx") == DBStatus::OK);
    }
    assert(engine.createIndex(kDatabase, "grandchild", "child_id") ==
           DBStatus::OK);
    assert(engine.createHashIndex(kDatabase, "grandchild", "child_id") ==
           DBStatus::OK);

    TableSchema compositeParent;
    compositeParent.tablename = "composite_parent";
    compositeParent.formatVersion = 2;
    compositeParent.append(makeIntColumn("a", false, 4, true));
    compositeParent.append(makeIntColumn("b", false, 4, true));
    compositeParent.pkColIndices = {0, 1};
    assert(engine.createTable(kDatabase, compositeParent) == DBStatus::OK);

    TableSchema compositeChild = keyedTable("composite_child");
    compositeChild.append(makeIntColumn("parent_a", false, 4, false));
    compositeChild.append(makeIntColumn("parent_b", false, 4, false));
    assert(engine.createTable(kDatabase, compositeChild) == DBStatus::OK);
    assert(engine.alterTableAddFKConstraint(
               kDatabase, "composite_child", "composite_child_parent_fk",
               {"parent_a", "parent_b"}, "composite_parent", {"a", "b"},
               "restrict", "restrict") == DBStatus::OK);
}

void insertFamily(StorageEngine& engine, const std::string& parentId,
                  const std::string& childId,
                  const std::string& nullableId,
                  const std::string& grandchildId) {
    assert(engine.insert(kDatabase, "parent", {{"id", parentId}}) ==
           DBStatus::OK);
    assert(engine.insert(
               kDatabase, "cascade_child",
               {{"id", childId}, {"parent_id", parentId}}) == DBStatus::OK);
    assert(engine.insert(
               kDatabase, "nullable_child",
               {{"id", nullableId}, {"parent_id", parentId},
                {"payload", kPayload}}) == DBStatus::OK);
    assert(engine.insert(
               kDatabase, "grandchild",
               {{"id", grandchildId}, {"child_id", childId}}) ==
           DBStatus::OK);
}

int64_t primaryRid(StorageEngine& engine, const std::string& table,
                   const std::string& id) {
    BPTree* index = engine.getPKIndex(kDatabase, table);
    assert(index != nullptr);
    int64_t rid = -1;
    assert(index->search(id, rid));
    return rid;
}

bool primaryMissing(StorageEngine& engine, const std::string& table,
                    const std::string& id) {
    BPTree* index = engine.getPKIndex(kDatabase, table);
    assert(index != nullptr);
    int64_t rid = -1;
    return !index->search(id, rid);
}

void assertIndexedParent(StorageEngine& engine, const std::string& table,
                         const std::string& id,
                         const std::string& parentId,
                         bool checkBloom = true) {
    const int64_t rid = primaryRid(engine, table, id);
    BPTree* secondary = engine.getSecondaryIndex(
        kDatabase, table, "parent_id");
    HashIndex* hash = engine.getHashIndex(kDatabase, table, "parent_id");
    BloomIndex* bloom = engine.getBloomIndex(kDatabase, table, "parent_id");
    BPTree* composite = engine.getCompositeIndexTree(
        kDatabase, table, "parent_id_id_idx");
    assert(secondary && hash && bloom && composite);
    assert(secondary->searchMulti(parentId) == std::vector<int64_t>{rid});
    assert(hash->search(parentId) == std::vector<int64_t>{rid});
    if (checkBloom) {
        const auto bloomRows = bloom->search(parentId);
        if (bloomRows != std::vector<int64_t>{rid}) {
            std::cerr << "bloom mismatch table=" << table << " id=" << id
                      << " parent=" << parentId << " rid=" << rid << '\n';
        }
        assert(bloomRows == std::vector<int64_t>{rid});
    }
    assert(composite->searchMulti(parentId + '\x01' + id) ==
           std::vector<int64_t>{rid});
}

std::map<std::string, std::string> rowById(
    StorageEngine& engine, const std::string& table,
    const std::string& wantedId) {
    const TableSchema schema = engine.getTableSchema(kDatabase, table);
    std::map<std::string, std::string> result;
    assert(engine.forEachRow(
        kDatabase, table,
        [&](uint32_t, uint16_t, const char* data, size_t length) {
            const std::string row(data, length);
            if (engine.extractColumnValue(
                    row, schema, 0, kDatabase, true) != wantedId) {
                return;
            }
            for (size_t column = 0; column < schema.len; ++column) {
                result[schema.cols[column].dataName] =
                    engine.extractColumnValue(
                        row, schema, column, kDatabase, true);
            }
        }));
    return result;
}

void assertSetNullState(StorageEngine& engine, const std::string& id,
                        const std::string& oldParentId) {
    const auto row = rowById(engine, "nullable_child", id);
    assert(!row.empty());
    assert(row.at("parent_id").empty());
    assert(row.at("payload") == kPayload);

    const int64_t rid = primaryRid(engine, "nullable_child", id);
    assert(engine.isColumnNullByRid(
        kDatabase, "nullable_child", rid, 1));
    BPTree* secondary = engine.getSecondaryIndex(
        kDatabase, "nullable_child", "parent_id");
    HashIndex* hash = engine.getHashIndex(
        kDatabase, "nullable_child", "parent_id");
    BloomIndex* bloom = engine.getBloomIndex(
        kDatabase, "nullable_child", "parent_id");
    BPTree* composite = engine.getCompositeIndexTree(
        kDatabase, "nullable_child", "parent_id_id_idx");
    assert(secondary && hash && bloom && composite);
    assert(secondary->searchMulti(oldParentId).empty());
    assert(hash->search(oldParentId).empty());
    assert(bloom->search(oldParentId).empty());
    assert(composite->searchMulti(id) == std::vector<int64_t>{rid});
    assert(composite->searchMulti(oldParentId + '\x01' + id).empty());
}

void testAutocommitActions(StorageEngine& engine) {
    insertFamily(engine, "1", "100", "101", "1000");
    assertIndexedParent(engine, "cascade_child", "100", "1");
    assertIndexedParent(engine, "nullable_child", "101", "1");

    assert(engine.update(
               kDatabase, "cascade_child", {{"parent_id", "999"}},
               {"=id 100"}) == DBStatus::FOREIGN_KEY_VIOLATION);
    assert(rowById(engine, "cascade_child", "100").at("parent_id") == "1");
    assertIndexedParent(engine, "cascade_child", "100", "1");
    assert(engine.insert(kDatabase, "parent", {{"id", "3"}}) == DBStatus::OK);
    assert(engine.update(
               kDatabase, "cascade_child", {{"parent_id", "3"}},
               {"=id 100"}) == DBStatus::OK);
    assert(engine.update(
               kDatabase, "cascade_child", {{"parent_id", "1"}},
               {"=id 100"}) == DBStatus::OK);

    assert(engine.insert(
               kDatabase, "composite_parent", {{"a", "7"}, {"b", "8"}}) ==
           DBStatus::OK);
    assert(engine.insert(
               kDatabase, "composite_child",
               {{"id", "700"}, {"parent_a", "7"}, {"parent_b", "8"}}) ==
           DBStatus::OK);
    assert(engine.update(
               kDatabase, "composite_child", {{"parent_b", "9"}},
               {"=id 700"}) == DBStatus::FOREIGN_KEY_VIOLATION);
    assert(rowById(engine, "composite_child", "700").at("parent_b") == "8");
    assert(engine.insert(
               kDatabase, "composite_parent", {{"a", "7"}, {"b", "9"}}) ==
           DBStatus::OK);
    assert(engine.update(
               kDatabase, "composite_child", {{"parent_b", "9"}},
               {"=id 700"}) == DBStatus::OK);
    assert(engine.update(
               kDatabase, "composite_child", {{"parent_b", "8"}},
               {"=id 700"}) == DBStatus::OK);

    assert(engine.update(
               kDatabase, "parent", {{"id", "10"}}, {"=id 1"}) ==
           DBStatus::OK);
    assert(primaryMissing(engine, "parent", "1"));
    primaryRid(engine, "parent", "10");
    assertIndexedParent(engine, "cascade_child", "100", "10");
    assertIndexedParent(engine, "nullable_child", "101", "10");
    assert(engine.getSecondaryIndex(
               kDatabase, "cascade_child", "parent_id")
               ->searchMulti("1").empty());

    assert(engine.remove(kDatabase, "parent", {"=id 10"}) ==
           DBStatus::OK);
    assert(primaryMissing(engine, "parent", "10"));
    assert(primaryMissing(engine, "cascade_child", "100"));
    assert(primaryMissing(engine, "grandchild", "1000"));
    assert(engine.getSecondaryIndex(
               kDatabase, "grandchild", "child_id")
               ->searchMulti("100").empty());
    assert(engine.getHashIndex(kDatabase, "grandchild", "child_id")
               ->search("100").empty());
    assertSetNullState(engine, "101", "10");
    std::cout << "[FOREIGN KEY ACTION] autocommit DML/index/TOAST cascade OK\n";
}

void testRollbackActions(StorageEngine& engine) {
    insertFamily(engine, "2", "200", "201", "2000");

    assert(engine.beginTransaction(kDatabase) == DBStatus::OK);
    assert(engine.update(
               kDatabase, "parent", {{"id", "20"}}, {"=id 2"}) ==
           DBStatus::OK);
    assertIndexedParent(engine, "cascade_child", "200", "20");
    assertIndexedParent(engine, "nullable_child", "201", "20");
    assert(engine.rollbackTransaction() == DBStatus::OK);
    primaryRid(engine, "parent", "2");
    assert(primaryMissing(engine, "parent", "20"));
    assertIndexedParent(engine, "cascade_child", "200", "2");
    assertIndexedParent(engine, "nullable_child", "201", "2");

    assert(engine.beginTransaction(kDatabase) == DBStatus::OK);
    assert(engine.remove(kDatabase, "parent", {"=id 2"}) == DBStatus::OK);
    assert(primaryMissing(engine, "cascade_child", "200"));
    assert(primaryMissing(engine, "grandchild", "2000"));
    assert(engine.rollbackTransaction() == DBStatus::OK);
    primaryRid(engine, "parent", "2");
    assertIndexedParent(engine, "cascade_child", "200", "2");
    assertIndexedParent(engine, "nullable_child", "201", "2");
    primaryRid(engine, "grandchild", "2000");
    std::cout << "[FOREIGN KEY ACTION] transactional rollback cascade OK\n";
}

void createRenameFixture(StorageEngine& engine) {
    assert(engine.createDatabase(kRenameDatabase) == DBStatus::OK);
    assert(engine.createTable(kRenameDatabase, keyedTable("parent")) ==
           DBStatus::OK);

    TableSchema child = keyedTable("child");
    child.append(makeIntColumn("parent_id", false, 4, false));
    assert(engine.createTable(kRenameDatabase, child) == DBStatus::OK);
    assert(engine.alterTableAddFKConstraint(
               kRenameDatabase, "child", "child_parent_fk", {"parent_id"},
               "parent", {"id"}, "cascade", "cascade") == DBStatus::OK);

    assert(engine.insert(kRenameDatabase, "parent", {{"id", "1"}}) ==
           DBStatus::OK);
    assert(engine.alterTableRenameTable(
               kRenameDatabase, "parent", "renamed_parent") == DBStatus::OK);

    const TableSchema childAfterRename =
        engine.getTableSchema(kRenameDatabase, "child");
    assert(childAfterRename.fkLen == 1);
    assert(childAfterRename.fks[0].refTable == "renamed_parent");
    assert(engine.insert(
               kRenameDatabase, "child",
               {{"id", "10"}, {"parent_id", "1"}}) == DBStatus::OK);
    assert(engine.insert(
               kRenameDatabase, "child",
               {{"id", "11"}, {"parent_id", "999"}}) ==
           DBStatus::FOREIGN_KEY_VIOLATION);

    assert(engine.remove(
               kRenameDatabase, "renamed_parent", {"=id 1"}) ==
           DBStatus::OK);
    int64_t childRid = -1;
    assert(!engine.getPKIndex(kRenameDatabase, "child")->search(
        "10", childRid));
    std::cout << "[FOREIGN KEY ACTION] parent rename keeps FK actions OK\n";
}

void testRenamedReferenceAfterRestart() {
    StorageEngine restarted;
    const TableSchema child =
        restarted.getTableSchema(kRenameDatabase, "child");
    assert(child.fkLen == 1);
    assert(child.fks[0].refTable == "renamed_parent");
    assert(restarted.insert(
               kRenameDatabase, "renamed_parent", {{"id", "2"}}) ==
           DBStatus::OK);
    assert(restarted.insert(
               kRenameDatabase, "child",
               {{"id", "20"}, {"parent_id", "2"}}) == DBStatus::OK);
    std::cout << "[FOREIGN KEY ACTION] renamed FK persists across restart OK\n";
}

} // namespace

int main() {
    cleanup();
    {
        StorageEngine engine;
        createFixture(engine);
        // Release fixture-building statement locks before the tested paths.
        engine.getLockManager().unlockAll();
        engine.getLockManager().unlockAllGaps();
        testAutocommitActions(engine);
        testRollbackActions(engine);
        createRenameFixture(engine);
    }
    testRenamedReferenceAfterRestart();
    cleanup();
    std::cout << "[FOREIGN KEY ACTION] all passed\n";
    return 0;
}
