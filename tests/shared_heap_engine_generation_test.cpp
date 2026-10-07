#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "storage/PageAllocator.h"
#include "storage/WAL.h"

#include <cassert>
#include <iostream>
#include <set>

using namespace dbms;

static TableSchema schema(const std::string& name, bool wide = false) {
    TableSchema result;
    result.tablename = name;
    result.append(makeIntColumn("id", false, 2, false));
    if (wide) {
        for (auto field : {"a", "b", "c"})
            result.append(makeVarCharColumn(field, false, 1500));
    }
    return result;
}

static void growDifferentPages() {
    const std::string db = "__t_shared_heap_growth";
    {
        StorageEngine first, second;
        assert(first.createDatabase(db) == DBStatus::OK);
        const auto table = schema("items", true);
        assert(first.createTable(db, table) == DBStatus::OK);
        auto* left = first.getPageAllocator(db, "items");
        auto* right = second.getPageAllocator(db, "items");
        assert(left && left == right && left->numPages() == 1);
        assert(first.beginTransaction(db) == DBStatus::OK);
        assert(second.beginTransaction(db) == DBStatus::OK);
        const std::string a(1500, 'a'), b(1500, 'b');
        assert(first.insert(db, "items", {{"id", "99"}, {"a", a}, {"b", a}, {"c", a}}) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "100"}, {"a", b}, {"b", b}, {"c", b}}) == DBStatus::OK);
        assert(first.commitTransaction() == DBStatus::OK);
        assert(second.commitTransaction() == DBStatus::OK);
        assert(left->numPages() == 3 && right->numPages() == 3);
        std::set<uint32_t> pages;
        std::set<std::string> values;
        assert(second.forEachRow(db, "items", [&](uint32_t page, uint16_t, const char* bytes, size_t length) {
            pages.insert(page);
            const std::string row(bytes, length);
            values.insert(second.extractColumnValue(row, table, 0, db, true));
        }));
        assert(pages.size() == 2 && values == (std::set<std::string>{"99", "100"}));
    }
    StorageEngine restarted;
    assert(restarted.getPageAllocator(db, "items")->numPages() == 3);
    assert(restarted.query(db, "items", {}, {"id"}).size() == 2);
}

static void replaceHeapGeneration() {
    const std::string db = "__t_shared_heap_generation";
    StorageEngine first, second;
    assert(first.createDatabase(db) == DBStatus::OK);
    const auto table = schema("items");
    assert(first.createTable(db, table) == DBStatus::OK);
    assert(first.insert(db, "items", {{"id", "1"}}) == DBStatus::OK);
    assert(second.query(db, "items", {}, {"id"}).size() == 1);
    auto* oldOwner = second.getPageAllocator(db, "items");
    assert(first.dropTable(db, "items") == DBStatus::OK);
    assert(first.createTable(db, table) == DBStatus::OK);
    assert(first.insert(db, "items", {{"id", "2"}}) == DBStatus::OK);
    // The old descriptor must not create a marker or publish its old header
    // through the new path, even if the old owner still exists in a peer.
    assert(!oldOwner->flush());
    const auto rows = second.query(db, "items", {}, {"id"});
    assert(rows.size() == 1 && rows.front() == "2 ");
}

static void replaceDatabaseGeneration() {
    const std::string db = "__t_shared_wal_generation";
    StorageEngine first, second;
    assert(first.createDatabase(db) == DBStatus::OK);
    const auto table = schema("items");
    assert(first.createTable(db, table) == DBStatus::OK);
    assert(first.insert(db, "items", {{"id", "1"}}) == DBStatus::OK);
    assert(second.query(db, "items", {}, {"id"}).size() == 1);
    assert(first.getWAL(db) == second.getWAL(db));
    assert(first.dropDatabase(db) == DBStatus::OK);
    assert(first.createDatabase(db) == DBStatus::OK);
    assert(first.createTable(db, table) == DBStatus::OK);
    assert(first.insert(db, "items", {{"id", "2"}}) == DBStatus::OK);
    assert(second.beginTransaction(db) == DBStatus::OK);
    assert(second.insert(db, "items", {{"id", "3"}}) == DBStatus::OK);
    assert(first.getWAL(db) == second.getWAL(db));
    assert(second.commitTransaction() == DBStatus::OK);
    assert(second.query(db, "items", {}, {"id"}).size() == 2);
}

int main() {
    growDifferentPages();
    replaceHeapGeneration();
    replaceDatabaseGeneration();
    std::cout << "[SHARED HEAP GENERATION] extent growth, heap and WAL generations passed\n";
}
