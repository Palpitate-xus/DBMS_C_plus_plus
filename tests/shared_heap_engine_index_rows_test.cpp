#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "access/BPTree.h"

#include <cassert>
#include <iostream>

using namespace dbms;

int main() {
    const std::string db = "__t_shared_heap_index_rows";
    {
        StorageEngine first, second;
        assert(first.createDatabase(db) == DBStatus::OK);
        TableSchema table;
        table.tablename = "items";
        table.append(makeIntColumn("id", false, 2, true));
        table.pkColIndices.push_back(0);
        assert(first.createTable(db, table) == DBStatus::OK);
        assert(first.query(db, "items", {"=id 99"}, {"id"}).empty());
        assert(second.query(db, "items", {"=id 100"}, {"id"}).empty());
        assert(first.beginTransaction(db) == DBStatus::OK);
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(first.insert(db, "items", {{"id", "99"}}) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "100"}}) == DBStatus::OK);
        assert(first.commitTransaction() == DBStatus::OK);
        assert(second.commitTransaction() == DBStatus::OK);
        assert(first.query(db, "items", {}, {"id"}).size() == 2);
        assert(second.query(db, "items", {}, {"id"}).size() == 2);
        for (auto* engine : {&first, &second}) {
            for (const auto* id : {"99", "100"}) {
                const auto rows = engine->query(db, "items", {std::string("=id ") + id}, {"id"});
                std::cerr << "[SHARED INDEX ROW] id=" << id << " rows=" << rows.size() << '\n';
                assert(rows.size() == 1);
            }
            const auto entries = engine->getPKIndex(db, "items")->allValues();
            assert(entries.size() == 2);
        }
    }
    StorageEngine reopened;
    assert(reopened.query(db, "items", {}, {"id"}).size() == 2);
    assert(reopened.query(db, "items", {"=id 99"}, {"id"}).size() == 1);
    assert(reopened.query(db, "items", {"=id 100"}, {"id"}).size() == 1);
    assert(reopened.getPKIndex(db, "items")->allValues().size() == 2);
    std::cout << "[SHARED INDEX ROW] all indexed rows and reopen passed\n";
}
