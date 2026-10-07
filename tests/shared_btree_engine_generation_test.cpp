#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "access/BPTree.h"

#include <cassert>
#include <iostream>

using namespace dbms;

static TableSchema schema() {
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 4, true));
    return table;
}

static void replaceClosedGeneration() {
    const std::string db = "__t_shared_btree_closed_generation";
    StorageEngine first, peer;
    assert(first.createDatabase(db) == DBStatus::OK);
    assert(first.createTable(db, schema()) == DBStatus::OK);
    assert(first.insert(db, "items", {{"id", "1"}}) == DBStatus::OK);
    auto* old = peer.getPKIndex(db, "items");
    assert(old && old->isOpen());
    assert(!old->hasStaleFileGeneration());
    old->close();
    assert(!old->isOpen());
    assert(!old->hasStaleFileGeneration());
    assert(first.dropTable(db, "items") == DBStatus::OK);
    assert(old->hasStaleFileGeneration());
    assert(first.createTable(db, schema()) == DBStatus::OK);
    assert(old->hasStaleFileGeneration());
    const auto inserted = first.insert(db, "items", {{"id", "2"}});
    std::cerr << "[CLOSED INDEX GENERATION] replacement insert="
              << sqlstateForDBStatus(inserted) << '\n';
    assert(inserted == DBStatus::OK);
    for (auto* engine : {&first, &peer}) {
        auto* current = engine->getPKIndex(db, "items");
        assert(current && current->isOpen());
        assert(current->allValues().size() == 1);
        int64_t rid = -1;
        assert(current->search("2", rid));
        assert(!current->search("1", rid));
    }
}

static void rebuildClosedGeneration() {
    const std::string db = "__t_shared_btree_closed_reindex";
    StorageEngine first, peer;
    assert(first.createDatabase(db) == DBStatus::OK);
    assert(first.createTable(db, schema()) == DBStatus::OK);
    assert(first.insert(db, "items", {{"id", "1"}}) == DBStatus::OK);
    auto* old = peer.getPKIndex(db, "items");
    assert(old);
    old->close();
    assert(!old->hasStaleFileGeneration());
    assert(first.reindex(db, "items") == DBStatus::OK);
    assert(old->hasStaleFileGeneration());
    for (auto* engine : {&first, &peer}) {
        auto* current = engine->getPKIndex(db, "items");
        assert(current && current->isOpen());
        assert(current->allValues().size() == 1);
        int64_t rid = -1;
        assert(current->search("1", rid));
    }
}

int main() {
    replaceClosedGeneration();
    rebuildClosedGeneration();
    std::cout << "[CLOSED INDEX GENERATION] closed failure is not inherited by rebuilt files\n";
}
