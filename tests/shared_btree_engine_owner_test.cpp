#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "access/BPTree.h"

#include <cassert>
#include <iostream>
#include <memory>
#include <set>

using namespace dbms;

static void assertIndexes(StorageEngine& engine, const std::string& db) {
    const auto table = engine.getTableSchema(db, "items");
    auto* primary = engine.getPKIndex(db, "items");
    auto* secondary = engine.getSecondaryIndex(db, "items", "tag");
    auto* composite = engine.getCompositeIndexTree(db, "items", "tag_part");
    assert(primary && secondary && composite);
    assert(primary->allValues().size() == 2);
    assert(secondary->allValues().size() == 2);
    assert(composite->allValues().size() == 2);
    assert(secondary->searchMulti("same").size() == 2);
    assert(composite->searchMulti("same" + std::string(1, '\x01') + "part").size() == 2);
    const auto primaryValues = primary->allValues();
    const std::set<int64_t> expectedRids(primaryValues.begin(), primaryValues.end());
    assert(expectedRids.size() == 2);
    const auto secondaryValues = secondary->searchMulti("same");
    const auto compositeValues = composite->searchMulti("same" + std::string(1, '\x01') + "part");
    assert(std::set<int64_t>(secondaryValues.begin(), secondaryValues.end()) == expectedRids);
    assert(std::set<int64_t>(compositeValues.begin(), compositeValues.end()) == expectedRids);
    for (auto id : {"99", "100"}) {
        int64_t rid = -1;
        // Explicit PK ordinals use a terminated composite key even for one
        // column; the raw SQL spelling is not the physical tree key.
        assert(!primary->search(id, rid));
        assert(primary->search(table.buildPKValue({{"id", id}}), rid));
        assert(engine.query(db, "items", {std::string("=id ") + id}, {"id"}).size() == 1);
    }
}

int main() {
    const std::string db = "__t_shared_btree_owner";
    {
        auto first = std::make_unique<StorageEngine>();
        StorageEngine second;
        assert(first->createDatabase(db) == DBStatus::OK);
        TableSchema table;
        table.tablename = "items";
        table.append(makeIntColumn("id", false, 2, true));
        table.pkColIndices.push_back(0);
        table.append(makeVarCharColumn("tag", false, 20));
        table.append(makeVarCharColumn("part", false, 20));
        assert(first->createTable(db, table) == DBStatus::OK);
        assert(first->createIndex(db, "items", "tag") == DBStatus::OK);
        assert(first->createCompositeIndex(db, "items", {"tag", "part"}, "tag_part") == DBStatus::OK);
        // Warm every physical index before either writer changes it.
        for (auto* engine : {first.get(), &second}) {
            assert(engine->getPKIndex(db, "items")->allValues().empty());
            assert(engine->getSecondaryIndex(db, "items", "tag")->allValues().empty());
            assert(engine->getCompositeIndexTree(db, "items", "tag_part")->allValues().empty());
        }
        assert(first->beginTransaction(db) == DBStatus::OK);
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(first->insert(db, "items", {{"id", "99"}, {"tag", "same"}, {"part", "part"}}) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "100"}, {"tag", "same"}, {"part", "part"}}) == DBStatus::OK);
        assert(first->commitTransaction() == DBStatus::OK);
        assert(second.commitTransaction() == DBStatus::OK);
        assertIndexes(*first, db);
        assertIndexes(second, db);
        assert(first->getPKIndex(db, "items") == second.getPKIndex(db, "items"));
        assert(first->getSecondaryIndex(db, "items", "tag") == second.getSecondaryIndex(db, "items", "tag"));
        assert(first->getCompositeIndexTree(db, "items", "tag_part") == second.getCompositeIndexTree(db, "items", "tag_part"));
        {
            StorageEngine observer;
            assertIndexes(observer, db);
            assert(observer.getPKIndex(db, "items") == second.getPKIndex(db, "items"));
        }
        first.reset();
        assertIndexes(second, db);
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "101"}, {"tag", "same"}, {"part", "part"}}) == DBStatus::OK);
        assert(second.rollbackTransaction() == DBStatus::OK);
        assertIndexes(second, db);
        StorageEngine peer;
        assertIndexes(peer, db);
        assert(second.reindex(db, "items") == DBStatus::OK);
        assertIndexes(second, db);
        assertIndexes(peer, db);
        assert(second.getPKIndex(db, "items") == peer.getPKIndex(db, "items"));
    }
    StorageEngine reopened;
    assertIndexes(reopened, db);
    std::cout << "[SHARED BTREE] primary, secondary, composite, lifetime, rollback and reindex passed\n";
}
