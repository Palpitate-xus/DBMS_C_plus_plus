#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "storage/PageAllocator.h"
#include "storage/WAL.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <set>

using namespace dbms;

static std::set<std::string> ids(StorageEngine& engine, const std::string& db) {
    std::set<std::string> result;
    for (auto row : engine.query(db, "items", {}, {"id"})) {
        while (!row.empty() && row.back() == ' ') row.pop_back();
        assert(result.insert(row).second);
    }
    return result;
}

int main() {
    const std::string db = "__t_shared_heap_engine_owner";
    {
        auto first = std::make_unique<StorageEngine>();
        StorageEngine second;
        first->setBackgroundIntervals(1000000, 1000000);
        second.setBackgroundIntervals(1000000, 1000000);
        assert(first->createDatabase(db) == DBStatus::OK);
        TableSchema table;
        table.tablename = "items";
        table.append(makeIntColumn("id", false, 2, false));
        assert(first->createTable(db, table) == DBStatus::OK);
        auto* allocator = first->getPageAllocator(db, "items");
        assert(allocator && allocator == second.getPageAllocator(db, "items"));
        auto* wal = first->getWAL(db);
        assert(wal && wal == second.getWAL(db));
        assert(first->beginTransaction(db) == DBStatus::OK);
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(first->insert(db, "items", {{"id", "99"}}) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "100"}}) == DBStatus::OK);
        assert(first->commitTransaction() == DBStatus::OK);
        assert(second.commitTransaction() == DBStatus::OK);
        const std::set<std::string> expected{"99", "100"};
        assert(ids(*first, db) == expected);
        assert(ids(second, db) == expected);
        {
            StorageEngine observer;
            assert(ids(observer, db) == expected);
            assert(observer.getPageAllocator(db, "items") == allocator);
        }
        // An observer's destruction must not close another engine's cache.
        assert(allocator->isOpen());
        first.reset();
        // The heap WAL barrier must not dangle after its first opener exits.
        assert(allocator->isOpen());
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "101"}}) == DBStatus::OK);
        assert(second.commitTransaction() == DBStatus::OK);
        assert(ids(second, db) == (std::set<std::string>{"99", "100", "101"}));
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "102"}}) == DBStatus::OK);
        assert(second.rollbackTransaction() == DBStatus::OK);
        assert(ids(second, db) == (std::set<std::string>{"99", "100", "101"}));
        // Failure retains the shared dirty state for a later safe retry.
        allocator->bufferPool()->setWritebackBarrier([] { return false; });
        assert(second.beginTransaction(db) == DBStatus::OK);
        assert(second.insert(db, "items", {{"id", "103"}}) == DBStatus::OK);
        assert(!allocator->flush());
        allocator->bufferPool()->setWritebackBarrier([wal] {
            return wal->XLogFlush(wal->currentWriteLsn());
        });
        assert(second.rollbackTransaction() == DBStatus::OK);
        assert(allocator->flush());
        assert(ids(second, db) == (std::set<std::string>{"99", "100", "101"}));
    }
    // All owners are gone: this opens physical files rather than reusing a
    // live shared frame. Both successful commits and the later commit survive.
    {
        StorageEngine reopened;
        assert(ids(reopened, db) == (std::set<std::string>{"99", "100", "101"}));
    }
    std::cout << "[SHARED HEAP OWNER] rows, lifetime, WAL, rollback and reopen passed\n";
}
