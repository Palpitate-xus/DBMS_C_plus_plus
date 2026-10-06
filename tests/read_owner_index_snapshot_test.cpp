#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "process/RuntimeStats.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <future>
#include <iostream>
#include <sstream>
#include <thread>

extern dbms::StorageEngine g_engine;

int main() {
    using dbms::DBStatus;
    using dbms::IsolationLevel;
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("read_owner_index_snapshot");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "index_owner";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, table) == DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"id", "1"}}) == DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"id", "2"}}) == DBStatus::OK);
    const auto stats = [&]() {
        const auto rows = dbms::getRuntimeTableStats(db);
        const auto found = std::find_if(rows.begin(), rows.end(), [&](const auto& row) {
            return row.relname == table.tablename;
        });
        assert(found != rows.end());
        return *found;
    };
    const auto lookup = [&](int id) {
        const auto rows = g_engine.query(db, table.tablename,
                                        {"=id " + std::to_string(id)}, {"id"});
        assert(rows.size() == 1);
        std::istringstream row(rows.front());
        int actual = 0;
        std::string extra;
        assert(row >> actual);
        assert(actual == id && !(row >> extra));
    };
    // Positive control without an active owner uses the physical PK index.
    lookup(1);
    assert(stats().idxScan >= 1 && stats().idxTupFetch >= 1);

    // A current, otherwise isolated read transaction is not an overlapping
    // writer. The old any-active guard disables this useful index scan.
    assert(g_engine.setIsolationLevel(IsolationLevel::REPEATABLE_READ));
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    auto before = stats();
    lookup(1);
    auto after = stats();
    assert(after.idxScan == before.idxScan + 1);
    assert(after.idxTupFetch == before.idxTupFetch + 1);
    assert(g_engine.finishSqlCommand());

    // Another command commits an update of an unread row. Even with no
    // other currently active transaction, our fixed old snapshot needs its
    // old heap version, which the current-version PK index no longer holds.
    std::thread writer([&]() {
        assert(g_engine.beginTransaction(db) == DBStatus::OK);
        assert(g_engine.update(db, table.tablename, {{"id", "20"}},
                               {"=id 2"}) == DBStatus::OK);
        assert(g_engine.commitTransaction() == DBStatus::OK);
    });
    writer.join();
    assert(g_engine.beginSqlCommand());
    before = stats();
    lookup(2);
    after = stats();
    assert(after.idxScan == before.idxScan && after.seqScan == before.seqScan + 1);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.commitTransaction() == DBStatus::OK);
    lookup(20);

    // A snapshot taken while a different transaction is active still uses
    // the conservative heap path, even when that transaction has no writes.
    std::promise<void> active;
    std::promise<void> finish;
    auto finished = finish.get_future();
    std::thread overlapping([&]() {
        assert(g_engine.beginTransaction(db) == DBStatus::OK);
        active.set_value();
        finished.wait();
        assert(g_engine.rollbackTransaction() == DBStatus::OK);
    });
    active.get_future().wait();
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    before = stats();
    lookup(20);
    after = stats();
    assert(after.idxScan == before.idxScan && after.seqScan == before.seqScan + 1);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.commitTransaction() == DBStatus::OK);
    finish.set_value();
    overlapping.join();

    // The current command can still see its own row before a same-command
    // deletion/update. Own writes therefore cannot use current-only indexes.
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    assert(g_engine.update(db, table.tablename, {{"id", "30"}},
                           {"=id 20"}) == DBStatus::OK);
    before = stats();
    lookup(20);
    after = stats();
    assert(after.idxScan == before.idxScan && after.seqScan == before.seqScan + 1);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.beginSqlCommand());
    lookup(30);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    lookup(20);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    std::cout << "[READ OWNER INDEX SNAPSHOT] passed\n";
}
