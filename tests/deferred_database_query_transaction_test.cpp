#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <cassert>
#include <future>
#include <iostream>
#include <thread>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("deferred_database_query_transaction");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    TableSchema table;
    table.tablename = "rows";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, table) == DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"id", "1"}}) == DBStatus::OK);
    assert(g_engine.getWAL(db) != nullptr); // warm-cache path must also fence
    g_engine.getLockManager().setLockTimeout(50);

    std::promise<void> locked, release;
    auto released = release.get_future();
    std::thread ddl([&] {
        assert(g_engine.beginTransaction(db, true) == DBStatus::OK);
        locked.set_value();
        released.wait();
        assert(g_engine.rollbackTransaction() == DBStatus::OK);
        g_engine.endBackendSession();
    });
    locked.get_future().wait();
    for (bool commit : {false, true}) {
        StorageEngine::DatabaseIndependentBeginScope deferred(g_engine);
        assert(g_engine.beginTransaction(db) == DBStatus::OK);
        assert(g_engine.inTransaction() && g_engine.databaseTransactionOwnershipDeferred());
        const auto xid = g_engine.currentTxnId();
        assert(xid != 0);
        assert(g_engine.beginSqlCommand());
        g_engine.noteQuerySnapshot();
        const auto snapshot = Snapshot::importFromBytes(g_engine.exportSnapshot());
        assert(snapshot);
        assert(g_engine.finishSqlCommand() && g_engine.currentCommandId() == 1);
        assert(!g_engine.setIsolationLevel(IsolationLevel::SERIALIZABLE));
        assert(g_engine.ensureDatabaseTransactionOwnership() == DBStatus::LOCK_CONFLICT);
        assert(g_engine.currentTxnId() == xid && g_engine.currentCommandId() == 1);
        const auto after = Snapshot::importFromBytes(g_engine.exportSnapshot());
        assert(after && after->xmin == snapshot->xmin && after->xmax == snapshot->xmax);
        assert(after->activeXids == snapshot->activeXids);
        bool viewBlocked = false;
        try { (void)g_engine.getCurrentReadView(); }
        catch (const DbError& error) { viewBlocked = error.sqlState() == "55P03"; }
        assert(viewBlocked);
        // Direct native callers cannot bypass a deferred owner's physical fence.
        assert(g_engine.insert(db, table.tablename, {{"id", "90"}}) == DBStatus::LOCK_CONFLICT);
        bool schemaBlocked = false;
        try { (void)g_engine.getTableSchema(db, table.tablename); }
        catch (const DbError& error) { schemaBlocked = error.sqlState() == "55P03"; }
        assert(schemaBlocked);
        bool walBlocked = false;
        try { (void)g_engine.getWAL(db); }
        catch (const DbError& error) { walBlocked = error.sqlState() == "55P03"; }
        assert(walBlocked && g_engine.databaseTransactionOwnershipDeferred());
        assert(!g_engine.createTransactionBackup());
        assert((commit ? g_engine.commitTransaction() : g_engine.rollbackTransaction()) == DBStatus::OK);
        assert(!g_engine.inTransaction() && g_engine.currentTxnId() == 0);
    }
    for (int kind : {0, 1, 2}) {
        StorageEngine::DatabaseIndependentBeginScope deferred(g_engine);
        assert(g_engine.beginTransaction(db) == DBStatus::OK);
        const auto xid = g_engine.currentTxnId();
        auto interrupt = std::make_shared<SessionInterruptState>();
        interrupt->cancelRequested = kind != 2;
        interrupt->timeoutRequested = kind == 1;
        interrupt->terminateRequested = kind == 2;
        setCurrentQueryInterruptState(interrupt);
        bool interrupted = false;
        try { (void)g_engine.ensureDatabaseTransactionOwnership(); }
        catch (const DbError& error) { interrupted = error.sqlState() == (kind == 2 ? "57P01" : "57014"); }
        setCurrentQueryInterruptState(nullptr);
        assert(interrupted && g_engine.inTransaction() && g_engine.databaseTransactionOwnershipDeferred());
        assert(g_engine.currentTxnId() == xid);
        assert(g_engine.rollbackTransaction() == DBStatus::OK);
    }
    release.set_value();
    ddl.join();
    assert(g_engine.query(db, table.tablename, {}, {"id"}).size() == 1);

    // Promotion preserves the first RR snapshot and command identity rather
    // than silently restarting the transaction after another writer commits.
    assert(g_engine.setIsolationLevel(IsolationLevel::REPEATABLE_READ));
    {
        StorageEngine::DatabaseIndependentBeginScope deferred(g_engine);
        assert(g_engine.beginTransaction(db) == DBStatus::OK);
    }
    g_engine.noteQuerySnapshot();
    assert(g_engine.beginSqlCommand() && g_engine.finishSqlCommand());
    const auto xid = g_engine.currentTxnId();
    const auto snapshot = Snapshot::importFromBytes(g_engine.exportSnapshot());
    assert(snapshot);
    std::thread writer([&] {
        assert(g_engine.beginTransaction(db) == DBStatus::OK);
        assert(g_engine.insert(db, table.tablename, {{"id", "2"}}) == DBStatus::OK);
        assert(g_engine.commitTransaction() == DBStatus::OK);
        g_engine.endBackendSession();
    });
    writer.join();
    assert(g_engine.ensureDatabaseTransactionOwnership() == DBStatus::OK);
    assert(!g_engine.databaseTransactionOwnershipDeferred());
    assert(g_engine.currentTxnId() == xid && g_engine.currentCommandId() == 1);
    const auto after = Snapshot::importFromBytes(g_engine.exportSnapshot());
    assert(after && after->xmin == snapshot->xmin && after->xmax == snapshot->xmax);
    assert(after->activeXids == snapshot->activeXids);
    assert(g_engine.getCurrentReadView()->commitLog != nullptr);
    assert(g_engine.query(db, table.tablename, {}, {"id"}).size() == 1);
    assert(g_engine.savepoint("child") == DBStatus::OK);
    assert(g_engine.insert(db, table.tablename, {{"id", "3"}}) == DBStatus::OK);
    assert(g_engine.rollbackToSavepoint("child") == DBStatus::OK);
    assert(g_engine.releaseSavepoint("child") == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(g_engine.query(db, table.tablename, {}, {"id"}).size() == 2);
    cleanupTestDb("deferred_database_query_transaction");
    finalCleanupTestData();
    std::cout << "[DEFERRED DATABASE QUERY TRANSACTION] passed\n";
}
