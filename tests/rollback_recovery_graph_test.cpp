#include "access/BPTree.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "storage/WAL.h"
#include "storage/CommitLog.h"
#include "test_utils.h"

#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <exception>
#include <execinfo.h>
#include <iostream>
#include <thread>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static size_t abortRecords(dbms::WALManager* wal, uint64_t xid) {
    assert(wal);
    size_t count = 0;
    for (dbms::Lsn lsn = wal->earliestAvailableLsn();;) {
        auto record = wal->ReadRecord(lsn);
        if (!record || !record->header.xl_tot_len) break;
        if (record->rmid() == dbms::RM_XACT_ID && record->info() == dbms::XLOG_XACT_ABORT && record->header.xl_xid == xid)
            ++count;
        lsn += record->header.xl_tot_len;
    }
    return count;
}

static void rowGraphRetry(int operation) {
    using namespace dbms;
    const auto database = testDbPath("rollback_row_graph_" + std::to_string(operation));
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "rows";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 2, true));
    table.append(makeIntColumn("value", false, 2));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    assert(g_engine.createIndex(database, "rows", "value") == DBStatus::OK);
    assert(g_engine.insert(database, "rows", {{"id", "1"}, {"value", "10"}}) == DBStatus::OK);
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    const auto xid = g_engine.currentTxnId();
    auto* wal = g_engine.getWAL(database);
    auto* clog = g_engine.getCommitLog(database);
    if (operation == 0)
        assert(g_engine.insert(database, "rows", {{"id", "2"}, {"value", "20"}}) == DBStatus::OK);
    else if (operation == 1)
        assert(g_engine.update(database, "rows", {{"value", "20"}}, {"=id 1"}) == DBStatus::OK);
    else
        assert(g_engine.remove(database, "rows", {"=id 1"}) == DBStatus::OK);
    bool healthy = false;
    int calls = 0;
    g_engine.registerDdlUndo([&] { ++calls; return healthy; });
    assert(g_engine.rollbackTransaction() == DBStatus::IO_ERROR);
    std::cout << "ROW_GRAPH " << operation << " pending" << std::endl;
    assert(g_engine.inTransaction() && g_engine.currentTxnId() == xid && !g_engine.hasTransactionBackup());
    assert(abortRecords(wal, xid) == 0);
    assert(clog->getStatus(xid) == CommitLog::Status::InProgress);
    assert(g_engine.beginTransaction(database) == DBStatus::IO_ERROR);
    assert(g_engine.commitTransaction() == DBStatus::IO_ERROR);
    healthy = true;
    std::cout << "ROW_GRAPH " << operation << " retry" << std::endl;
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    std::cout << "ROW_GRAPH " << operation << " complete" << std::endl;
    assert(calls == 2 && !g_engine.inTransaction() && abortRecords(wal, xid) == 1);
    assert(clog->getStatus(xid) == CommitLog::Status::Aborted);
    assert(g_engine.query(database, "rows", {}, {"id", "value"}) == std::vector<std::string>{"1 10 "});
    auto* secondary = g_engine.getSecondaryIndex(database, "rows", "value");
    assert(secondary && secondary->searchMulti("10").size() == 1 && secondary->searchMulti("20").empty());
    assert(g_engine.insert(database, "rows", {{"id", "2"}, {"value", "30"}}) == DBStatus::OK);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
}

static void multiplePendingOwners() {
    using namespace dbms;
    const auto database = testDbPath("rollback_multiple_owners");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "rows";
    table.append(makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    std::promise<void> ready[2], release[2], done[2];
    auto ready0 = ready[0].get_future(), ready1 = ready[1].get_future();
    auto done0 = done[0].get_future(), done1 = done[1].get_future();
    std::thread owners[2];
    // Both are genuine already-admitted transactions, not new writes after
    // quarantine. A second failed owner must not replace the first xid.
    std::promise<void> admitted[2], startRollback[2];
    auto admitted0 = admitted[0].get_future(), admitted1 = admitted[1].get_future();
    for (int i = 0; i < 2; ++i) {
        owners[i] = std::thread([&, i] {
            assert(g_engine.beginTransaction(database) == DBStatus::OK);
            bool healthy = false;
            int calls = 0;
            g_engine.registerDdlUndo([&] { ++calls; return healthy; });
            admitted[i].set_value();
            startRollback[i].get_future().get();
            assert(g_engine.rollbackTransaction() == DBStatus::IO_ERROR);
            assert(g_engine.inTransaction());
            ready[i].set_value();
            release[i].get_future().get();
            healthy = true;
            assert(g_engine.rollbackTransaction() == DBStatus::OK);
            assert(calls == 2 && !g_engine.inTransaction());
            done[i].set_value();
            g_engine.endBackendSession();
        });
    }
    assert(admitted0.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    assert(admitted1.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    startRollback[0].set_value();
    assert(ready0.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    startRollback[1].set_value();
    assert(ready1.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    assert(g_engine.insert(database, "rows", {{"id", "1"}}) == DBStatus::IO_ERROR);
    release[0].set_value();
    assert(done0.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    assert(g_engine.insert(database, "rows", {{"id", "1"}}) == DBStatus::IO_ERROR);
    release[1].set_value();
    assert(done1.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    owners[0].join();
    owners[1].join();
    assert(g_engine.insert(database, "rows", {{"id", "1"}}) == DBStatus::OK);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
}

int main() {
    std::set_terminate([] {
        void* frames[32];
        const int count = ::backtrace(frames, 32);
        ::backtrace_symbols_fd(frames, count, STDERR_FILENO);
        ::_exit(90);
    });
    using namespace dbms;
    const auto database = testDbPath("rollback_recovery_graph");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "rows";
    table.append(makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    assert(g_engine.insert(database, "rows", {{"id", "1"}}) == DBStatus::OK);
    assert(g_engine.beginTransaction(database, true) == DBStatus::OK);
    assert(g_engine.createTransactionBackup());
    const auto xid = g_engine.currentTxnId();
    auto* wal = g_engine.getWAL(database);
    auto* clog = g_engine.getCommitLog(database);
    const fs::path image = database + ".txn_backup." + std::to_string(xid);
    assert(fs::is_directory(image));
    bool healthy = false;
    int calls = 0;
    // Normal registered undo callbacks can report an ordinary IO failure.
    // The graph must remain retryable; no malformed heap/WAL input is used.
    g_engine.registerDdlUndo([&] { ++calls; return healthy; });
    const auto status = g_engine.rollbackTransaction();
    const auto aborts = abortRecords(wal, xid);
    const bool active = g_engine.inTransaction();
    const bool retained = fs::is_directory(image);
    std::cout << "ROLLBACK_GRAPH failed_status=" << static_cast<int>(status)
              << " active=" << active << " retained=" << retained
              << " aborts=" << aborts << " calls=" << calls << std::endl;
    assert(status == DBStatus::IO_ERROR);
    assert(active && retained && aborts == 0);
    assert(clog->getStatus(xid) == CommitLog::Status::InProgress);
    assert(g_engine.currentTxnId() == xid);
    assert(g_engine.beginTransaction(database) == DBStatus::IO_ERROR);
    assert(g_engine.commitTransaction() == DBStatus::IO_ERROR);
    assert(g_engine.insert(database, "rows", {{"id", "2"}}) == DBStatus::IO_ERROR);
    DBStatus backendStatus = DBStatus::OK;
    bool cachedConsumerRejected = false;
    std::thread backend([&] {
        backendStatus = g_engine.insert(database, "rows", {{"id", "3"}});
        try { (void)g_engine.getPageAllocator(database, "rows"); }
        catch (const DbError& error) { cachedConsumerRejected = error.sqlState() == "58030"; }
        g_engine.endBackendSession();
    });
    backend.join();
    assert(backendStatus == DBStatus::IO_ERROR && cachedConsumerRejected);
    g_engine.endBackendSession();
    assert(g_engine.inTransaction() && g_engine.currentTxnId() == xid);
    assert(fs::is_directory(image) && calls == 2);
    healthy = true;
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(calls == 3 && !g_engine.inTransaction() && !fs::exists(image));
    assert(abortRecords(wal, xid) == 1);
    assert(g_engine.query(database, "rows", {}, {"id"}) == std::vector<std::string>{"1 "});
    assert(g_engine.insert(database, "rows", {{"id", "2"}}) == DBStatus::OK);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    for (int operation = 0; operation < 3; ++operation) rowGraphRetry(operation);
    multiplePendingOwners();
    std::cout << "[ROLLBACK RECOVERY GRAPH] ordinary error, preserved graph, explicit healthy retry OK\n";
}
