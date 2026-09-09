#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "storage/CommitLog.h"
#include "storage/WAL.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

extern dbms::StorageEngine g_engine;

namespace {

void cleanup(const std::string& db) {
    std::error_code ec;
    std::filesystem::remove_all(db, ec);
    std::filesystem::remove_all("info/.prepared", ec);
}

void test_prepare_rejects_deferred_constraint_violation() {
    const std::string db = testDbPath("prepared_deferred_constraint");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE parent (id INT PRIMARY KEY)", session));
    assert(!ddl.executeSql(
        "CREATE TABLE child ("
        "id INT PRIMARY KEY, pid INT, "
        "CONSTRAINT child_pid_fkey FOREIGN KEY (pid) REFERENCES parent(id) "
        "DEFERRABLE INITIALLY DEFERRED)",
        session));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "child", {{"id", "1"}, {"pid", "999"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("prepared_deferred_violation") ==
           dbms::DBStatus::FOREIGN_KEY_VIOLATION);
    assert(!g_engine.inTransaction());
    const auto prepared = g_engine.listPreparedTransactions();
    assert(std::find(prepared.begin(), prepared.end(),
                     "prepared_deferred_violation") == prepared.end());
    assert(g_engine.query(db, "child", {}, {"id"}).empty());

    cleanup(db);
    std::cout << "[PREPARED-TXN] deferred violation rejected before prepare OK\n";
}

void test_prepare_resets_originating_transaction_modes() {
    const std::string db = testDbPath("prepared_constraint_mode");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE mode_rows ("
        "id INT PRIMARY KEY, tag INT, "
        "CONSTRAINT mode_rows_tag_key UNIQUE (tag) "
        "DEFERRABLE INITIALLY IMMEDIATE)",
        session));
    assert(g_engine.insert(db, "mode_rows", {{"id", "1"}, {"tag", "10"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    g_engine.setConstraintMode({"all"}, true);
    assert(g_engine.insert(db, "mode_rows", {{"id", "2"}, {"tag", "20"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("prepared_constraint_mode") ==
           dbms::DBStatus::OK);
    dbms::StorageEngine completingBackend;
    assert(completingBackend.commitPrepared("prepared_constraint_mode") ==
           dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "mode_rows", {{"id", "3"}, {"tag", "10"}}) ==
           dbms::DBStatus::DUPLICATE_KEY);
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[PREPARED-TXN] preparing session transaction modes reset OK\n";
}

void test_prepare_rejects_physical_ddl_snapshot() {
    const std::string db = testDbPath("prepared_physical_ddl");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "accounts";
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    dbms::DdlExecutor ddl;

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(!ddl.executeSql(
        "ALTER TABLE accounts ADD COLUMN note TEXT", session));
    assert(g_engine.hasTransactionBackup());
    assert(g_engine.transactionBackupDirty());

    // A prepared backend no longer owns the database-wide lock needed by a
    // whole-directory DDL restore. Fail closed without aborting the live
    // transaction, which remains available for an explicit decision.
    assert(g_engine.prepareTransaction("prepared_physical_ddl") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.inTransaction());
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(!g_engine.hasTransactionBackup());

    const dbms::TableSchema restored =
        g_engine.getTableSchema(db, "accounts");
    assert(restored.len == 1);
    assert(restored.cols[0].dataName == "id");
    const auto prepared = g_engine.listPreparedTransactions();
    assert(std::find(prepared.begin(), prepared.end(),
                     "prepared_physical_ddl") == prepared.end());

    cleanup(db);
    std::cout << "[PREPARED-TXN] physical DDL cannot escape its rollback lock through PREPARE OK\n";
}

void test_prepare_rejects_temporary_relation_writes() {
    const std::string db = testDbPath("prepared_temporary_relation");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    session.pid = 8675309;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TEMP TABLE temp_accounts (id INT PRIMARY KEY)", session));
    const std::string physicalName =
        tempTablePrefix(session, "temp_accounts");
    assert(g_engine.tableExists(db, physicalName));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, physicalName, {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("prepared_temporary_relation") ==
           dbms::DBStatus::INVALID_VALUE);
    assert(g_engine.inTransaction());
    assert(g_engine.rollbackTransaction() == dbms::DBStatus::OK);
    assert(g_engine.query(db, physicalName, {}, {"id"}).empty());
    const auto prepared = g_engine.listPreparedTransactions();
    assert(std::find(prepared.begin(), prepared.end(),
                     "prepared_temporary_relation") == prepared.end());

    cleanup(db);
    std::cout << "[PREPARED-TXN] temporary relation writes stay session-local OK\n";
}

void test_cross_backend_prepare_completion() {
    const std::string db = testDbPath("prepared_transaction");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "accounts";
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);

    dbms::StorageEngine backendB;
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "accounts", {{"id", "1"}}) == dbms::DBStatus::OK);
    g_engine.getLockManager().setResourceNamespace(db);
    assert(g_engine.getLockManager().lockExclusive("accounts"));
    assert(g_engine.prepareTransaction("prepared_commit") == dbms::DBStatus::OK);
    assert(!g_engine.inTransaction());

    backendB.getLockManager().setResourceNamespace(db);
    backendB.getLockManager().setLockTimeout(25);
    assert(!backendB.getLockManager().lockExclusive("accounts"));
    assert(backendB.commitPrepared("prepared_commit") == dbms::DBStatus::OK);
    assert(backendB.getLockManager().lockExclusive("accounts"));
    backendB.getLockManager().unlock("accounts");
    assert(backendB.query(db, "accounts", {"=id 1"}, {"id"}).size() == 1);

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "accounts", {{"id", "2"}}) == dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("prepared_rollback") == dbms::DBStatus::OK);

    assert(backendB.beginTransaction(db) == dbms::DBStatus::OK);
    assert(backendB.commitPrepared("prepared_rollback") == dbms::DBStatus::INVALID_VALUE);
    assert(backendB.rollbackTransaction() == dbms::DBStatus::OK);
    assert(backendB.rollbackPrepared("prepared_rollback") == dbms::DBStatus::OK);
    assert(backendB.query(db, "accounts", {"=id 2"}, {"id"}).empty());
    assert(backendB.insert(db, "accounts", {{"id", "2"}}) ==
           dbms::DBStatus::OK);
    assert(backendB.query(db, "accounts", {"=id 2"}, {"id"}).size() == 1);

    cleanup(db);
    std::cout << "[PREPARED-TXN] cross-backend commit/rollback and lock ownership OK\n";
}

void test_prepared_update_allows_mvcc_reader() {
    const std::string db = testDbPath("prepared_mvcc_reader");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "accounts";
    table.append(dbms::makeIntColumn("id", false, 2, true));
    table.append(dbms::makeVarCharColumn("value", false, 32));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "accounts", {{"id", "1"}, {"value", "old"}}) ==
           dbms::DBStatus::OK);

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.update(db, "accounts", {{"value", "new"}}, {"=id 1"}) ==
           dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("prepared_mvcc_reader") ==
           dbms::DBStatus::OK);

    dbms::StorageEngine reader;
    reader.getLockManager().setResourceNamespace(db);
    reader.getLockManager().setLockTimeout(100);
    const auto oldRows = reader.query(
        db, "accounts", {"=id 1"}, {"id", "value"});
    assert(oldRows.size() == 1 && oldRows.front().find("old") != std::string::npos);
    assert(reader.update(db, "accounts", {{"value", "blocked"}}, {"=id 1"}) ==
           dbms::DBStatus::LOCK_CONFLICT);

    assert(reader.rollbackPrepared("prepared_mvcc_reader") == dbms::DBStatus::OK);
    const auto restored = reader.query(
        db, "accounts", {"=id 1"}, {"id", "value"});
    assert(restored.size() == 1 && restored.front().find("old") != std::string::npos);

    cleanup(db);
    std::cout << "[PREPARED-TXN] prepared UPDATE allows MVCC readers OK\n";
}

void test_quoted_names_round_trip_through_prepared_metadata() {
    const std::string db = testDbPath("prepared quoted database");
    const std::string tableName = "order items";
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = tableName;
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);

    dbms::StorageEngine completingBackend;
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, tableName, {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("prepared_quoted_commit") ==
           dbms::DBStatus::OK);
    assert(completingBackend.commitPrepared("prepared_quoted_commit") ==
           dbms::DBStatus::OK);
    assert(completingBackend.query(
               db, tableName, {"=id 1"}, {"id"}).size() == 1);

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, tableName, {{"id", "2"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("prepared_quoted_rollback") ==
           dbms::DBStatus::OK);
    assert(completingBackend.rollbackPrepared("prepared_quoted_rollback") ==
           dbms::DBStatus::OK);
    assert(completingBackend.query(
               db, tableName, {"=id 2"}, {"id"}).empty());

    cleanup(db);
    std::cout << "[PREPARED-TXN] quoted database and table names round-trip OK\n";
}

void test_commit_refreshes_warm_completion_backend() {
    const std::string db = testDbPath("prepared_warm_commit");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "accounts";
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);

    dbms::StorageEngine completingBackend;
    assert(completingBackend.query(
               db, "accounts", {"=id 7"}, {"id"}).empty());

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "accounts", {{"id", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.prepareTransaction("prepared_warm_commit") ==
           dbms::DBStatus::OK);
    assert(completingBackend.commitPrepared("prepared_warm_commit") ==
           dbms::DBStatus::OK);
    assert(completingBackend.query(
               db, "accounts", {"=id 7"}, {"id"}).size() == 1);

    cleanup(db);
    std::cout << "[PREPARED-TXN] warm completion cache refreshed OK\n";
}

void test_commit_clog_failure_is_irrevocable() {
    const std::string db = testDbPath("prepared_clog_failure");
    cleanup(db);
    uint64_t xid = 0;
    {
        dbms::StorageEngine engine;
        assert(engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
        dbms::TableSchema table;
        table.tablename = "accounts";
        table.append(dbms::makeIntColumn("id", false, 2, true));
        assert(engine.createTable(db, table) == dbms::DBStatus::OK);

        assert(engine.beginTransaction(db) == dbms::DBStatus::OK);
        assert(engine.insert(db, "accounts", {{"id", "3"}}) ==
               dbms::DBStatus::OK);
        xid = engine.currentTxnId();
        assert(engine.prepareTransaction("prepared_clog_failure") ==
               dbms::DBStatus::OK);

        (void)engine.getCommitLog(db);
        std::error_code error;
        std::filesystem::remove_all(
            std::filesystem::path(db) / "pg_xact", error);
        assert(!error);
        assert(engine.commitPrepared("prepared_clog_failure") ==
               dbms::DBStatus::OK);
        assert(engine.listPreparedTransactions().empty());
        assert(engine.query(db, "accounts", {"=id 3"}, {"id"}).size() == 1);

        size_t commitRecords = 0;
        size_t abortRecords = 0;
        dbms::WALManager* wal = engine.getWAL(db);
        assert(wal);
        for (dbms::Lsn lsn = wal->earliestAvailableLsn();;) {
            const auto record = wal->ReadRecord(lsn);
            if (!record || record->header.xl_tot_len == 0) break;
            if (record->rmid() == dbms::RM_XACT_ID &&
                record->header.xl_xid == xid) {
                if (record->info() == dbms::XLOG_XACT_COMMIT) ++commitRecords;
                if (record->info() == dbms::XLOG_XACT_ABORT) ++abortRecords;
            }
            lsn += record->header.xl_tot_len;
        }
        assert(commitRecords == 1);
        assert(abortRecords == 0);

        dbms::StorageEngine recovered;
        assert(recovered.query(db, "accounts", {"=id 3"}, {"id"}).size() == 1);
        dbms::CommitLog* recoveredClog = recovered.getCommitLog(db);
        assert(recoveredClog);
        assert(recoveredClog->getStatus(xid) ==
               dbms::CommitLog::Status::Committed);
    }
    cleanup(db);
    std::cout << "[PREPARED-TXN] durable COMMIT survives CLOG publication failure OK\n";
}

void test_terminal_metadata_cleanup_is_idempotent() {
    const std::string db = testDbPath("prepared_terminal_cleanup");
    cleanup(db);
    {
        dbms::StorageEngine engine;
        assert(engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
        dbms::TableSchema table;
        table.tablename = "accounts";
        table.append(dbms::makeIntColumn("id", false, 2, true));
        assert(engine.createTable(db, table) == dbms::DBStatus::OK);

        const std::filesystem::path preparedDir =
            std::filesystem::path("info") / ".prepared";
        const auto terminalCounts = [&](uint64_t xid) {
            size_t commits = 0;
            size_t aborts = 0;
            dbms::WALManager* wal = engine.getWAL(db);
            assert(wal);
            for (dbms::Lsn lsn = wal->earliestAvailableLsn();;) {
                const auto record = wal->ReadRecord(lsn);
                if (!record || record->header.xl_tot_len == 0) break;
                if (record->rmid() == dbms::RM_XACT_ID &&
                    record->header.xl_xid == xid) {
                    if (record->info() == dbms::XLOG_XACT_COMMIT) ++commits;
                    if (record->info() == dbms::XLOG_XACT_ABORT) ++aborts;
                }
                lsn += record->header.xl_tot_len;
            }
            return std::make_pair(commits, aborts);
        };

        assert(engine.beginTransaction(db) == dbms::DBStatus::OK);
        assert(engine.insert(db, "accounts", {{"id", "10"}}) ==
               dbms::DBStatus::OK);
        const uint64_t committedXid = engine.currentTxnId();
        assert(engine.prepareTransaction("retained_commit") ==
               dbms::DBStatus::OK);
        assert(::chmod(preparedDir.c_str(), 0500) == 0);
        assert(engine.commitPrepared("retained_commit") == dbms::DBStatus::OK);
        assert(::chmod(preparedDir.c_str(), 0700) == 0);
        assert(std::filesystem::exists(preparedDir / "retained_commit"));
        assert(engine.listPreparedTransactions().empty());
        assert(engine.rollbackPrepared("retained_commit") ==
               dbms::DBStatus::INVALID_VALUE);
        assert(!std::filesystem::exists(preparedDir / "retained_commit"));
        assert((terminalCounts(committedXid) ==
                std::pair<size_t, size_t>{1, 0}));
        assert(engine.query(db, "accounts", {"=id 10"}, {"id"}).size() == 1);

        assert(engine.beginTransaction(db) == dbms::DBStatus::OK);
        assert(engine.insert(db, "accounts", {{"id", "11"}}) ==
               dbms::DBStatus::OK);
        const uint64_t abortedXid = engine.currentTxnId();
        assert(engine.prepareTransaction("retained_abort") ==
               dbms::DBStatus::OK);
        assert(::chmod(preparedDir.c_str(), 0500) == 0);
        assert(engine.rollbackPrepared("retained_abort") == dbms::DBStatus::OK);
        assert(::chmod(preparedDir.c_str(), 0700) == 0);
        assert(std::filesystem::exists(preparedDir / "retained_abort"));
        assert(engine.listPreparedTransactions().empty());
        assert(engine.commitPrepared("retained_abort") ==
               dbms::DBStatus::INVALID_VALUE);
        assert(!std::filesystem::exists(preparedDir / "retained_abort"));
        assert((terminalCounts(abortedXid) ==
                std::pair<size_t, size_t>{0, 1}));
        assert(engine.query(db, "accounts", {"=id 11"}, {"id"}).empty());
    }
    cleanup(db);
    std::cout << "[PREPARED-TXN] terminal metadata cleanup is idempotent OK\n";
}

int runPreparedRestartWorker() {
    const std::string db = testDbPath("prepared_restart");
    dbms::StorageEngine source;
    assert(source.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "restart_rows";
    table.append(dbms::makeIntColumn("id", false, 2, true));
    assert(source.createTable(db, table) == dbms::DBStatus::OK);
    assert(source.beginTransaction(db) == dbms::DBStatus::OK);
    assert(source.insert(db, "restart_rows", {{"id", "7"}}) == dbms::DBStatus::OK);
    source.getLockManager().setResourceNamespace(db);
    assert(source.getLockManager().lockExclusive("restart_rows"));
    assert(source.getLockManager().rowLockExclusive("restart_rows", 7));
    assert(source.getLockManager().pageLockExclusive(db, "restart_rows", 1));
    assert(source.getLockManager().lockGap("restart_rows", "", "~"));
    assert(source.prepareTransaction("prepared_restart_commit") == dbms::DBStatus::OK);
    return 0;
}

int runPreparedRestartVerifier() {
    const std::string db = testDbPath("prepared_restart");
    dbms::StorageEngine restarted;
    const auto prepared = restarted.listPreparedTransactions();
    assert(std::find(prepared.begin(), prepared.end(),
                     "prepared_restart_commit") != prepared.end());
    restarted.getLockManager().setResourceNamespace(db);
    restarted.getLockManager().setLockTimeout(50);
    // A separate process must have reconstructed the durable table lock
    // before exposing the engine. The explicit second phase is required.
    assert(!restarted.getLockManager().lockExclusive("restart_rows"));
    assert(!restarted.getLockManager().rowLockShared("restart_rows", 7));
    assert(!restarted.getLockManager().pageLockShared(db, "restart_rows", 1));
    assert(!restarted.getLockManager().lockGap("restart_rows", "", "~"));
    assert(restarted.query(db, "restart_rows", {}, {"id"}).empty());
    assert(restarted.query(db, "restart_rows", {"=id 7"}, {"id"}).empty());
    assert(restarted.commitPrepared("prepared_restart_commit") == dbms::DBStatus::OK);
    assert(restarted.getLockManager().lockExclusive("restart_rows"));
    restarted.getLockManager().unlock("restart_rows");
    assert(restarted.getLockManager().rowLockExclusive("restart_rows", 7));
    restarted.getLockManager().rowUnlock("restart_rows", 7);
    assert(restarted.getLockManager().pageLockExclusive(db, "restart_rows", 1));
    restarted.getLockManager().pageUnlock(db, "restart_rows", 1);
    assert(restarted.getLockManager().lockGap("restart_rows", "", "~"));
    restarted.getLockManager().unlockGaps("restart_rows");
    assert(restarted.query(db, "restart_rows", {}, {"id"}).size() == 1);
    return 0;
}

void test_prepared_survives_engine_restart(const char* executable) {
    const std::string db = testDbPath("prepared_restart");
    cleanup(db);

    const auto runChild = [&](const char* mode) {
        const pid_t child = ::fork();
        assert(child >= 0);
        if (child == 0) {
            ::execl(executable, executable, mode, static_cast<char*>(nullptr));
            ::_exit(127);
        }
        int status = 0;
        assert(::waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    };

    runChild("--prepared-restart-worker");
    runChild("--prepared-restart-verifier");

    cleanup(db);
    std::cout << "[PREPARED-TXN] cross-process restart preserves locks and in-doubt state OK\n";
}

} // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--prepared-restart-worker") {
        return runPreparedRestartWorker();
    }
    if (argc == 2 && std::string(argv[1]) == "--prepared-restart-verifier") {
        return runPreparedRestartVerifier();
    }
    cleanupAllTestData();
    test_prepare_rejects_deferred_constraint_violation();
    test_prepare_resets_originating_transaction_modes();
    test_prepare_rejects_physical_ddl_snapshot();
    test_prepare_rejects_temporary_relation_writes();
    test_cross_backend_prepare_completion();
    test_prepared_update_allows_mvcc_reader();
    test_quoted_names_round_trip_through_prepared_metadata();
    test_commit_refreshes_warm_completion_backend();
    test_commit_clog_failure_is_irrevocable();
    test_terminal_metadata_cleanup_is_idempotent();
    test_prepared_survives_engine_restart(argv[0]);
    finalCleanupTestData();
    return 0;
}
