#include "commands/TableManage.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <sys/wait.h>
#include <unistd.h>

extern dbms::StorageEngine g_engine;

namespace {

void cleanup(const std::string& db) {
    std::error_code ec;
    std::filesystem::remove_all(db, ec);
    std::filesystem::remove_all("info/.prepared", ec);
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

    cleanup(db);
    std::cout << "[PREPARED-TXN] cross-backend commit/rollback and lock ownership OK\n";
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
    test_cross_backend_prepare_completion();
    test_prepared_survives_engine_restart(argv[0]);
    finalCleanupTestData();
    return 0;
}
