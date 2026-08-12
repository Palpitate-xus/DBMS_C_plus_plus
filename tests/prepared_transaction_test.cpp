#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>

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

} // namespace

int main() {
    cleanupAllTestData();
    test_cross_backend_prepare_completion();
    finalCleanupTestData();
    return 0;
}
