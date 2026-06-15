#include "TableManage.h"
#include "Config.h"
#include <filesystem>
#include <iostream>
#include <cassert>

dbms::Config g_config;

using namespace dbms;

static bool rowContains(const std::vector<std::string>& rows, const std::string& substring) {
    for (const auto& row : rows) {
        if (row.find(substring) != std::string::npos) return true;
    }
    return false;
}

static bool hasExactly(const std::vector<std::string>& rows, size_t n) {
    return rows.size() == n;
}

int main() {
    std::string dbname = "hot_update_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        DBStatus r = engine.createDatabase(dbname);
    if (r != DBStatus::OK) {
        std::cerr << "createDatabase failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    // Create a formatVersion=2 table with no indexes/PK so HOT update is eligible.
    TableSchema tbl;
    tbl.tablename = "hot_t";
    tbl.formatVersion = 2;
    tbl.append(makeIntColumn("id", false, 0, false));
    tbl.append(makeVarCharColumn("payload", false, 50, false));
    r = engine.createTable(dbname, tbl);
    if (r != DBStatus::OK) {
        std::cerr << "createTable failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    // Insert initial row inside a transaction.
    r = engine.beginTransaction(dbname);
    if (r != DBStatus::OK) {
        std::cerr << "beginTransaction failed\n";
        return 1;
    }

    std::map<std::string, std::string> vals;
    vals["id"] = "1";
    vals["payload"] = "original";
    r = engine.insert(dbname, "hot_t", vals);
    if (r != DBStatus::OK) {
        std::cerr << "insert failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    r = engine.commitTransaction();
    if (r != DBStatus::OK) {
        std::cerr << "commit failed\n";
        return 1;
    }

    // HOT update: change only non-indexed payload column inside a new transaction.
    r = engine.beginTransaction(dbname);
    if (r != DBStatus::OK) {
        std::cerr << "beginTransaction 2 failed\n";
        return 1;
    }

    std::map<std::string, std::string> updates;
    updates["payload"] = "hot_updated";
    r = engine.update(dbname, "hot_t", updates, {});
    if (r != DBStatus::OK) {
        std::cerr << "update failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    // Query within the same transaction should see the new value.
    auto rows = engine.query(dbname, "hot_t", {}, {"id", "payload"});
    if (!hasExactly(rows, 1) || !rowContains(rows, "hot_updated")) {
        std::cerr << "query after update did not see updated value\n";
        for (const auto& row : rows) std::cerr << "  " << row << "\n";
        return 1;
    }
    std::cout << "[HOT UPDATE TEST] in-transaction query sees updated value OK\n";

    r = engine.commitTransaction();
    if (r != DBStatus::OK) {
        std::cerr << "commit 2 failed\n";
        return 1;
    }

    // Query after commit should still see the new value.
    rows = engine.query(dbname, "hot_t", {}, {"id", "payload"});
    if (!hasExactly(rows, 1) || !rowContains(rows, "hot_updated")) {
        std::cerr << "query after commit did not see updated value\n";
        for (const auto& row : rows) std::cerr << "  " << row << "\n";
        return 1;
    }
    std::cout << "[HOT UPDATE TEST] post-commit query sees updated value OK\n";

    // Test rollback of an update: changes should not be visible after rollback.
    r = engine.beginTransaction(dbname);
    if (r != DBStatus::OK) {
        std::cerr << "beginTransaction 3 failed\n";
        return 1;
    }

    updates.clear();
    updates["payload"] = "rolled_back";
    r = engine.update(dbname, "hot_t", updates, {});
    if (r != DBStatus::OK) {
        std::cerr << "update 2 failed: " << static_cast<int>(r) << "\n";
        return 1;
    }

    rows = engine.query(dbname, "hot_t", {}, {"id", "payload"});
    if (!hasExactly(rows, 1) || !rowContains(rows, "rolled_back")) {
        std::cerr << "query before rollback did not see rolled_back value\n";
        return 1;
    }

    r = engine.rollbackTransaction();
    if (r != DBStatus::OK) {
        std::cerr << "rollback failed\n";
        return 1;
    }

    rows = engine.query(dbname, "hot_t", {}, {"id", "payload"});
    if (!hasExactly(rows, 1) || rowContains(rows, "rolled_back")) {
        std::cerr << "query after rollback still sees rolled_back value\n";
        for (const auto& row : rows) std::cerr << "  " << row << "\n";
        return 1;
    }
    std::cout << "[HOT UPDATE TEST] rollback restores old value OK\n";
    }

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[HOT UPDATE TEST] all passed\n";
    return 0;
}
