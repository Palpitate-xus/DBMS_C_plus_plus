// Independently constructed StorageEngine backends must share table/row/gap
// locks while keeping identically named tables in different databases apart.

#include "TableManage.h"
#include "test_utils.h"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

int main() {
    const std::string dbA = testDbPath("cross_backend_lock_a");
    const std::string dbB = testDbPath("cross_backend_lock_b");
    cleanupTestDb("cross_backend_lock_a");
    cleanupTestDb("cross_backend_lock_b");

    dbms::StorageEngine first;
    dbms::StorageEngine second;
    assert(first.createDatabase(dbA) == dbms::DBStatus::OK);
    assert(first.createDatabase(dbB) == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 0, true));
    assert(first.createTable(dbA, table) == dbms::DBStatus::OK);
    assert(first.createTable(dbB, table) == dbms::DBStatus::OK);

    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool held = false;
    bool release = false;
    std::atomic<bool> blocked{false};

    std::thread holder([&] {
        first.getLockManager().setResourceNamespace(dbA);
        assert(first.getLockManager().lockExclusive("items"));
        {
            std::lock_guard<std::mutex> lock(gateMutex);
            held = true;
        }
        gateCv.notify_all();
        std::unique_lock<std::mutex> lock(gateMutex);
        gateCv.wait(lock, [&] { return release; });
        first.getLockManager().unlock("items");
    });

    {
        std::unique_lock<std::mutex> lock(gateMutex);
        gateCv.wait(lock, [&] { return held; });
    }

    second.getLockManager().setLockTimeout(50);
    std::thread waiter([&] {
        second.getLockManager().setResourceNamespace(dbA);
        blocked = !second.getLockManager().lockShared("items");
    });
    waiter.join();
    assert(blocked.load());

    // The same logical table name in another database is independent.
    second.getLockManager().setResourceNamespace(dbB);
    assert(second.getLockManager().lockShared("items"));
    second.getLockManager().unlock("items");

    {
        std::lock_guard<std::mutex> lock(gateMutex);
        release = true;
    }
    gateCv.notify_all();
    holder.join();

    cleanupTestDb("cross_backend_lock_a");
    cleanupTestDb("cross_backend_lock_b");
    std::cout << "[CROSS BACKEND LOCK] shared registry and database isolation OK\n";
    return 0;
}
