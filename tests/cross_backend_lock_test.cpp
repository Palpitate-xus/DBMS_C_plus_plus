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
#include <sys/wait.h>
#include <unistd.h>

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

    std::thread waiter([&] {
        second.getLockManager().setResourceNamespace(dbA);
        second.getLockManager().setLockTimeout(50);
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

    // Wait policies belong to the backend thread, not to the shared
    // registry. A short timeout configured by one backend must not cause an
    // unconfigured backend to abort its wait before the holder releases.
    std::mutex policyMutex;
    std::condition_variable policyCv;
    bool policyHeld = false;
    bool policyRelease = false;
    std::thread configured([&] {
        first.getLockManager().setResourceNamespace(dbA);
        first.getLockManager().setLockTimeout(25);
        assert(first.getLockManager().lockExclusive("items"));
        {
            std::lock_guard<std::mutex> lock(policyMutex);
            policyHeld = true;
        }
        policyCv.notify_all();
        std::unique_lock<std::mutex> lock(policyMutex);
        policyCv.wait(lock, [&] { return policyRelease; });
        first.getLockManager().unlock("items");
    });
    {
        std::unique_lock<std::mutex> lock(policyMutex);
        policyCv.wait(lock, [&] { return policyHeld; });
    }

    std::atomic<bool> waitedAndAcquired{false};
    std::thread unconfigured([&] {
        second.getLockManager().setResourceNamespace(dbA);
        waitedAndAcquired = second.getLockManager().lockShared("items");
        if (waitedAndAcquired) second.getLockManager().unlock("items");
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(75));
    {
        std::lock_guard<std::mutex> lock(policyMutex);
        policyRelease = true;
    }
    policyCv.notify_all();
    configured.join();
    unconfigured.join();
    assert(waitedAndAcquired.load());

    // A separately forked process must observe the same database-scoped
    // advisory lock, and can acquire it after the parent releases it.
    first.getLockManager().setResourceNamespace(dbA);
    assert(first.getLockManager().lockExclusive("items"));
    pid_t child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        dbms::LockManager childManager;
        childManager.setResourceNamespace(dbA);
        childManager.setLockTimeout(75);
        const bool acquired = childManager.lockShared("items");
        ::_exit(acquired ? 1 : 0);
    }
    int childStatus = 0;
    assert(::waitpid(child, &childStatus, 0) == child);
    assert(WIFEXITED(childStatus) && WEXITSTATUS(childStatus) == 0);
    first.getLockManager().unlock("items");

    child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        dbms::LockManager childManager;
        childManager.setResourceNamespace(dbA);
        childManager.setLockTimeout(500);
        const bool acquired = childManager.lockExclusive("items");
        if (acquired) childManager.unlock("items");
        ::_exit(acquired ? 0 : 1);
    }
    childStatus = 0;
    assert(::waitpid(child, &childStatus, 0) == child);
    assert(WIFEXITED(childStatus) && WEXITSTATUS(childStatus) == 0);

    cleanupTestDb("cross_backend_lock_a");
    cleanupTestDb("cross_backend_lock_b");
    std::cout << "[CROSS BACKEND LOCK] shared registry and database isolation OK\n";
    return 0;
}
