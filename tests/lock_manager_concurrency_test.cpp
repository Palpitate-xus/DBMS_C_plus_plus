// Lock manager concurrency regression: deadlock detection and per-thread
// lock cleanup must not release another thread's tokens.

#include "LockManager.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

using namespace dbms;

int main() {
    LockManager manager;
    manager.setLockTimeout(2000);
    manager.clearDeadlockLog();

    std::mutex gateMutex;
    std::condition_variable gateCv;
    int ready = 0;
    bool proceed = false;
    bool firstFailed = false;
    bool secondFailed = false;

    auto deadlockWorker = [&](const char* first, const char* second,
                              bool& failed) {
        assert(manager.lockExclusive(first));
        {
            std::lock_guard<std::mutex> lock(gateMutex);
            ++ready;
        }
        gateCv.notify_all();
        {
            std::unique_lock<std::mutex> lock(gateMutex);
            gateCv.wait(lock, [&] { return proceed; });
        }
        failed = !manager.lockExclusive(second);
        if (!failed) manager.unlock(second);
        manager.unlock(first);
    };

    std::thread first(deadlockWorker, "deadlock_a", "deadlock_b",
                      std::ref(firstFailed));
    std::thread second(deadlockWorker, "deadlock_b", "deadlock_a",
                       std::ref(secondFailed));
    {
        std::unique_lock<std::mutex> lock(gateMutex);
        gateCv.wait(lock, [&] { return ready == 2; });
        proceed = true;
    }
    gateCv.notify_all();
    first.join();
    second.join();
    assert(firstFailed != secondFailed);
    assert(!manager.getDeadlockLog().empty());
    assert(manager.getLockWaits().empty());
    assert(manager.getLockHolds().empty());
    std::cout << "[LOCK] two-thread deadlock detection and victim release OK\n";

    // Re-entrant table locks use one underlying mutex token and balanced
    // logical depth; upgrading a sole shared holder must not self-deadlock.
    assert(manager.lockShared("reentrant"));
    assert(manager.lockShared("reentrant"));
    manager.unlock("reentrant");
    assert(!manager.lockedTables().empty());
    manager.unlock("reentrant");
    assert(manager.lockedTables().empty());
    assert(manager.lockShared("upgrade"));
    assert(manager.lockExclusive("upgrade"));
    manager.unlock("upgrade");
    assert(manager.lockedTables().empty());
    std::cout << "[LOCK] re-entrant and upgrade paths OK\n";

    // A bulk unlock from an unrelated thread must not release the shared
    // tokens held by the actual readers.
    std::mutex readersMutex;
    std::condition_variable readersCv;
    int readersReady = 0;
    bool releaseReaders = false;
    std::thread readerOne([&] {
        assert(manager.rowLockShared("rows", 7));
        {
            std::lock_guard<std::mutex> lock(readersMutex);
            ++readersReady;
        }
        readersCv.notify_all();
        std::unique_lock<std::mutex> lock(readersMutex);
        readersCv.wait(lock, [&] { return releaseReaders; });
        manager.rowUnlock("rows", 7);
    });
    std::thread readerTwo([&] {
        assert(manager.rowLockShared("rows", 7));
        {
            std::lock_guard<std::mutex> lock(readersMutex);
            ++readersReady;
        }
        readersCv.notify_all();
        std::unique_lock<std::mutex> lock(readersMutex);
        readersCv.wait(lock, [&] { return releaseReaders; });
        manager.rowUnlock("rows", 7);
    });
    {
        std::unique_lock<std::mutex> lock(readersMutex);
        readersCv.wait(lock, [&] { return readersReady == 2; });
    }
    manager.rowUnlockAll("rows");

    std::atomic<bool> writerAcquired{false};
    std::thread writer([&] {
        writerAcquired = manager.rowLockExclusive("rows", 7);
        if (writerAcquired) manager.rowUnlock("rows", 7);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    assert(!writerAcquired.load());
    {
        std::lock_guard<std::mutex> lock(readersMutex);
        releaseReaders = true;
    }
    readersCv.notify_all();
    readerOne.join();
    readerTwo.join();
    writer.join();
    assert(writerAcquired.load());
    assert(manager.lockedRows("rows").empty());
    manager.pageUnlockAll();
    manager.unlockAll();
    std::cout << "[LOCK] row token ownership and cleanup OK\n";

    std::cout << "[LOCK] all passed\n";
    return 0;
}
