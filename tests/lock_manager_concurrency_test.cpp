// Lock manager concurrency regression: deadlock detection and per-thread
// lock cleanup must not release another thread's tokens.

#include "LockManager.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

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

    // A waiter can hold the physical row mutex while publishing ownership.
    // Competing fast-path acquisitions must never block under rowMutex_ or
    // accumulate obsolete wait-for edges across handoffs.
    manager.clearDeadlockLog();
    constexpr int handoffWorkers = 12;
    constexpr int handoffIterations = 50;
    std::atomic<int> handoffReady{0};
    std::atomic<bool> handoffStart{false};
    std::atomic<bool> handoffFailed{false};
    std::vector<std::thread> handoffThreads;
    for (int worker = 0; worker < handoffWorkers; ++worker) {
        handoffThreads.emplace_back([&, worker] {
            manager.setLockTimeout(3000);
            handoffReady.fetch_add(1, std::memory_order_release);
            while (!handoffStart.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int iteration = 0; iteration < handoffIterations; ++iteration) {
                const bool shared = (iteration + worker) % 4 == 0;
                const bool acquired = shared
                    ? manager.rowLockShared("handoff_rows", 1)
                    : manager.rowLockExclusive("handoff_rows", 1);
                if (!acquired) {
                    handoffFailed.store(true, std::memory_order_release);
                    return;
                }
                manager.rowUnlock("handoff_rows", 1);
            }
        });
    }
    while (handoffReady.load(std::memory_order_acquire) != handoffWorkers) {
        std::this_thread::yield();
    }
    handoffStart.store(true, std::memory_order_release);
    for (auto& worker : handoffThreads) worker.join();
    assert(!handoffFailed.load(std::memory_order_acquire));
    assert(manager.getDeadlockLog().empty());
    assert(manager.lockedRows("handoff_rows").empty());
    std::cout << "[LOCK] row handoff contention OK\n";

    std::mutex noWaitMutex;
    std::condition_variable noWaitCv;
    bool noWaitHeld = false;
    bool noWaitRelease = false;
    std::thread noWaitHolder([&] {
        assert(manager.rowLockExclusive("no_wait_rows", 1));
        {
            std::lock_guard<std::mutex> lock(noWaitMutex);
            noWaitHeld = true;
        }
        noWaitCv.notify_all();
        std::unique_lock<std::mutex> lock(noWaitMutex);
        noWaitCv.wait(lock, [&] { return noWaitRelease; });
        manager.rowUnlock("no_wait_rows", 1);
    });
    {
        std::unique_lock<std::mutex> lock(noWaitMutex);
        noWaitCv.wait(lock, [&] { return noWaitHeld; });
    }
    const auto noWaitStart = std::chrono::steady_clock::now();
    assert(!manager.rowLockSharedNoWait("no_wait_rows", 1));
    assert(!manager.rowLockExclusiveNoWait("no_wait_rows", 1));
    assert(std::chrono::steady_clock::now() - noWaitStart <
           std::chrono::seconds(1));
    {
        std::lock_guard<std::mutex> lock(noWaitMutex);
        noWaitRelease = true;
    }
    noWaitCv.notify_all();
    noWaitHolder.join();
    assert(manager.rowLockExclusiveNoWait("no_wait_rows", 1));
    manager.rowUnlock("no_wait_rows", 1);
    assert(manager.lockedRows("no_wait_rows").empty());
    std::cout << "[LOCK] row no-wait contract OK\n";

    // NOWAIT only rejects an incompatible holder: two FOR SHARE readers may
    // hold the same row concurrently, including when the second uses NOWAIT.
    assert(manager.rowLockShared("compatible_nowait_rows", 1));
    std::atomic<bool> compatibleSharedAcquired{false};
    std::atomic<bool> compatibleSharedAttempted{false};
    std::atomic<bool> releaseCompatibleReader{false};
    std::thread compatibleSharedReader([&] {
        const bool acquired = manager.rowLockSharedNoWait("compatible_nowait_rows", 1);
        compatibleSharedAcquired.store(acquired, std::memory_order_release);
        compatibleSharedAttempted.store(true, std::memory_order_release);
        if (acquired) {
            while (!releaseCompatibleReader.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            manager.rowUnlock("compatible_nowait_rows", 1);
        }
    });
    while (!compatibleSharedAttempted.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    assert(compatibleSharedAcquired.load(std::memory_order_acquire));
    assert(!manager.rowLockExclusiveNoWait("compatible_nowait_rows", 1));
    releaseCompatibleReader.store(true, std::memory_order_release);
    compatibleSharedReader.join();
    manager.rowUnlock("compatible_nowait_rows", 1);
    assert(manager.lockedRows("compatible_nowait_rows").empty());
    std::cout << "[LOCK] compatible shared row NOWAIT OK\n";

    // A timed-out upgrade must retain the original shared token even when
    // the caller restores an earlier savepoint-style lock checkpoint.
    LockManager upgradeManager;
    upgradeManager.setLockTimeout(75);
    assert(upgradeManager.rowLockShared("upgrade_timeout_rows", 1));
    const auto sharedCheckpoint = upgradeManager.captureCheckpoint();
    std::mutex upgradeMutex;
    std::condition_variable upgradeCv;
    bool competingShareHeld = false;
    bool releaseCompetingShare = false;
    std::thread competingShare([&] {
        assert(upgradeManager.rowLockShared("upgrade_timeout_rows", 1));
        {
            std::lock_guard<std::mutex> lock(upgradeMutex);
            competingShareHeld = true;
        }
        upgradeCv.notify_all();
        std::unique_lock<std::mutex> lock(upgradeMutex);
        upgradeCv.wait(lock, [&] { return releaseCompetingShare; });
        lock.unlock();
        upgradeManager.rowUnlock("upgrade_timeout_rows", 1);
    });
    {
        std::unique_lock<std::mutex> lock(upgradeMutex);
        upgradeCv.wait(lock, [&] { return competingShareHeld; });
    }
    assert(!upgradeManager.rowLockExclusiveNoWait("upgrade_timeout_rows", 1));
    assert(!upgradeManager.rowLockExclusive("upgrade_timeout_rows", 1));
    assert(!upgradeManager.lastRowLockWasDeadlock());
    upgradeManager.rollbackToCheckpoint(sharedCheckpoint);
    const auto afterFailedUpgrade = upgradeManager.captureCheckpoint();
    assert(afterFailedUpgrade.rowLocks == sharedCheckpoint.rowLocks);
    assert(afterFailedUpgrade.rowModes == sharedCheckpoint.rowModes);
    {
        std::lock_guard<std::mutex> lock(upgradeMutex);
        releaseCompetingShare = true;
    }
    upgradeCv.notify_all();
    competingShare.join();
    std::atomic<bool> failedUpgradeKeptShare{false};
    std::thread exclusiveProbe([&] {
        const bool acquired = upgradeManager.rowLockExclusiveNoWait("upgrade_timeout_rows", 1);
        failedUpgradeKeptShare = !acquired;
        if (acquired) upgradeManager.rowUnlock("upgrade_timeout_rows", 1);
    });
    exclusiveProbe.join();
    assert(failedUpgradeKeptShare.load());
    upgradeManager.rowUnlock("upgrade_timeout_rows", 1);
    assert(upgradeManager.lockedRows("upgrade_timeout_rows").empty());
    std::cout << "[LOCK] failed row upgrade preserves checkpoint SHARE OK\n";

    // Two upgraders retain their shared tokens while waiting; therefore the
    // wait graph has a real cycle and selects exactly one deadlock victim.
    LockManager upgradeDeadlockManager;
    std::mutex upgradeDeadlockMutex;
    std::condition_variable upgradeDeadlockCv;
    int upgradeReaders = 0;
    bool attemptUpgrade = false;
    std::atomic<int> upgradeSuccesses{0};
    std::atomic<int> upgradeDeadlocks{0};
    auto upgradeWorker = [&] {
        upgradeDeadlockManager.setLockTimeout(5000);
        assert(upgradeDeadlockManager.rowLockShared("upgrade_deadlock_rows", 1));
        {
            std::lock_guard<std::mutex> lock(upgradeDeadlockMutex);
            ++upgradeReaders;
        }
        upgradeDeadlockCv.notify_all();
        {
            std::unique_lock<std::mutex> lock(upgradeDeadlockMutex);
            upgradeDeadlockCv.wait(lock, [&] { return attemptUpgrade; });
        }
        if (upgradeDeadlockManager.rowLockExclusive("upgrade_deadlock_rows", 1)) {
            ++upgradeSuccesses;
        } else if (upgradeDeadlockManager.lastRowLockWasDeadlock()) {
            ++upgradeDeadlocks;
        }
        upgradeDeadlockManager.rowUnlock("upgrade_deadlock_rows", 1);
    };
    std::thread upgraderA(upgradeWorker);
    std::thread upgraderB(upgradeWorker);
    {
        std::unique_lock<std::mutex> lock(upgradeDeadlockMutex);
        upgradeDeadlockCv.wait(lock, [&] { return upgradeReaders == 2; });
        attemptUpgrade = true;
    }
    upgradeDeadlockCv.notify_all();
    upgraderA.join();
    upgraderB.join();
    assert(upgradeSuccesses.load() == 1 && upgradeDeadlocks.load() == 1);
    assert(upgradeDeadlockManager.lockedRows("upgrade_deadlock_rows").empty());
    assert(upgradeDeadlockManager.getLockWaits().empty());
    std::cout << "[LOCK] shared row upgrade deadlock detection OK\n";

    // Checkpoint restoration must not wait on a competing local SHARE while
    // holding the row registry mutex. Use a child watchdog so a regression
    // cannot hang the entire suite; no timing determines the expected result.
    const pid_t checkpointChild = ::fork();
    assert(checkpointChild >= 0);
    if (checkpointChild == 0) {
        ::alarm(8);
        LockManager restoreManager;
        assert(restoreManager.rowLockExclusive("restore_exclusive_rows", 1));
        const auto exclusiveCheckpoint = restoreManager.captureCheckpoint();
        restoreManager.rowUnlock("restore_exclusive_rows", 1);
        assert(restoreManager.rowLockShared("restore_exclusive_rows", 1));
        const auto sharedCheckpoint = restoreManager.captureCheckpoint();

        std::mutex mutex;
        std::condition_variable cv;
        bool peerHeld = false;
        bool releasePeer = false;
        std::thread peer([&] {
            assert(restoreManager.rowLockShared("restore_exclusive_rows", 1));
            {
                std::lock_guard<std::mutex> lock(mutex);
                peerHeld = true;
            }
            cv.notify_all();
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [&] { return releasePeer; });
            lock.unlock();
            restoreManager.rowUnlock("restore_exclusive_rows", 1);
        });
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [&] { return peerHeld; });
        }
        restoreManager.rollbackToCheckpoint(exclusiveCheckpoint);
        const auto whileCompeting = restoreManager.captureCheckpoint();
        assert(whileCompeting.rowLocks == sharedCheckpoint.rowLocks &&
               whileCompeting.rowModes == sharedCheckpoint.rowModes);
        {
            std::lock_guard<std::mutex> lock(mutex);
            releasePeer = true;
        }
        cv.notify_all();
        peer.join();
        restoreManager.rollbackToCheckpoint(exclusiveCheckpoint);
        const auto restored = restoreManager.captureCheckpoint();
        assert(restored.rowLocks == exclusiveCheckpoint.rowLocks &&
               restored.rowModes == exclusiveCheckpoint.rowModes);
        restoreManager.rowUnlock("restore_exclusive_rows", 1);
        ::_exit(0);
    }
    int checkpointStatus = 0;
    assert(::waitpid(checkpointChild, &checkpointStatus, 0) == checkpointChild);
    assert(WIFEXITED(checkpointStatus) && WEXITSTATUS(checkpointStatus) == 0);
    std::cout << "[LOCK] checkpoint promotion with competing SHARE OK\n";

    // Savepoint-style lock checkpoints retain pre-savepoint locks but release
    // every table/row/page/gap token acquired afterward.
    std::filesystem::create_directories("checkpoint_db");
    LockManager checkpointManager;
    assert(checkpointManager.lockShared("before_savepoint"));
    assert(checkpointManager.rowLockShared("checkpoint_rows", 1));
    assert(checkpointManager.pageLockShared("checkpoint_db", "checkpoint_pages", 1));
    assert(checkpointManager.lockGap("checkpoint_gaps", "", "~"));
    const auto checkpoint = checkpointManager.captureCheckpoint();
    assert(checkpointManager.lockExclusive("before_savepoint"));
    assert(checkpointManager.rowLockExclusive("checkpoint_rows", 1));
    assert(checkpointManager.pageLockExclusive("checkpoint_db", "checkpoint_pages", 1));
    assert(checkpointManager.lockExclusive("after_savepoint"));
    assert(checkpointManager.rowLockExclusive("checkpoint_rows", 2));
    assert(checkpointManager.pageLockExclusive("checkpoint_db", "checkpoint_pages", 2));
    assert(checkpointManager.lockGap("checkpoint_gaps", "1", "2"));
    checkpointManager.rollbackToCheckpoint(checkpoint);

    assert(!checkpointManager.lockedTables().empty());
    std::atomic<bool> restoredSharedMode{false};
    std::atomic<bool> restoredRowSharedMode{false};
    std::atomic<bool> restoredPageSharedMode{false};
    std::thread restoredSharedReader([&] {
        checkpointManager.setLockTimeout(100);
        restoredSharedMode = checkpointManager.lockShared("before_savepoint");
        if (restoredSharedMode) checkpointManager.unlock("before_savepoint");
        restoredRowSharedMode = checkpointManager.rowLockShared("checkpoint_rows", 1);
        if (restoredRowSharedMode) checkpointManager.rowUnlock("checkpoint_rows", 1);
        restoredPageSharedMode = checkpointManager.pageLockShared(
            "checkpoint_db", "checkpoint_pages", 1);
        if (restoredPageSharedMode) {
            checkpointManager.pageUnlock("checkpoint_db", "checkpoint_pages", 1);
        }
    });
    restoredSharedReader.join();
    assert(restoredSharedMode.load());
    assert(restoredRowSharedMode.load());
    assert(restoredPageSharedMode.load());
    assert(checkpointManager.lockedRows("checkpoint_rows").size() == 1);
    assert(checkpointManager.lockedRows("checkpoint_rows").front() == 1);
    assert(checkpointManager.lockGap("checkpoint_gaps", "2", "3"));
    checkpointManager.unlockGaps("checkpoint_gaps");

    std::atomic<bool> postSavepointTableAcquired{false};
    std::atomic<bool> postSavepointRowAcquired{false};
    std::atomic<bool> postSavepointPageAcquired{false};
    std::thread postSavepointWaiter([&] {
        checkpointManager.setLockTimeout(100);
        postSavepointTableAcquired = checkpointManager.lockExclusive("after_savepoint");
        if (postSavepointTableAcquired) checkpointManager.unlock("after_savepoint");
        postSavepointRowAcquired = checkpointManager.rowLockExclusive("checkpoint_rows", 2);
        if (postSavepointRowAcquired) checkpointManager.rowUnlock("checkpoint_rows", 2);
        postSavepointPageAcquired = checkpointManager.pageLockExclusive(
            "checkpoint_db", "checkpoint_pages", 2);
        if (postSavepointPageAcquired) {
            checkpointManager.pageUnlock("checkpoint_db", "checkpoint_pages", 2);
        }
    });
    postSavepointWaiter.join();
    assert(postSavepointTableAcquired.load());
    assert(postSavepointRowAcquired.load());
    assert(postSavepointPageAcquired.load());
    checkpointManager.rowUnlock("checkpoint_rows", 1);
    checkpointManager.pageUnlock("checkpoint_db", "checkpoint_pages", 1);
    checkpointManager.unlock("before_savepoint");

    // A placement probe must leave an existing page lock untouched, even
    // when the calling thread holds the shared lock itself.
    assert(checkpointManager.pageLockShared("checkpoint_db", "checkpoint_pages", 3));
    assert(!checkpointManager.pageLockExclusiveNoWait("checkpoint_db", "checkpoint_pages", 3));
    std::thread placementProbe([&] {
        checkpointManager.setLockTimeout(100);
        assert(!checkpointManager.pageLockExclusiveNoWait("checkpoint_db", "checkpoint_pages", 3));
    });
    placementProbe.join();
    assert(checkpointManager.getLockWaits().empty());
    checkpointManager.pageUnlock("checkpoint_db", "checkpoint_pages", 3);
    assert(checkpointManager.pageLockExclusiveNoWait("checkpoint_db", "checkpoint_pages", 3));
    assert(!checkpointManager.pageLockExclusiveNoWait("checkpoint_db", "checkpoint_pages", 3));
    checkpointManager.pageUnlock("checkpoint_db", "checkpoint_pages", 3);
    assert(checkpointManager.pageLockExclusiveNoWait("checkpoint_db", "checkpoint_pages", 3));
    checkpointManager.pageUnlock("checkpoint_db", "checkpoint_pages", 3);

    std::filesystem::remove_all("checkpoint_db");
    std::cout << "[LOCK] savepoint checkpoint releases post-savepoint tokens OK\n";

    std::cout << "[LOCK] all passed\n";
    return 0;
}
