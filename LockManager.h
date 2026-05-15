#pragma once

#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace dbms {

// Table-level lock manager with shared (read) / exclusive (write) locks
// and deadlock detection via wait-for graph.
class LockManager {
public:
    // Acquire shared lock. Returns true on success, false if deadlock detected.
    bool lockShared(const std::string& table);

    // Acquire exclusive lock. Returns true on success, false if deadlock detected.
    bool lockExclusive(const std::string& table);

    // Release all locks held for a table
    void unlock(const std::string& table);

    // Release all locks (used on COMMIT/ROLLBACK)
    void unlockAll();

    // Get list of currently locked tables
    std::vector<std::string> lockedTables() const;

    // ========================================================================
    // Row-level locking
    // ========================================================================
    bool rowLockShared(const std::string& table, int64_t rid);
    bool rowLockExclusive(const std::string& table, int64_t rid);
    void rowUnlock(const std::string& table, int64_t rid);
    void rowUnlockAll(const std::string& table);
    std::vector<int64_t> lockedRows(const std::string& table) const;

private:
    struct LockState {
        std::shared_mutex mtx;
        int sharedCount = 0;
        bool exclusive = false;
        std::vector<std::thread::id> holders;  // threads currently holding this lock
    };
    std::map<std::string, LockState> locks_;
    std::mutex globalMutex_;

    // Row locks: key = "table:rid"
    std::map<std::string, LockState> rowLocks_;
    std::mutex rowMutex_;

    // Wait-for graph: thread A waits for thread B to release a lock
    std::map<std::thread::id, std::set<std::thread::id>> waitFor_;
    std::mutex waitMutex_;

    // Record that 'waiter' is waiting for 'holder'
    void addWaitEdge(std::thread::id waiter, std::thread::id holder);
    // Remove all outgoing wait edges from 'waiter'
    void removeWaitEdges(std::thread::id waiter);
    // Detect cycle in wait-for graph starting from 'start'
    bool hasCycle(std::thread::id start);
};

} // namespace dbms
