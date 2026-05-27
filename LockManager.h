#pragma once

#include <chrono>
#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace dbms {

// Table-level lock manager with shared (read) / exclusive (write) / intention locks
// metadata locks, and deadlock detection via wait-for graph.
class LockManager {
public:
    enum class LockMode { Shared, Exclusive, IntentShared, IntentExclusive, Metadata };

    // Acquire shared lock. Returns true on success, false if deadlock detected.
    bool lockShared(const std::string& table);
    // Acquire exclusive lock. Returns true on success, false if deadlock detected.
    bool lockExclusive(const std::string& table);
    // Acquire intent shared lock (used before row-level shared locks).
    bool lockIntentShared(const std::string& table);
    // Acquire intent exclusive lock (used before row-level exclusive locks).
    bool lockIntentExclusive(const std::string& table);
    // Acquire metadata lock (used for DDL operations like ALTER TABLE / DROP TABLE).
    bool lockMetadata(const std::string& table);

    // Release all locks held for a table
    void unlock(const std::string& table);

    // Release all locks (used on COMMIT/ROLLBACK)
    void unlockAll();

    // Get list of currently locked tables
    std::vector<std::string> lockedTables() const;

    // ========================================================================
    // Monitoring
    // ========================================================================
    struct DeadlockEntry {
        std::string timestamp;
        std::string description;
    };
    void logDeadlock(const std::string& description);
    std::vector<DeadlockEntry> getDeadlockLog() const;
    void clearDeadlockLog();

    struct LockWaitInfo {
        std::string resource;
        std::string waiterTid;
        std::vector<std::string> holderTids;
    };
    std::vector<LockWaitInfo> getLockWaits() const;

    struct LockHoldInfo {
        std::string resource;
        std::string holderTid;
        std::string mode; // "shared" | "exclusive" | "IS" | "IX" | "MDL"
    };
    std::vector<LockHoldInfo> getLockHolds() const;

    // ========================================================================
    // Row-level locking
    // ========================================================================
    bool rowLockShared(const std::string& table, int64_t rid);
    bool rowLockExclusive(const std::string& table, int64_t rid);
    // Non-blocking variants: return false immediately if lock cannot be acquired
    bool rowLockSharedNoWait(const std::string& table, int64_t rid);
    bool rowLockExclusiveNoWait(const std::string& table, int64_t rid);
    void rowUnlock(const std::string& table, int64_t rid);
    void rowUnlockAll(const std::string& table);
    std::vector<int64_t> lockedRows(const std::string& table) const;

    // ========================================================================
    // Gap locking (simplified: prevents INSERT in a key range)
    // ========================================================================
    // Lock a gap (range) on a table. Returns true if acquired, false if blocked.
    bool lockGap(const std::string& table, const std::string& leftKey, const std::string& rightKey);
    // Check if a key falls within any held gap lock
    bool isGapLocked(const std::string& table, const std::string& key) const;
    // Release all gap locks for a table held by current thread
    void unlockGaps(const std::string& table);
    // Release all gap locks held by current thread
    void unlockAllGaps();

    // ========================================================================
    // Page-level locking (used inside table-level locks for finer granularity)
    // Resource format: "page:dbname:tablename:pageId"
    // ========================================================================
    bool pageLockShared(const std::string& dbname, const std::string& table, uint32_t pageId) const;
    bool pageLockExclusive(const std::string& dbname, const std::string& table, uint32_t pageId) const;
    void pageUnlock(const std::string& dbname, const std::string& table, uint32_t pageId) const;
    void pageUnlockAll(const std::string& dbname, const std::string& table) const;
    void pageUnlockAll() const;

private:
    struct LockState {
        std::shared_mutex mtx;
        int sharedCount = 0;
        bool exclusive = false;
        int intentSharedCount = 0;
        int intentExclusiveCount = 0;
        bool metadata = false;
        std::vector<std::thread::id> holders;  // threads currently holding this lock
        std::map<std::thread::id, LockMode> holderModes; // mode per holder
    };
    std::map<std::string, LockState> locks_;
    mutable std::mutex globalMutex_;

    // Row locks: key = "table:rid"
    std::map<std::string, LockState> rowLocks_;
    mutable std::mutex rowMutex_;

    // Page locks: key = "page:dbname:table:pageId"
    mutable std::map<std::string, LockState> pageLocks_;
    mutable std::mutex pageMutex_;

    // Gap locks: table -> list of (leftKey, rightKey, holder)
    struct GapLock {
        std::string leftKey;
        std::string rightKey;
        std::thread::id holder;
    };
    std::map<std::string, std::vector<GapLock>> gapLocks_;
    mutable std::mutex gapMutex_;

    // Wait-for graph: thread A waits for thread B to release a lock
    std::map<std::thread::id, std::set<std::thread::id>> waitFor_;
    mutable std::mutex waitMutex_;

    // Deadlock log
    std::vector<DeadlockEntry> deadlockLog_;
    mutable std::mutex deadlockMutex_;

    // Internal acquire with mode
    bool acquireLock(const std::string& table, LockMode mode);

    // Record that 'waiter' is waiting for 'holder'
    void addWaitEdge(std::thread::id waiter, std::thread::id holder);
    // Remove all outgoing wait edges from 'waiter'
    void removeWaitEdges(std::thread::id waiter);
    // Detect cycle in wait-for graph starting from 'start'
    bool hasCycle(std::thread::id start);
    // Build deadlock description from current wait-for graph
    std::string buildDeadlockDescription(std::thread::id start) const;
};

} // namespace dbms
