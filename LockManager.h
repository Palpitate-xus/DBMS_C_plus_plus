#pragma once

#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace dbms {

// Simple table-level lock manager with shared (read) / exclusive (write) locks
class LockManager {
public:
    // Acquire shared lock (for SELECT). Returns true on success.
    bool lockShared(const std::string& table);

    // Acquire exclusive lock (for INSERT/UPDATE/DELETE). Returns true on success.
    bool lockExclusive(const std::string& table);

    // Release all locks held for a table
    void unlock(const std::string& table);

    // Release all locks (used on COMMIT/ROLLBACK)
    void unlockAll();

    // Get list of currently locked tables
    std::vector<std::string> lockedTables() const;

private:
    struct LockState {
        std::shared_mutex mtx;
        int sharedCount = 0;
        bool exclusive = false;
    };
    std::map<std::string, LockState> locks_;
    std::mutex globalMutex_;
};

} // namespace dbms
