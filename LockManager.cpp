#include "LockManager.h"

#include <algorithm>

namespace dbms {

// ========================================================================
// Wait-for graph helpers
// ========================================================================

void LockManager::addWaitEdge(std::thread::id waiter, std::thread::id holder) {
    std::lock_guard<std::mutex> guard(waitMutex_);
    waitFor_[waiter].insert(holder);
}

void LockManager::removeWaitEdges(std::thread::id waiter) {
    std::lock_guard<std::mutex> guard(waitMutex_);
    waitFor_.erase(waiter);
}

static std::string tidToString(std::thread::id tid) {
    std::ostringstream oss;
    oss << tid;
    return oss.str();
}

static std::string nowIso8601() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&time_t_now);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    return std::string(buf);
}

bool LockManager::hasCycle(std::thread::id start) {
    std::lock_guard<std::mutex> guard(waitMutex_);
    std::set<std::thread::id> visited, recStack;
    std::vector<std::thread::id> stack;
    stack.push_back(start);
    while (!stack.empty()) {
        std::thread::id node = stack.back();
        if (recStack.find(node) == recStack.end()) {
            recStack.insert(node);
            auto it = waitFor_.find(node);
            if (it != waitFor_.end()) {
                for (const auto& neighbor : it->second) {
                    if (recStack.find(neighbor) != recStack.end()) {
                        // Deadlock detected — record it before returning
                        std::string desc = "cycle: " + tidToString(node) + " -> " + tidToString(neighbor);
                        for (const auto& e : waitFor_) {
                            desc += "; " + tidToString(e.first) + " waits for ";
                            bool first = true;
                            for (const auto& h : e.second) {
                                if (!first) desc += ",";
                                desc += tidToString(h);
                                first = false;
                            }
                        }
                        {
                            std::lock_guard<std::mutex> dlGuard(deadlockMutex_);
                            deadlockLog_.push_back({nowIso8601(), desc});
                            if (deadlockLog_.size() > 100) deadlockLog_.erase(deadlockLog_.begin());
                        }
                        return true;
                    }
                    if (visited.find(neighbor) == visited.end()) {
                        stack.push_back(neighbor);
                    }
                }
            }
        } else {
            recStack.erase(node);
            visited.insert(node);
            stack.pop_back();
        }
        if (!stack.empty() && visited.find(stack.back()) != visited.end()) {
            stack.pop_back();
        }
    }
    return false;
}

// ========================================================================
// Lock acquisition with deadlock detection
// ========================================================================

bool LockManager::lockShared(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        auto& state = locks_[table];
        // Fast path: no exclusive lock held
        if (!state.exclusive && state.holders.empty()) {
            state.mtx.lock_shared();
            state.sharedCount++;
            state.holders.push_back(self);
            return true;
        }
        // Need to wait: check if any holder has exclusive lock
        if (state.exclusive && !state.holders.empty()) {
            addWaitEdge(self, state.holders[0]);
            if (hasCycle(self)) {
                // Deadlock detected - remove edge and fail
                removeWaitEdges(self);
                return false;
            }
        }
    }
    // Slow path: block waiting for the lock
    auto& state = locks_[table];
    state.mtx.lock_shared();
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        state.sharedCount++;
        state.holders.push_back(self);
        removeWaitEdges(self);
    }
    return true;
}

bool LockManager::lockExclusive(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        auto& state = locks_[table];
        // Fast path: no one holds the lock
        if (state.sharedCount == 0 && !state.exclusive) {
            state.mtx.lock();
            state.exclusive = true;
            state.holders.push_back(self);
            return true;
        }
        // Need to wait: add wait edges to all holders
        for (const auto& holder : state.holders) {
            if (holder != self) addWaitEdge(self, holder);
        }
        if (hasCycle(self)) {
            removeWaitEdges(self);
            return false;
        }
    }
    // Slow path: block waiting for the lock
    auto& state = locks_[table];
    state.mtx.lock();
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        state.exclusive = true;
        state.holders.push_back(self);
        removeWaitEdges(self);
    }
    return true;
}

// ========================================================================
// Unlock
// ========================================================================

void LockManager::unlock(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(globalMutex_);
    removeWaitEdges(self);
    auto it = locks_.find(table);
    if (it == locks_.end()) return;
    auto& state = it->second;
    // Remove self from holders list
    auto hit = std::find(state.holders.begin(), state.holders.end(), self);
    if (hit != state.holders.end()) state.holders.erase(hit);
    if (state.exclusive) {
        state.mtx.unlock();
        state.exclusive = false;
    } else if (state.sharedCount > 0) {
        state.mtx.unlock_shared();
        state.sharedCount--;
    }
}

void LockManager::unlockAll() {
    std::thread::id self = std::this_thread::get_id();
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        removeWaitEdges(self);
        for (auto& kv : locks_) {
            auto& state = kv.second;
            // Remove self from holders
            auto hit = std::find(state.holders.begin(), state.holders.end(), self);
            if (hit != state.holders.end()) state.holders.erase(hit);
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            }
            while (state.sharedCount > 0) {
                state.mtx.unlock_shared();
                state.sharedCount--;
            }
        }
    }
    // Also release all row locks held by this thread
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (auto it = rowLocks_.begin(); it != rowLocks_.end(); ) {
            auto& state = it->second;
            auto hit = std::find(state.holders.begin(), state.holders.end(), self);
            if (hit != state.holders.end()) state.holders.erase(hit);
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            }
            while (state.sharedCount > 0) {
                state.mtx.unlock_shared();
                state.sharedCount--;
            }
            if (state.sharedCount == 0 && !state.exclusive && state.holders.empty()) {
                it = rowLocks_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ========================================================================
// Helpers
// ========================================================================

std::vector<std::string> LockManager::lockedTables() const {
    std::vector<std::string> result;
    for (const auto& kv : locks_) {
        if (kv.second.exclusive || kv.second.sharedCount > 0) {
            result.push_back(kv.first);
        }
    }
    return result;
}

// ========================================================================
// Row-level locking
// ========================================================================

static std::string makeRowKey(const std::string& table, int64_t rid) {
    return table + ":" + std::to_string(rid);
}

bool LockManager::rowLockShared(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makeRowKey(table, rid);
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        auto& state = rowLocks_[key];
        // Already holding shared or exclusive lock on this row
        if (std::find(state.holders.begin(), state.holders.end(), self) != state.holders.end()) {
            state.sharedCount++;
            return true;
        }
        if (!state.exclusive && state.holders.empty()) {
            state.mtx.lock_shared();
            state.sharedCount++;
            state.holders.push_back(self);
            return true;
        }
        if (state.exclusive && !state.holders.empty()) {
            addWaitEdge(self, state.holders[0]);
            if (hasCycle(self)) {
                removeWaitEdges(self);
                return false;
            }
        }
    }
    auto& state = rowLocks_[key];
    state.mtx.lock_shared();
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        state.sharedCount++;
        state.holders.push_back(self);
        removeWaitEdges(self);
    }
    return true;
}

bool LockManager::rowLockExclusive(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makeRowKey(table, rid);
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        auto& state = rowLocks_[key];
        // Already holding exclusive lock on this row
        if (state.exclusive && state.holders.size() == 1 && state.holders[0] == self) {
            return true;
        }
        if (state.sharedCount == 0 && !state.exclusive) {
            state.mtx.lock();
            state.exclusive = true;
            state.holders.push_back(self);
            return true;
        }
        for (const auto& holder : state.holders) {
            if (holder != self) addWaitEdge(self, holder);
        }
        if (hasCycle(self)) {
            removeWaitEdges(self);
            return false;
        }
    }
    auto& state = rowLocks_[key];
    state.mtx.lock();
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        state.exclusive = true;
        state.holders.push_back(self);
        removeWaitEdges(self);
    }
    return true;
}

bool LockManager::rowLockSharedNoWait(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makeRowKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    auto& state = rowLocks_[key];
    if (std::find(state.holders.begin(), state.holders.end(), self) != state.holders.end()) {
        state.sharedCount++;
        return true;
    }
    if (!state.exclusive && state.holders.empty()) {
        state.mtx.lock_shared();
        state.sharedCount++;
        state.holders.push_back(self);
        return true;
    }
    return false;
}

bool LockManager::rowLockExclusiveNoWait(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makeRowKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    auto& state = rowLocks_[key];
    if (state.exclusive && state.holders.size() == 1 && state.holders[0] == self) {
        return true;
    }
    if (state.sharedCount == 0 && !state.exclusive) {
        state.mtx.lock();
        state.exclusive = true;
        state.holders.push_back(self);
        return true;
    }
    return false;
}

void LockManager::rowUnlock(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makeRowKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    removeWaitEdges(self);
    auto it = rowLocks_.find(key);
    if (it == rowLocks_.end()) return;
    auto& state = it->second;
    auto hit = std::find(state.holders.begin(), state.holders.end(), self);
    if (hit != state.holders.end()) state.holders.erase(hit);
    if (state.exclusive) {
        state.mtx.unlock();
        state.exclusive = false;
    } else if (state.sharedCount > 0) {
        state.mtx.unlock_shared();
        state.sharedCount--;
    }
    // Clean up empty lock state
    if (state.sharedCount == 0 && !state.exclusive && state.holders.empty()) {
        rowLocks_.erase(it);
    }
}

void LockManager::rowUnlockAll(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(rowMutex_);
    removeWaitEdges(self);
    std::string prefix = table + ":";
    for (auto it = rowLocks_.begin(); it != rowLocks_.end(); ) {
        if (it->first.find(prefix) != 0) { ++it; continue; }
        auto& state = it->second;
        auto hit = std::find(state.holders.begin(), state.holders.end(), self);
        if (hit != state.holders.end()) state.holders.erase(hit);
        if (state.exclusive) {
            state.mtx.unlock();
            state.exclusive = false;
        }
        while (state.sharedCount > 0) {
            state.mtx.unlock_shared();
            state.sharedCount--;
        }
        if (state.sharedCount == 0 && !state.exclusive && state.holders.empty()) {
            it = rowLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

std::vector<int64_t> LockManager::lockedRows(const std::string& table) const {
    std::vector<int64_t> result;
    std::string prefix = table + ":";
    std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(rowMutex_));
    for (const auto& kv : rowLocks_) {
        if (kv.first.find(prefix) != 0) continue;
        if (kv.second.exclusive || kv.second.sharedCount > 0) {
            // Extract rid from key
            size_t pos = kv.first.find(':');
            if (pos != std::string::npos) {
                try { result.push_back(std::stoll(kv.first.substr(pos + 1))); } catch (...) {}
            }
        }
    }
    return result;
}

// ========================================================================
// Gap locking
// ========================================================================

bool LockManager::lockGap(const std::string& table, const std::string& leftKey, const std::string& rightKey) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(gapMutex_);
    // Simplified: always succeed (no deadlock detection for gap locks)
    gapLocks_[table].push_back({leftKey, rightKey, self});
    return true;
}

bool LockManager::isGapLocked(const std::string& table, const std::string& key) const {
    std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(gapMutex_));
    auto it = gapLocks_.find(table);
    if (it == gapLocks_.end()) return false;
    std::thread::id self = std::this_thread::get_id();
    for (const auto& gl : it->second) {
        if (gl.holder == self) continue; // skip own gap locks
        if (key > gl.leftKey && key < gl.rightKey) return true;
    }
    return false;
}

void LockManager::unlockGaps(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(gapMutex_);
    auto it = gapLocks_.find(table);
    if (it == gapLocks_.end()) return;
    auto& vec = it->second;
    vec.erase(std::remove_if(vec.begin(), vec.end(),
        [&self](const GapLock& gl) { return gl.holder == self; }), vec.end());
    if (vec.empty()) gapLocks_.erase(it);
}

void LockManager::unlockAllGaps() {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(gapMutex_);
    for (auto it = gapLocks_.begin(); it != gapLocks_.end(); ) {
        auto& vec = it->second;
        vec.erase(std::remove_if(vec.begin(), vec.end(),
            [&self](const GapLock& gl) { return gl.holder == self; }), vec.end());
        if (vec.empty()) {
            it = gapLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

// ========================================================================
// Monitoring
// ========================================================================

void LockManager::logDeadlock(const std::string& description) {
    std::lock_guard<std::mutex> guard(deadlockMutex_);
    deadlockLog_.push_back({nowIso8601(), description});
    if (deadlockLog_.size() > 100) deadlockLog_.erase(deadlockLog_.begin());
}

std::vector<LockManager::DeadlockEntry> LockManager::getDeadlockLog() const {
    std::lock_guard<std::mutex> guard(deadlockMutex_);
    return deadlockLog_;
}

void LockManager::clearDeadlockLog() {
    std::lock_guard<std::mutex> guard(deadlockMutex_);
    deadlockLog_.clear();
}

std::vector<LockManager::LockWaitInfo> LockManager::getLockWaits() const {
    std::vector<LockWaitInfo> result;
    std::lock_guard<std::mutex> guard(waitMutex_);
    for (const auto& kv : waitFor_) {
        LockWaitInfo info;
        info.waiterTid = tidToString(kv.first);
        for (const auto& h : kv.second) {
            info.holderTids.push_back(tidToString(h));
        }
        result.push_back(info);
    }
    return result;
}

std::vector<LockManager::LockHoldInfo> LockManager::getLockHolds() const {
    std::vector<LockHoldInfo> result;
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        for (const auto& kv : locks_) {
            const auto& state = kv.second;
            std::string mode = state.exclusive ? "exclusive" : "shared";
            for (const auto& holder : state.holders) {
                result.push_back({kv.first, tidToString(holder), mode});
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (const auto& kv : rowLocks_) {
            const auto& state = kv.second;
            std::string mode = state.exclusive ? "exclusive" : "shared";
            for (const auto& holder : state.holders) {
                result.push_back({kv.first, tidToString(holder), mode});
            }
        }
    }
    return result;
}

} // namespace dbms
