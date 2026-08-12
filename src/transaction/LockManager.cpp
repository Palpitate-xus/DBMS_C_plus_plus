#include "LockManager.h"

#include <algorithm>

namespace dbms {

LockManager& LockManager::global() {
    static LockManager manager;
    return manager;
}

LockManager::ThreadSettings& LockManager::threadSettings() const {
    return threadSettings_[instanceId_];
}

void LockManager::setResourceNamespace(const std::string& dbname) const {
    threadSettings().resourceNamespace = dbname;
}

std::string LockManager::resourceKey(const std::string& resource) const {
    const std::string& resourceNamespace = threadSettings().resourceNamespace;
    if (resourceNamespace.empty()) return resource;
    return resourceNamespace + '\x1f' + resource;
}

std::string LockManager::rowResourceKey(const std::string& table, int64_t rid) const {
    return resourceKey(table + ":" + std::to_string(rid));
}

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

void LockManager::setLockTimeout(int ms) {
    threadSettings().lockTimeoutMs = ms > 0 ? ms : 0;
}

void LockManager::setDeadlockTimeout(int ms) {
    threadSettings().deadlockTimeoutMs = ms > 0 ? ms : 0;
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

// ========================================================================
// Lock compatibility
// ========================================================================

static bool isCompatible(LockManager::LockMode requested, LockManager::LockMode held, bool sameThread) {
    if (sameThread) return true; // Same thread can upgrade or re-acquire
    switch (requested) {
        case LockManager::LockMode::Shared:
            return held == LockManager::LockMode::Shared || held == LockManager::LockMode::IntentShared;
        case LockManager::LockMode::Exclusive:
            return false;
        case LockManager::LockMode::IntentShared:
            return held == LockManager::LockMode::Shared ||
                   held == LockManager::LockMode::IntentShared;
        case LockManager::LockMode::IntentExclusive:
            // IntentExclusive uses the underlying exclusive mutex in this
            // manager, so claiming compatibility with IS/IX would allow the
            // wait graph to say "compatible" while shared_mutex blocks.
            return false;
        case LockManager::LockMode::Metadata:
            return false;
    }
    return false;
}

static const char* modeToStr(LockManager::LockMode mode) {
    switch (mode) {
        case LockManager::LockMode::Shared: return "shared";
        case LockManager::LockMode::Exclusive: return "exclusive";
        case LockManager::LockMode::IntentShared: return "IS";
        case LockManager::LockMode::IntentExclusive: return "IX";
        case LockManager::LockMode::Metadata: return "MDL";
    }
    return "";
}

bool LockManager::acquireLock(const std::string& table, LockMode mode) {
    const std::thread::id self = std::this_thread::get_id();

    auto isSharedMode = [](LockMode value) {
        return value == LockMode::Shared || value == LockMode::IntentShared;
    };
    auto isReusable = [&](LockMode held, LockMode requested) {
        if (held == LockMode::Exclusive || held == LockMode::Metadata) return true;
        if (held == LockMode::IntentExclusive) {
            return requested == LockMode::Shared ||
                   requested == LockMode::IntentShared ||
                   requested == LockMode::IntentExclusive;
        }
        return isSharedMode(held) && isSharedMode(requested);
    };

    auto releasePhysical = [&](LockState& state) {
        auto modeIt = state.holderModes.find(self);
        if (modeIt == state.holderModes.end()) return;
        switch (modeIt->second) {
            case LockMode::Shared:
                state.mtx.unlock_shared();
                --state.sharedCount;
                break;
            case LockMode::Exclusive:
                state.mtx.unlock();
                state.exclusive = false;
                break;
            case LockMode::IntentShared:
                state.mtx.unlock_shared();
                --state.intentSharedCount;
                break;
            case LockMode::IntentExclusive:
                state.mtx.unlock();
                --state.intentExclusiveCount;
                break;
            case LockMode::Metadata:
                state.mtx.unlock();
                state.metadata = false;
                break;
        }
        state.holders.erase(std::remove(state.holders.begin(), state.holders.end(), self),
                            state.holders.end());
        state.holderModes.erase(modeIt);
        state.holderCounts.erase(self);
    };

    auto acquirePhysical = [&](LockState& state) {
        switch (mode) {
            case LockMode::Shared:
                state.mtx.lock_shared();
                ++state.sharedCount;
                break;
            case LockMode::Exclusive:
                state.mtx.lock();
                state.exclusive = true;
                break;
            case LockMode::IntentShared:
                state.mtx.lock_shared();
                ++state.intentSharedCount;
                break;
            case LockMode::IntentExclusive:
                state.mtx.lock();
                ++state.intentExclusiveCount;
                break;
            case LockMode::Metadata:
                state.mtx.lock();
                state.metadata = true;
                break;
        }
        state.holders.push_back(self);
        state.holderModes[self] = mode;
        state.holderCounts[self] = 1;
        removeWaitEdges(self);
    };

    auto inspect = [&](LockState& state) {
        // Replace, rather than accumulate, this waiter's outgoing edges. This
        // matters when a lock is released and a different transaction takes
        // its place before the next polling pass.
        removeWaitEdges(self);
        bool compatible = true;
        for (const auto& holder : state.holders) {
            if (holder == self) continue;
            const auto it = state.holderModes.find(holder);
            const LockMode held = it == state.holderModes.end()
                ? LockMode::Shared : it->second;
            if (!isCompatible(mode, held, false)) {
                compatible = false;
                addWaitEdge(self, holder);
            }
        }
        return compatible;
    };

    const auto waitStart = std::chrono::steady_clock::now();
    const auto deadlockCheckAt = waitStart +
        std::chrono::milliseconds(std::max(0, threadSettings().deadlockTimeoutMs));
    while (true) {
        bool cycle = false;
        {
            std::lock_guard<std::mutex> guard(globalMutex_);
            auto& state = locks_[resourceKey(table)];

            auto selfMode = state.holderModes.find(self);
            if (selfMode != state.holderModes.end()) {
                if (isReusable(selfMode->second, mode)) {
                    ++state.holderCounts[self];
                    return true;
                }
                // Upgrade from a shared/intent lock. Drop this thread's
                // physical token before waiting for the stronger mode so it
                // cannot wait on itself.
                releasePhysical(state);
            }

            if (inspect(state)) {
                acquirePhysical(state);
                return true;
            }
            cycle = std::chrono::steady_clock::now() >= deadlockCheckAt &&
                    hasCycle(self);
        }
        if (cycle) {
            removeWaitEdges(self);
            return false;
        }

        if (threadSettings().lockTimeoutMs > 0) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - waitStart).count();
            if (elapsed >= threadSettings().lockTimeoutMs) {
                removeWaitEdges(self);
                return false;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

bool LockManager::lockShared(const std::string& table) {
    return acquireLock(table, LockMode::Shared);
}

bool LockManager::lockExclusive(const std::string& table) {
    return acquireLock(table, LockMode::Exclusive);
}

bool LockManager::lockIntentShared(const std::string& table) {
    return acquireLock(table, LockMode::IntentShared);
}

bool LockManager::lockIntentExclusive(const std::string& table) {
    return acquireLock(table, LockMode::IntentExclusive);
}

bool LockManager::lockMetadata(const std::string& table) {
    return acquireLock(table, LockMode::Metadata);
}

// ========================================================================
// Unlock
// ========================================================================

void LockManager::unlock(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(globalMutex_);
    removeWaitEdges(self);
    auto it = locks_.find(resourceKey(table));
    if (it == locks_.end()) return;
    auto& state = it->second;
    auto countIt = state.holderCounts.find(self);
    auto modeIt = state.holderModes.find(self);
    if (countIt == state.holderCounts.end() || modeIt == state.holderModes.end()) return;
    if (countIt->second > 1) {
        --countIt->second;
        return;
    }
    const LockMode mode = modeIt->second;
    switch (mode) {
        case LockMode::Shared:
            state.mtx.unlock_shared();
            --state.sharedCount;
            break;
        case LockMode::Exclusive:
            state.mtx.unlock();
            state.exclusive = false;
            break;
        case LockMode::IntentShared:
            state.mtx.unlock_shared();
            state.intentSharedCount--;
            break;
        case LockMode::IntentExclusive:
            state.mtx.unlock();
            state.intentExclusiveCount--;
            break;
        case LockMode::Metadata:
            state.mtx.unlock();
            state.metadata = false;
            break;
    }
    state.holders.erase(std::remove(state.holders.begin(), state.holders.end(), self),
                        state.holders.end());
    state.holderModes.erase(modeIt);
    state.holderCounts.erase(countIt);
}

void LockManager::unlockAll() {
    std::thread::id self = std::this_thread::get_id();
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        removeWaitEdges(self);
        for (auto& kv : locks_) {
            auto& state = kv.second;
            auto modeIt = state.holderModes.find(self);
            if (modeIt != state.holderModes.end()) {
                switch (modeIt->second) {
                    case LockMode::Shared:
                        state.mtx.unlock_shared();
                        state.sharedCount--;
                        break;
                    case LockMode::Exclusive:
                        state.mtx.unlock();
                        state.exclusive = false;
                        break;
                    case LockMode::IntentShared:
                        state.mtx.unlock_shared();
                        state.intentSharedCount--;
                        break;
                    case LockMode::IntentExclusive:
                        state.mtx.unlock();
                        state.intentExclusiveCount--;
                        break;
                    case LockMode::Metadata:
                        state.mtx.unlock();
                        state.metadata = false;
                        break;
                }
                state.holderModes.erase(modeIt);
                state.holderCounts.erase(self);
                state.holders.erase(std::remove(state.holders.begin(), state.holders.end(), self),
                                    state.holders.end());
            }
        }
    }
    // Also release all row locks held by this thread
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (auto it = rowLocks_.begin(); it != rowLocks_.end(); ) {
            auto& state = it->second;
            auto hit = std::find(state.holders.begin(), state.holders.end(), self);
            if (hit == state.holders.end()) {
                ++it;
                continue;
            }
            state.holders.erase(hit);
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            } else {
                state.mtx.unlock_shared();
                --state.sharedCount;
            }
            if (state.sharedCount == 0 && !state.exclusive &&
                state.holders.empty() && state.waiters == 0) {
                it = rowLocks_.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Also release all page locks held by this thread
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        for (auto it = pageLocks_.begin(); it != pageLocks_.end(); ) {
            auto& state = it->second;
            auto hit = std::find(state.holders.begin(), state.holders.end(), self);
            if (hit == state.holders.end()) {
                ++it;
                continue;
            }
            state.holders.erase(hit);
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            } else {
                state.mtx.unlock_shared();
                --state.sharedCount;
            }
            if (state.sharedCount == 0 && !state.exclusive &&
                state.holders.empty() && state.waiters == 0) {
                it = pageLocks_.erase(it);
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
    std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(globalMutex_));
    for (const auto& kv : locks_) {
        const auto& state = kv.second;
        if (state.exclusive || state.sharedCount > 0 ||
            state.intentSharedCount > 0 || state.intentExclusiveCount > 0 ||
            state.metadata) {
            result.push_back(kv.first);
        }
    }
    return result;
}

// ========================================================================
// Row-level locking
// ========================================================================

bool LockManager::rowLockShared(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = rowResourceKey(table, rid);
    LockState* state = nullptr;
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        auto& current = rowLocks_[key];
        // Already holding shared or exclusive lock on this row
        if (std::find(current.holders.begin(), current.holders.end(), self) !=
            current.holders.end()) {
            return true;
        }
        if (!current.exclusive) {
            current.mtx.lock_shared();
            ++current.sharedCount;
            current.holders.push_back(self);
            return true;
        }
        for (const auto& holder : current.holders) {
            if (holder != self) addWaitEdge(self, holder);
        }
        if (hasCycle(self)) {
            removeWaitEdges(self);
            return false;
        }
        ++current.waiters;
        state = &current;
    }
    state->mtx.lock_shared();
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        --state->waiters;
        ++state->sharedCount;
        state->holders.push_back(self);
        removeWaitEdges(self);
    }
    return true;
}

bool LockManager::rowLockExclusive(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = rowResourceKey(table, rid);
    LockState* state = nullptr;
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        auto& current = rowLocks_[key];
        // Already holding exclusive lock on this row
        if (current.exclusive && current.holders.size() == 1 && current.holders[0] == self) {
            return true;
        }
        // Upgrade our own shared row lock before acquiring the exclusive
        // mutex.  Calling lock() while this thread still owns a shared lock
        // self-deadlocks even when no other transaction is present.
        if (!current.exclusive && current.sharedCount > 0 &&
            current.holders.size() == 1 && current.holders[0] == self) {
            current.mtx.unlock_shared();
            current.sharedCount = 0;
            current.holders.clear();
            current.mtx.lock();
            current.exclusive = true;
            current.holders.push_back(self);
            removeWaitEdges(self);
            return true;
        }
        // With other readers present, an in-place upgrade would wait on a
        // mutex still held by this thread.  Drop our shared token before
        // entering the normal wait path; the statement/transaction will
        // retain its snapshot and either acquire the exclusive lock or be
        // rejected by deadlock detection.
        auto selfHolder = std::find(current.holders.begin(), current.holders.end(), self);
        if (!current.exclusive && selfHolder != current.holders.end()) {
            current.mtx.unlock_shared();
            --current.sharedCount;
            current.holders.erase(selfHolder);
        }
        if (current.sharedCount == 0 && !current.exclusive) {
            current.mtx.lock();
            current.exclusive = true;
            current.holders.push_back(self);
            return true;
        }
        for (const auto& holder : current.holders) {
            if (holder != self) addWaitEdge(self, holder);
        }
        if (hasCycle(self)) {
            removeWaitEdges(self);
            return false;
        }
        ++current.waiters;
        state = &current;
    }
    state->mtx.lock();
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        --state->waiters;
        state->exclusive = true;
        state->holders.push_back(self);
        removeWaitEdges(self);
    }
    return true;
}

bool LockManager::rowLockSharedNoWait(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = rowResourceKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    auto& state = rowLocks_[key];
    if (std::find(state.holders.begin(), state.holders.end(), self) != state.holders.end()) {
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
    std::string key = rowResourceKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    auto& state = rowLocks_[key];
    if (state.exclusive && state.holders.size() == 1 && state.holders[0] == self) {
        return true;
    }
    if (!state.exclusive && state.sharedCount > 0 &&
        state.holders.size() == 1 && state.holders[0] == self) {
        state.mtx.unlock_shared();
        state.sharedCount = 0;
        state.holders.clear();
        state.mtx.lock();
        state.exclusive = true;
        state.holders.push_back(self);
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
    std::string key = rowResourceKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    removeWaitEdges(self);
    auto it = rowLocks_.find(key);
    if (it == rowLocks_.end()) return;
    auto& state = it->second;
    auto hit = std::find(state.holders.begin(), state.holders.end(), self);
    if (hit == state.holders.end()) return;
    state.holders.erase(hit);
    if (state.exclusive) {
        state.mtx.unlock();
        state.exclusive = false;
    } else {
        state.mtx.unlock_shared();
        --state.sharedCount;
    }
    // Clean up empty lock state
    if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
        state.waiters == 0) {
        rowLocks_.erase(it);
    }
}

void LockManager::rowUnlockAll(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(rowMutex_);
    removeWaitEdges(self);
    std::string prefix = resourceKey(table + ":");
    for (auto it = rowLocks_.begin(); it != rowLocks_.end(); ) {
        if (it->first.find(prefix) != 0) { ++it; continue; }
        auto& state = it->second;
        auto hit = std::find(state.holders.begin(), state.holders.end(), self);
        if (hit == state.holders.end()) { ++it; continue; }
        state.holders.erase(hit);
        if (state.exclusive) {
            state.mtx.unlock();
            state.exclusive = false;
        } else {
            state.mtx.unlock_shared();
            --state.sharedCount;
        }
        if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
            state.waiters == 0) {
            it = rowLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

std::vector<int64_t> LockManager::lockedRows(const std::string& table) const {
    std::vector<int64_t> result;
    std::string prefix = resourceKey(table + ":");
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
    gapLocks_[resourceKey(table)].push_back({leftKey, rightKey, self});
    return true;
}

bool LockManager::isGapLocked(const std::string& table, const std::string& key) const {
    std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(gapMutex_));
    auto it = gapLocks_.find(resourceKey(table));
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
    auto it = gapLocks_.find(resourceKey(table));
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
            for (const auto& holder : state.holders) {
                auto modeIt = state.holderModes.find(holder);
                std::string modeStr = modeToStr(
                    (modeIt != state.holderModes.end()) ? modeIt->second : LockMode::Shared);
                result.push_back({kv.first, tidToString(holder), modeStr});
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (const auto& kv : rowLocks_) {
            const auto& state = kv.second;
            std::string modeStr = state.exclusive ? "exclusive" : "shared";
            for (const auto& holder : state.holders) {
                result.push_back({kv.first, tidToString(holder), modeStr});
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        for (const auto& kv : pageLocks_) {
            const auto& state = kv.second;
            std::string modeStr = state.exclusive ? "exclusive" : "shared";
            for (const auto& holder : state.holders) {
                result.push_back({kv.first, tidToString(holder), modeStr});
            }
        }
    }
    return result;
}

// ========================================================================
// Page-level locking
// ========================================================================

static std::string makePageKey(const std::string& dbname, const std::string& table, uint32_t pageId) {
    return "page:" + dbname + ":" + table + ":" + std::to_string(pageId);
}

bool LockManager::pageLockShared(const std::string& dbname, const std::string& table, uint32_t pageId) const {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makePageKey(dbname, table, pageId);
    LockState* state = nullptr;
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        auto& current = const_cast<LockManager*>(this)->pageLocks_[key];
        if (std::find(current.holders.begin(), current.holders.end(), self) !=
            current.holders.end()) return true;
        if (!current.exclusive) {
            current.mtx.lock_shared();
            ++current.sharedCount;
            current.holders.push_back(self);
            return true;
        }
        for (const auto& holder : current.holders) {
            if (holder != self) const_cast<LockManager*>(this)->addWaitEdge(self, holder);
        }
        if (const_cast<LockManager*>(this)->hasCycle(self)) {
            const_cast<LockManager*>(this)->removeWaitEdges(self);
            return false;
        }
        ++current.waiters;
        state = &current;
    }
    state->mtx.lock_shared();
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        --state->waiters;
        ++state->sharedCount;
        state->holders.push_back(self);
        const_cast<LockManager*>(this)->removeWaitEdges(self);
    }
    return true;
}

bool LockManager::pageLockExclusive(const std::string& dbname, const std::string& table, uint32_t pageId) const {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makePageKey(dbname, table, pageId);
    LockState* state = nullptr;
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        auto& current = const_cast<LockManager*>(this)->pageLocks_[key];
        if (current.exclusive && current.holders.size() == 1 && current.holders[0] == self) {
            return true;
        }
        auto selfHolder = std::find(current.holders.begin(), current.holders.end(), self);
        if (!current.exclusive && selfHolder != current.holders.end() &&
            current.sharedCount == 1) {
            current.mtx.unlock_shared();
            current.sharedCount = 0;
            current.holders.clear();
            current.mtx.lock();
            current.exclusive = true;
            current.holders.push_back(self);
            const_cast<LockManager*>(this)->removeWaitEdges(self);
            return true;
        }
        if (!current.exclusive && selfHolder != current.holders.end()) {
            current.mtx.unlock_shared();
            --current.sharedCount;
            current.holders.erase(selfHolder);
        }
        if (current.sharedCount == 0 && !current.exclusive) {
            current.mtx.lock();
            current.exclusive = true;
            current.holders.push_back(self);
            return true;
        }
        for (const auto& holder : current.holders) {
            if (holder != self) const_cast<LockManager*>(this)->addWaitEdge(self, holder);
        }
        if (const_cast<LockManager*>(this)->hasCycle(self)) {
            const_cast<LockManager*>(this)->removeWaitEdges(self);
            return false;
        }
        ++current.waiters;
        state = &current;
    }
    state->mtx.lock();
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        --state->waiters;
        state->exclusive = true;
        state->holders.push_back(self);
        const_cast<LockManager*>(this)->removeWaitEdges(self);
    }
    return true;
}

void LockManager::pageUnlock(const std::string& dbname, const std::string& table, uint32_t pageId) const {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makePageKey(dbname, table, pageId);
    std::lock_guard<std::mutex> guard(pageMutex_);
    const_cast<LockManager*>(this)->removeWaitEdges(self);
    auto it = pageLocks_.find(key);
    if (it == pageLocks_.end()) return;
    auto& state = const_cast<LockState&>(it->second);
    auto hit = std::find(state.holders.begin(), state.holders.end(), self);
    if (hit == state.holders.end()) return;
    state.holders.erase(hit);
    if (state.exclusive) {
        state.mtx.unlock();
        state.exclusive = false;
    } else {
        state.mtx.unlock_shared();
        --state.sharedCount;
    }
    if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
        state.waiters == 0) {
        pageLocks_.erase(it);
    }
}

void LockManager::pageUnlockAll(const std::string& dbname, const std::string& table) const {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(pageMutex_);
    const_cast<LockManager*>(this)->removeWaitEdges(self);
    std::string prefix = "page:" + dbname + ":" + table + ":";
    for (auto it = pageLocks_.begin(); it != pageLocks_.end(); ) {
        if (it->first.find(prefix) != 0) { ++it; continue; }
        auto& state = const_cast<LockState&>(it->second);
        auto hit = std::find(state.holders.begin(), state.holders.end(), self);
        if (hit == state.holders.end()) { ++it; continue; }
        state.holders.erase(hit);
        if (state.exclusive) {
            state.mtx.unlock();
            state.exclusive = false;
        } else {
            state.mtx.unlock_shared();
            --state.sharedCount;
        }
        if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
            state.waiters == 0) {
            it = pageLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

void LockManager::pageUnlockAll() const {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(pageMutex_);
    const_cast<LockManager*>(this)->removeWaitEdges(self);
    for (auto it = pageLocks_.begin(); it != pageLocks_.end(); ) {
        auto& state = const_cast<LockState&>(it->second);
        auto hit = std::find(state.holders.begin(), state.holders.end(), self);
        if (hit == state.holders.end()) { ++it; continue; }
        state.holders.erase(hit);
        if (state.exclusive) {
            state.mtx.unlock();
            state.exclusive = false;
        } else {
            state.mtx.unlock_shared();
            --state.sharedCount;
        }
        if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
            state.waiters == 0) {
            it = pageLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace dbms
