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
                    if (recStack.find(neighbor) != recStack.end()) return true;
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

} // namespace dbms
