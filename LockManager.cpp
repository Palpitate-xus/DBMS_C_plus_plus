#include "LockManager.h"

namespace dbms {

bool LockManager::lockShared(const std::string& table) {
    std::lock_guard<std::mutex> guard(globalMutex_);
    auto& state = locks_[table];
    if (state.exclusive) return false;
    state.mtx.lock_shared();
    state.sharedCount++;
    return true;
}

bool LockManager::lockExclusive(const std::string& table) {
    std::lock_guard<std::mutex> guard(globalMutex_);
    auto& state = locks_[table];
    if (state.sharedCount > 0 || state.exclusive) return false;
    state.mtx.lock();
    state.exclusive = true;
    return true;
}

void LockManager::unlock(const std::string& table) {
    std::lock_guard<std::mutex> guard(globalMutex_);
    auto it = locks_.find(table);
    if (it == locks_.end()) return;
    auto& state = it->second;
    if (state.exclusive) {
        state.mtx.unlock();
        state.exclusive = false;
    } else if (state.sharedCount > 0) {
        state.mtx.unlock_shared();
        state.sharedCount--;
    }
}

void LockManager::unlockAll() {
    std::lock_guard<std::mutex> guard(globalMutex_);
    for (auto& kv : locks_) {
        auto& state = kv.second;
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
