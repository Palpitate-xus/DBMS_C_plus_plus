#include "AdvisoryLockManager.h"

#include <algorithm>
#include <iterator>
#include <tuple>

namespace dbms {

bool AdvisoryLockKey::operator<(const AdvisoryLockKey& other) const {
    return std::tie(database, keySpace, first, second) <
        std::tie(other.database, other.keySpace, other.first, other.second);
}

bool AdvisoryLockManager::compatibleLocked(
    const AdvisoryLockKey& key, uint64_t owner,
    AdvisoryLockMode mode) const {
    const auto found = holders_.find(key);
    if (found == holders_.end()) return true;
    for (const auto& holder : found->second) {
        if (holder.owner == owner) continue;
        if (mode == AdvisoryLockMode::Exclusive ||
            holder.mode == AdvisoryLockMode::Exclusive) {
            return false;
        }
    }
    return true;
}

bool AdvisoryLockManager::acquire(
    const AdvisoryLockKey& key, uint64_t owner,
    AdvisoryLockScope scope, AdvisoryLockMode mode, bool wait) {
    if (owner == 0 || scope == AdvisoryLockScope::Prepared) return false;
    std::unique_lock<std::mutex> lock(mutex_);
    if (!wait && !compatibleLocked(key, owner, mode)) return false;
    if (wait) {
        changed_.wait(lock, [&] { return compatibleLocked(key, owner, mode); });
    }
    uint64_t sequence = 0;
    if (scope == AdvisoryLockScope::Transaction) {
        auto& transaction = transactions_[owner];
        sequence = transaction.nextSequence++;
    }
    holders_[key].push_back(Holder{owner, scope, mode, sequence, {}});
    return true;
}

bool AdvisoryLockManager::unlockSession(
    const AdvisoryLockKey& key, uint64_t owner, AdvisoryLockMode mode) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = holders_.find(key);
    if (found == holders_.end()) return false;
    auto& holders = found->second;
    const auto holder = std::find_if(
        holders.rbegin(), holders.rend(), [&](const Holder& candidate) {
            return candidate.owner == owner &&
                candidate.scope == AdvisoryLockScope::Session &&
                candidate.mode == mode;
        });
    if (holder == holders.rend()) return false;
    holders.erase(std::next(holder).base());
    if (holders.empty()) holders_.erase(found);
    changed_.notify_all();
    return true;
}

size_t AdvisoryLockManager::removeLocked(
    uint64_t owner, bool session, bool transaction) {
    size_t removed = 0;
    for (auto& [key, holders] : holders_) {
        (void)key;
        const auto before = holders.size();
        holders.erase(std::remove_if(
            holders.begin(), holders.end(), [&](const Holder& holder) {
                if (holder.owner != owner) return false;
                return (session && holder.scope == AdvisoryLockScope::Session) ||
                    (transaction &&
                     holder.scope == AdvisoryLockScope::Transaction);
            }), holders.end());
        removed += before - holders.size();
    }
    return removed;
}

void AdvisoryLockManager::pruneEmptyLocked() {
    for (auto iterator = holders_.begin(); iterator != holders_.end();) {
        if (iterator->second.empty()) iterator = holders_.erase(iterator);
        else ++iterator;
    }
}

size_t AdvisoryLockManager::releaseSession(uint64_t owner) {
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t removed = removeLocked(owner, true, false);
    pruneEmptyLocked();
    if (removed != 0) changed_.notify_all();
    return removed;
}

size_t AdvisoryLockManager::releaseTransaction(uint64_t owner) {
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t removed = removeLocked(owner, false, true);
    transactions_.erase(owner);
    pruneEmptyLocked();
    if (removed != 0) changed_.notify_all();
    return removed;
}

size_t AdvisoryLockManager::releaseAll(uint64_t owner) {
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t removed = removeLocked(owner, true, true);
    transactions_.erase(owner);
    pruneEmptyLocked();
    if (removed != 0) changed_.notify_all();
    return removed;
}

void AdvisoryLockManager::beginTransaction(uint64_t owner) {
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t removed = removeLocked(owner, false, true);
    transactions_[owner] = TransactionState{};
    pruneEmptyLocked();
    if (removed != 0) changed_.notify_all();
}

void AdvisoryLockManager::savepoint(
    uint64_t owner, const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& transaction = transactions_[owner];
    transaction.savepoints.push_back(
        Savepoint{name, transaction.nextSequence - 1});
}

void AdvisoryLockManager::releaseSavepoint(
    uint64_t owner, const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = transactions_.find(owner);
    if (found == transactions_.end()) return;
    auto& savepoints = found->second.savepoints;
    const auto savepoint = std::find_if(
        savepoints.rbegin(), savepoints.rend(),
        [&](const Savepoint& item) { return item.name == name; });
    if (savepoint == savepoints.rend()) return;
    savepoints.erase(std::next(savepoint).base(), savepoints.end());
}

void AdvisoryLockManager::rollbackToSavepoint(
    uint64_t owner, const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = transactions_.find(owner);
    if (found == transactions_.end()) return;
    auto& transaction = found->second;
    const auto savepoint = std::find_if(
        transaction.savepoints.rbegin(), transaction.savepoints.rend(),
        [&](const Savepoint& item) { return item.name == name; });
    if (savepoint == transaction.savepoints.rend()) return;
    const uint64_t sequence = savepoint->sequence;
    size_t removed = 0;
    for (auto& [key, holders] : holders_) {
        (void)key;
        const auto before = holders.size();
        holders.erase(std::remove_if(
            holders.begin(), holders.end(), [&](const Holder& holder) {
                return holder.owner == owner &&
                    holder.scope == AdvisoryLockScope::Transaction &&
                    holder.sequence > sequence;
            }), holders.end());
        removed += before - holders.size();
    }
    transaction.savepoints.erase(
        savepoint.base(), transaction.savepoints.end());
    pruneEmptyLocked();
    if (removed != 0) changed_.notify_all();
}

bool AdvisoryLockManager::prepareTransaction(
    uint64_t owner, const std::string& gid) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (preparedOwners_.count(gid) != 0 || nextPreparedOwner_ == 0)
        return false;
    const uint64_t preparedOwner = nextPreparedOwner_--;
    preparedOwners_[gid] = preparedOwner;
    for (auto& [key, holders] : holders_) {
        (void)key;
        for (auto& holder : holders) {
            if (holder.owner != owner ||
                holder.scope != AdvisoryLockScope::Transaction) continue;
            holder.owner = preparedOwner;
            holder.scope = AdvisoryLockScope::Prepared;
            holder.preparedGid = gid;
        }
    }
    transactions_.erase(owner);
    return true;
}

void AdvisoryLockManager::finishPrepared(const std::string& gid) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto owner = preparedOwners_.find(gid);
    if (owner == preparedOwners_.end()) return;
    size_t removed = 0;
    for (auto& [key, holders] : holders_) {
        (void)key;
        const auto before = holders.size();
        holders.erase(std::remove_if(
            holders.begin(), holders.end(), [&](const Holder& holder) {
                return holder.scope == AdvisoryLockScope::Prepared &&
                    holder.preparedGid == gid;
            }), holders.end());
        removed += before - holders.size();
    }
    preparedOwners_.erase(owner);
    pruneEmptyLocked();
    if (removed != 0) changed_.notify_all();
}

AdvisoryLockManager& advisoryLockManager() {
    static AdvisoryLockManager manager;
    return manager;
}

}  // namespace dbms
