#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace dbms {

struct AsyncNotification {
    uint32_t senderPid = 0;
    std::string channel;
    std::string payload;
};

// Process-local LISTEN/NOTIFY registry.  A PostgreSQL role may own multiple
// concurrent backends, so subscriptions and queues are keyed by backend PID,
// never by role name.
class NotificationManager {
public:
    void beginTransaction(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (transactions_.find(backendId) != transactions_.end()) return;
        TransactionState state;
        const auto subscription = subscriptions_.find(backendId);
        if (subscription != subscriptions_.end()) {
            state.subscriptions = subscription->second;
        }
        transactions_.emplace(backendId, std::move(state));
    }

    bool commitTransaction(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction == transactions_.end()) return false;

        removeSubscriptionsLocked(backendId);
        for (const auto& channel : transaction->second.subscriptions) {
            listeners_[channel].insert(backendId);
            subscriptions_[backendId].insert(channel);
        }
        for (const auto& notification : transaction->second.outgoing) {
            publishLocked(notification);
        }
        transactions_.erase(transaction);
        return true;
    }

    void rollbackTransaction(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        transactions_.erase(backendId);
    }

    bool hasTransactionalActions(uint64_t backendId) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto transaction = transactions_.find(backendId);
        return transaction != transactions_.end() &&
               transaction->second.actionExecuted;
    }

    bool inTransaction(uint64_t backendId) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return transactions_.find(backendId) != transactions_.end();
    }

    bool savepoint(uint64_t backendId, const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction == transactions_.end()) return false;
        transaction->second.savepoints.push_back(SavepointState{
            name, transaction->second.subscriptions,
            transaction->second.outgoing.size(),
            transaction->second.actionExecuted});
        return true;
    }

    bool rollbackToSavepoint(uint64_t backendId, const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction == transactions_.end()) return false;
        auto& state = transaction->second;
        const auto reverse = std::find_if(
            state.savepoints.rbegin(), state.savepoints.rend(),
            [&](const SavepointState& savepoint) {
                return savepoint.name == name;
            });
        if (reverse == state.savepoints.rend()) return false;
        const size_t index = static_cast<size_t>(std::distance(
            state.savepoints.begin(), reverse.base()) - 1);
        state.subscriptions = reverse->subscriptions;
        state.outgoing.resize(reverse->outgoingSize);
        state.actionExecuted = reverse->actionExecuted;
        state.savepoints.erase(
            state.savepoints.begin() + static_cast<std::ptrdiff_t>(index + 1),
            state.savepoints.end());
        return true;
    }

    bool releaseSavepoint(uint64_t backendId, const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction == transactions_.end()) return false;
        auto& savepoints = transaction->second.savepoints;
        const auto reverse = std::find_if(
            savepoints.rbegin(), savepoints.rend(),
            [&](const SavepointState& savepoint) {
                return savepoint.name == name;
            });
        if (reverse == savepoints.rend()) return false;
        const size_t index = static_cast<size_t>(std::distance(
            savepoints.begin(), reverse.base()) - 1);
        savepoints.erase(
            savepoints.begin() + static_cast<std::ptrdiff_t>(index),
            savepoints.end());
        return true;
    }

    void listen(uint64_t backendId, const std::string& channel) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction != transactions_.end()) {
            transaction->second.actionExecuted = true;
            transaction->second.subscriptions.insert(channel);
            return;
        }
        listeners_[channel].insert(backendId);
        subscriptions_[backendId].insert(channel);
    }

    void unlisten(uint64_t backendId, const std::string& channel) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction != transactions_.end()) {
            transaction->second.actionExecuted = true;
            transaction->second.subscriptions.erase(channel);
            return;
        }
        auto listener = listeners_.find(channel);
        if (listener != listeners_.end()) {
            listener->second.erase(backendId);
            if (listener->second.empty()) listeners_.erase(listener);
        }
        auto subscription = subscriptions_.find(backendId);
        if (subscription != subscriptions_.end()) {
            subscription->second.erase(channel);
            if (subscription->second.empty()) subscriptions_.erase(subscription);
        }
    }

    void unlistenAll(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction != transactions_.end()) {
            transaction->second.actionExecuted = true;
            transaction->second.subscriptions.clear();
            return;
        }
        removeSubscriptionsLocked(backendId);
    }

    void publish(uint64_t senderId, const std::string& channel,
                 const std::string& payload) {
        std::lock_guard<std::mutex> lock(mutex_);
        const AsyncNotification notification{
            static_cast<uint32_t>(senderId), channel, payload};
        auto transaction = transactions_.find(senderId);
        if (transaction != transactions_.end()) {
            transaction->second.actionExecuted = true;
            const auto duplicate = std::find_if(
                transaction->second.outgoing.begin(),
                transaction->second.outgoing.end(),
                [&](const AsyncNotification& queued) {
                    return queued.channel == channel &&
                           queued.payload == payload;
                });
            if (duplicate == transaction->second.outgoing.end()) {
                transaction->second.outgoing.push_back(notification);
            }
            return;
        }
        publishLocked(notification);
    }

    std::vector<AsyncNotification> takePending(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto pending = pending_.find(backendId);
        if (pending == pending_.end()) return {};
        std::vector<AsyncNotification> result = std::move(pending->second);
        pending_.erase(pending);
        return result;
    }

    void disconnect(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        removeSubscriptionsLocked(backendId);
        pending_.erase(backendId);
        transactions_.erase(backendId);
    }

    std::set<std::string> subscriptions(uint64_t backendId) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto subscription = subscriptions_.find(backendId);
        return subscription == subscriptions_.end()
            ? std::set<std::string>{} : subscription->second;
    }

private:
    struct SavepointState {
        std::string name;
        std::set<std::string> subscriptions;
        size_t outgoingSize = 0;
        bool actionExecuted = false;
    };

    struct TransactionState {
        std::set<std::string> subscriptions;
        std::vector<AsyncNotification> outgoing;
        bool actionExecuted = false;
        std::vector<SavepointState> savepoints;
    };

    void publishLocked(const AsyncNotification& notification) {
        const auto listener = listeners_.find(notification.channel);
        if (listener == listeners_.end()) return;
        for (uint64_t backendId : listener->second) {
            pending_[backendId].push_back(notification);
        }
    }

    void removeSubscriptionsLocked(uint64_t backendId) {
        auto subscription = subscriptions_.find(backendId);
        if (subscription == subscriptions_.end()) return;
        for (const auto& channel : subscription->second) {
            auto listener = listeners_.find(channel);
            if (listener == listeners_.end()) continue;
            listener->second.erase(backendId);
            if (listener->second.empty()) listeners_.erase(listener);
        }
        subscriptions_.erase(subscription);
    }

    mutable std::mutex mutex_;
    std::map<std::string, std::set<uint64_t>> listeners_;
    std::map<uint64_t, std::set<std::string>> subscriptions_;
    std::map<uint64_t, std::vector<AsyncNotification>> pending_;
    std::map<uint64_t, TransactionState> transactions_;
};

inline NotificationManager& notificationManager() {
    static NotificationManager manager;
    return manager;
}

}  // namespace dbms
