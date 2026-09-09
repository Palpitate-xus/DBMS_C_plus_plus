#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
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
    static constexpr size_t kDefaultQueueBytes =
        static_cast<size_t>(1048576) * 8192;

    explicit NotificationManager(size_t maxQueueBytes = kDefaultQueueBytes)
        : maxQueueBytes_(maxQueueBytes) {}

    // This mirrors max_notify_queue_pages' startup-only behavior.  Runtime
    // reconfiguration is accepted only while no committed or pre-commit
    // notification occupies the queue.
    bool configureMaxQueueBytes(size_t maxQueueBytes) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (maxQueueBytes == 0 || pendingBytes_ != 0 || reservedBytes_ != 0) {
            return false;
        }
        maxQueueBytes_ = maxQueueBytes;
        return true;
    }

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
        if (!prepareCommitLocked(transaction->second)) return false;

        removeSubscriptionsLocked(backendId);
        for (const auto& channel : transaction->second.subscriptions) {
            listeners_[channel].insert(backendId);
            subscriptions_[backendId].insert(channel);
        }
        for (const auto& notification : transaction->second.outgoing) {
            publishReservedLocked(notification.key, notification.notification);
        }
        reservedBytes_ -= transaction->second.reservedBytes;
        transactions_.erase(transaction);
        return true;
    }

    // Reserve space before the storage transaction commits.  A caller can
    // then commit durable data knowing that notification publication cannot
    // fail afterwards; rollbackTransaction releases the reservation.
    bool prepareCommitTransaction(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        return transaction != transactions_.end() &&
               prepareCommitLocked(transaction->second);
    }

    void rollbackTransaction(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction == transactions_.end()) return;
        clearPreparationLocked(transaction->second);
        transactions_.erase(transaction);
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
        clearPreparationLocked(state);
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

    void listen(uint64_t backendId, const std::string& database,
                const std::string& channel) {
        std::lock_guard<std::mutex> lock(mutex_);
        const ChannelKey key{database, channel};
        auto transaction = transactions_.find(backendId);
        if (transaction != transactions_.end()) {
            clearPreparationLocked(transaction->second);
            transaction->second.actionExecuted = true;
            transaction->second.subscriptions.insert(key);
            return;
        }
        listeners_[key].insert(backendId);
        subscriptions_[backendId].insert(key);
    }

    void unlisten(uint64_t backendId, const std::string& database,
                  const std::string& channel) {
        std::lock_guard<std::mutex> lock(mutex_);
        const ChannelKey key{database, channel};
        auto transaction = transactions_.find(backendId);
        if (transaction != transactions_.end()) {
            clearPreparationLocked(transaction->second);
            transaction->second.actionExecuted = true;
            transaction->second.subscriptions.erase(key);
            return;
        }
        auto listener = listeners_.find(key);
        if (listener != listeners_.end()) {
            listener->second.erase(backendId);
            if (listener->second.empty()) listeners_.erase(listener);
        }
        auto subscription = subscriptions_.find(backendId);
        if (subscription != subscriptions_.end()) {
            subscription->second.erase(key);
            if (subscription->second.empty()) subscriptions_.erase(subscription);
        }
    }

    void unlistenAll(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto transaction = transactions_.find(backendId);
        if (transaction != transactions_.end()) {
            clearPreparationLocked(transaction->second);
            transaction->second.actionExecuted = true;
            transaction->second.subscriptions.clear();
            return;
        }
        removeSubscriptionsLocked(backendId);
    }

    bool publish(uint64_t senderId, const std::string& database,
                 const std::string& channel, const std::string& payload) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (database.empty() || channel.empty() ||
            database.find('\0') != std::string::npos ||
            channel.find('\0') != std::string::npos ||
            payload.find('\0') != std::string::npos ||
            payload.size() >= 8000) {
            return false;
        }
        const AsyncNotification notification{
            static_cast<uint32_t>(senderId), channel, payload};
        const ChannelKey key{database, channel};
        auto transaction = transactions_.find(senderId);
        if (transaction != transactions_.end()) {
            transaction->second.actionExecuted = true;
            const auto duplicate = std::find_if(
                transaction->second.outgoing.begin(),
                transaction->second.outgoing.end(),
                [&](const PendingNotification& queued) {
                    return queued.key == key &&
                           queued.notification.payload == payload;
                });
            if (duplicate == transaction->second.outgoing.end()) {
                clearPreparationLocked(transaction->second);
                transaction->second.outgoing.push_back(
                    PendingNotification{key, notification});
            }
            return true;
        }
        return publishLocked(key, notification);
    }

    std::vector<AsyncNotification> takePending(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        // PostgreSQL only delivers asynchronous notifications between
        // transactions.  Keeping the entry queued while the receiving
        // backend has an open transaction also prevents an idle network
        // poll from exposing a notification too early.
        if (transactions_.find(backendId) != transactions_.end()) return {};
        auto pending = pending_.find(backendId);
        if (pending == pending_.end()) return {};
        std::vector<AsyncNotification> result;
        result.reserve(pending->second.size());
        for (const auto& notification : pending->second) {
            result.push_back(notification->notification);
            releaseQueuedLocked(notification);
        }
        pending_.erase(pending);
        return result;
    }

    void disconnect(uint64_t backendId) {
        std::lock_guard<std::mutex> lock(mutex_);
        removeSubscriptionsLocked(backendId);
        auto pending = pending_.find(backendId);
        if (pending != pending_.end()) {
            for (const auto& notification : pending->second) {
                releaseQueuedLocked(notification);
            }
            pending_.erase(pending);
        }
        auto transaction = transactions_.find(backendId);
        if (transaction != transactions_.end()) {
            clearPreparationLocked(transaction->second);
            transactions_.erase(transaction);
        }
    }

    std::set<std::string> subscriptions(uint64_t backendId,
                                        const std::string& database) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto subscription = subscriptions_.find(backendId);
        std::set<std::string> result;
        if (subscription == subscriptions_.end()) return result;
        for (const auto& key : subscription->second) {
            if (key.first == database) result.insert(key.second);
        }
        return result;
    }

    double queueUsage() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (maxQueueBytes_ == 0 || pendingBytes_ == 0) return 0.0;
        return std::min(1.0, static_cast<double>(pendingBytes_) /
                                static_cast<double>(maxQueueBytes_));
    }

    size_t queueCapacityBytes() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return maxQueueBytes_;
    }

private:
    using ChannelKey = std::pair<std::string, std::string>;

    struct PendingNotification {
        ChannelKey key;
        AsyncNotification notification;
    };

    struct SavepointState {
        std::string name;
        std::set<ChannelKey> subscriptions;
        size_t outgoingSize = 0;
        bool actionExecuted = false;
    };

    struct TransactionState {
        std::set<ChannelKey> subscriptions;
        std::vector<PendingNotification> outgoing;
        bool actionExecuted = false;
        std::vector<SavepointState> savepoints;
        size_t reservedBytes = 0;
        bool commitPrepared = false;
    };

    struct QueuedNotification {
        AsyncNotification notification;
        size_t accountedBytes = 0;
        size_t remainingRecipients = 0;
    };

    static size_t notificationStorageBytes(
            const AsyncNotification& notification) {
        // Model the central queue entry (length/database/xid header, two
        // NUL-terminated strings, MAXALIGN), rather than multiplying the
        // payload by the number of listeners.
        const size_t raw = 12 + notification.channel.size() + 1 +
                           notification.payload.size() + 1;
        return (raw + 7) & ~static_cast<size_t>(7);
    }

    bool hasCapacityLocked(size_t bytes) const {
        if (bytes > maxQueueBytes_) return false;
        if (pendingBytes_ > maxQueueBytes_ - bytes) return false;
        return reservedBytes_ <= maxQueueBytes_ - bytes - pendingBytes_;
    }

    bool prepareCommitLocked(TransactionState& transaction) {
        if (transaction.commitPrepared) return true;
        size_t needed = 0;
        for (const auto& outgoing : transaction.outgoing) {
            const size_t entryBytes =
                notificationStorageBytes(outgoing.notification);
            if (needed > maxQueueBytes_ ||
                entryBytes > maxQueueBytes_ - needed) {
                return false;
            }
            needed += entryBytes;
        }
        if (!hasCapacityLocked(needed)) return false;
        reservedBytes_ += needed;
        transaction.reservedBytes = needed;
        transaction.commitPrepared = true;
        return true;
    }

    void clearPreparationLocked(TransactionState& transaction) {
        if (!transaction.commitPrepared) return;
        reservedBytes_ -= transaction.reservedBytes;
        transaction.reservedBytes = 0;
        transaction.commitPrepared = false;
    }

    void enqueueLocked(const ChannelKey& key,
                       const AsyncNotification& notification) {
        const auto listener = listeners_.find(key);
        if (listener == listeners_.end()) return;
        auto queued = std::make_shared<QueuedNotification>();
        queued->notification = notification;
        queued->accountedBytes = notificationStorageBytes(notification);
        queued->remainingRecipients = listener->second.size();
        pendingBytes_ += queued->accountedBytes;
        for (uint64_t backendId : listener->second) {
            pending_[backendId].push_back(queued);
        }
    }

    bool publishLocked(const ChannelKey& key,
                       const AsyncNotification& notification) {
        if (listeners_.find(key) == listeners_.end()) return true;
        if (!hasCapacityLocked(notificationStorageBytes(notification))) {
            return false;
        }
        enqueueLocked(key, notification);
        return true;
    }

    void publishReservedLocked(const ChannelKey& key,
                               const AsyncNotification& notification) {
        enqueueLocked(key, notification);
    }

    void releaseQueuedLocked(
            const std::shared_ptr<QueuedNotification>& notification) {
        if (notification->remainingRecipients == 0) return;
        --notification->remainingRecipients;
        if (notification->remainingRecipients == 0) {
            pendingBytes_ -= notification->accountedBytes;
        }
    }

    void removeSubscriptionsLocked(uint64_t backendId) {
        auto subscription = subscriptions_.find(backendId);
        if (subscription == subscriptions_.end()) return;
        for (const auto& key : subscription->second) {
            auto listener = listeners_.find(key);
            if (listener == listeners_.end()) continue;
            listener->second.erase(backendId);
            if (listener->second.empty()) listeners_.erase(listener);
        }
        subscriptions_.erase(subscription);
    }

    mutable std::mutex mutex_;
    std::map<ChannelKey, std::set<uint64_t>> listeners_;
    std::map<uint64_t, std::set<ChannelKey>> subscriptions_;
    std::map<uint64_t,
             std::vector<std::shared_ptr<QueuedNotification>>> pending_;
    std::map<uint64_t, TransactionState> transactions_;
    size_t maxQueueBytes_ = kDefaultQueueBytes;
    size_t pendingBytes_ = 0;
    size_t reservedBytes_ = 0;
};

inline NotificationManager& notificationManager() {
    static NotificationManager manager;
    return manager;
}

}  // namespace dbms
