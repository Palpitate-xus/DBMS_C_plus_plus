#pragma once

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
    void listen(uint64_t backendId, const std::string& channel) {
        std::lock_guard<std::mutex> lock(mutex_);
        listeners_[channel].insert(backendId);
        subscriptions_[backendId].insert(channel);
    }

    void unlisten(uint64_t backendId, const std::string& channel) {
        std::lock_guard<std::mutex> lock(mutex_);
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
        removeSubscriptionsLocked(backendId);
    }

    void publish(uint64_t senderId, const std::string& channel,
                 const std::string& payload) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto listener = listeners_.find(channel);
        if (listener == listeners_.end()) return;
        for (uint64_t backendId : listener->second) {
            pending_[backendId].push_back(AsyncNotification{
                static_cast<uint32_t>(senderId), channel, payload});
        }
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
    }

private:
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

    std::mutex mutex_;
    std::map<std::string, std::set<uint64_t>> listeners_;
    std::map<uint64_t, std::set<std::string>> subscriptions_;
    std::map<uint64_t, std::vector<AsyncNotification>> pending_;
};

inline NotificationManager& notificationManager() {
    static NotificationManager manager;
    return manager;
}

}  // namespace dbms
