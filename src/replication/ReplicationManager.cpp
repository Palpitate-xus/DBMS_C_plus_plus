#include "ReplicationManager.h"

#include <algorithm>
#include <filesystem>

namespace dbms {

ReplicationManager& ReplicationManager::instance() {
    static ReplicationManager mgr;
    return mgr;
}

bool ReplicationManager::createReplicationSlot(const std::string& name,
                                               const std::string& type,
                                               const std::string& plugin) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (slots_.count(name)) return false;
    ReplicationSlot slot;
    slot.name = name;
    slot.slotType = type;
    slot.plugin = plugin;
    slot.active = false;
    slots_[name] = std::move(slot);
    return true;
}

bool ReplicationManager::dropReplicationSlot(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(name);
    if (it == slots_.end()) return false;
    if (it->second.active) return false;  // cannot drop active slot
    slots_.erase(it);
    return true;
}

ReplicationManager::ReplicationSlot* ReplicationManager::findSlot(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(name);
    if (it != slots_.end()) return &it->second;
    return nullptr;
}

std::vector<ReplicationManager::ReplicationSlot> ReplicationManager::listSlots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ReplicationSlot> result;
    for (const auto& [name, slot] : slots_) {
        result.push_back(slot);
    }
    return result;
}

bool ReplicationManager::promote() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (standbyMode_ == StandbyMode::None) return false;
    standbyMode_ = StandbyMode::None;  // No longer a standby = promoted
    return true;
}

} // namespace dbms
