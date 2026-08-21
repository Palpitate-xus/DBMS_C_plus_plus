#include "ReplicationManager.h"

#include <algorithm>
#include <cctype>

namespace dbms {

ReplicationManager& ReplicationManager::instance() {
    static ReplicationManager mgr;
    return mgr;
}

bool ReplicationManager::validSlotDefinition(const std::string& name,
                                             const std::string& type,
                                             const std::string& plugin) {
    if (name.empty() || name.size() > 63) return false;
    for (unsigned char c : name) {
        if (!(std::isalnum(c) || c == '_' || c == '-')) return false;
    }
    if (type != "physical" && type != "logical") return false;
    return type == "physical" ? plugin.empty() : !plugin.empty();
}

bool ReplicationManager::createReplicationSlot(const std::string& name,
                                               const std::string& type,
                                               const std::string& plugin) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!validSlotDefinition(name, type, plugin)) return false;
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

std::optional<ReplicationManager::ReplicationSlot>
ReplicationManager::findSlot(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(name);
    if (it == slots_.end()) return std::nullopt;
    return it->second;
}

bool ReplicationManager::activateReplicationSlot(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(name);
    if (it == slots_.end() || it->second.active) return false;
    it->second.active = true;
    return true;
}

bool ReplicationManager::deactivateReplicationSlot(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(name);
    if (it == slots_.end() || !it->second.active) return false;
    it->second.active = false;
    return true;
}

bool ReplicationManager::advanceSlotLsn(const std::string& name, int64_t newRestartLsn) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(name);
    if (it == slots_.end()) return false;
    if (newRestartLsn < it->second.restartLsn) return false;  // never rewind
    it->second.restartLsn = newRestartLsn;
    return true;
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

void ReplicationManager::setStandbyMode(StandbyMode mode) {
    std::lock_guard<std::mutex> lock(mutex_);
    standbyMode_ = mode;
}

ReplicationManager::StandbyMode ReplicationManager::standbyMode() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return standbyMode_;
}

void ReplicationManager::setPrimaryConnInfo(const std::string& conninfo) {
    std::lock_guard<std::mutex> lock(mutex_);
    primaryConnInfo_ = conninfo;
}

std::string ReplicationManager::primaryConnInfo() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return primaryConnInfo_;
}

void ReplicationManager::setSyncReplication(bool on) {
    std::lock_guard<std::mutex> lock(mutex_);
    syncReplication_ = on;
}

bool ReplicationManager::syncReplication() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return syncReplication_;
}

bool ReplicationManager::isActiveStandby() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return standbyMode_ != StandbyMode::None;
}

} // namespace dbms
