#include "ReplicationManager.h"
#include "LogicalDecoder.h"
#include "access/IndexFileUtil.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace dbms {

ReplicationManager& ReplicationManager::instance() {
    static ReplicationManager mgr;
    return mgr;
}

bool ReplicationManager::configureSlotStorage(
    const std::string& path, std::string& error) {
    error.clear();
    if (path.empty()) {
        error = "replication slot storage path is required";
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    std::map<std::string, ReplicationSlot> loaded;
    std::error_code filesystemError;
    const bool stateExists = std::filesystem::exists(path, filesystemError);
    if (filesystemError) {
        error = "cannot inspect replication slot state";
        return false;
    }
    if (stateExists) {
        std::ifstream input(path, std::ios::binary);
        if (!input) {
            error = "cannot read replication slot state";
            return false;
        }
        std::string header;
        if (!std::getline(input, header) ||
            (header != "DBMS_REPLICATION_SLOTS_V1" &&
             header != "DBMS_REPLICATION_SLOTS_V2")) {
            error = "invalid replication slot state header";
            return false;
        }
        const bool hasInvalidationState =
            header == "DBMS_REPLICATION_SLOTS_V2";
        std::string line;
        while (std::getline(input, line)) {
            if (line.empty()) continue;
            ReplicationSlot slot;
            std::istringstream row(line);
            if (!(row >> std::quoted(slot.name) >> std::quoted(slot.slotType) >>
                  std::quoted(slot.plugin) >> std::quoted(slot.database) >>
                  slot.restartLsn) ||
                slot.restartLsn < 0 ||
                !validSlotDefinition(
                    slot.name, slot.slotType, slot.plugin, slot.database)) {
                error = "invalid replication slot state entry";
                return false;
            }
            if (hasInvalidationState) {
                int invalidated = 0;
                if (!(row >> invalidated) ||
                    (invalidated != 0 && invalidated != 1)) {
                    error = "invalid replication slot state entry";
                    return false;
                }
                slot.invalidated = invalidated != 0;
            }
            if (slot.invalidated && slot.slotType != "logical") {
                error = "invalid replication slot state entry";
                return false;
            }
            std::string trailing;
            if (row >> trailing || loaded.count(slot.name) != 0) {
                error = "invalid replication slot state entry";
                return false;
            }
            slot.active = false;
            loaded.emplace(slot.name, std::move(slot));
        }
        if (input.bad()) {
            error = "cannot read replication slot state";
            return false;
        }
    }

    for (const auto& [name, slot] : slots_) {
        (void)slot;
        LogicalChangeStore::instance().discard(name);
    }
    slots_ = std::move(loaded);
    slotStoragePath_ = path;
    return true;
}

bool ReplicationManager::validSlotDefinition(const std::string& name,
                                             const std::string& type,
                                             const std::string& plugin,
                                             const std::string& database) {
    if (name.empty() || name.size() > 63) return false;
    for (unsigned char c : name) {
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
              c == '_')) {
            return false;
        }
    }
    if (type != "physical" && type != "logical") return false;
    const auto validPersistedText = [](const std::string& value) {
        return std::none_of(value.begin(), value.end(), [](unsigned char c) {
            return c == '\0' || c == '\n' || c == '\r';
        });
    };
    if (!validPersistedText(plugin) || !validPersistedText(database))
        return false;
    if (type == "physical") return plugin.empty() && database.empty();
    if (plugin.empty() || database.empty()) return false;
    const auto plugins = LogicalDecoder::availablePlugins();
    return std::find(plugins.begin(), plugins.end(), plugin) != plugins.end();
}

bool ReplicationManager::createReplicationSlot(const std::string& name,
                                               const std::string& type,
                                               const std::string& plugin,
                                               const std::string& database) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!validSlotDefinition(name, type, plugin, database)) return false;
    if (slots_.count(name)) return false;
    ReplicationSlot slot;
    slot.name = name;
    slot.slotType = type;
    slot.plugin = plugin;
    slot.database = database;
    slot.active = false;
    slot.invalidated = false;
    slots_[name] = std::move(slot);
    if (!persistSlotsLocked()) {
        slots_.erase(name);
        (void)persistSlotsLocked();
        return false;
    }
    return true;
}

bool ReplicationManager::dropReplicationSlot(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(name);
    if (it == slots_.end()) return false;
    if (it->second.active) return false;  // cannot drop active slot
    const ReplicationSlot removed = it->second;
    slots_.erase(it);
    if (!persistSlotsLocked()) {
        slots_.emplace(name, removed);
        (void)persistSlotsLocked();
        return false;
    }
    LogicalChangeStore::instance().discard(name);
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
    if (it == slots_.end() || it->second.active || it->second.invalidated)
        return false;
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
    const int64_t previousRestartLsn = it->second.restartLsn;
    it->second.restartLsn = newRestartLsn;
    if (!persistSlotsLocked()) {
        it->second.restartLsn = previousRestartLsn;
        (void)persistSlotsLocked();
        return false;
    }
    return true;
}

bool ReplicationManager::confirmLogicalSlotLsn(
    const std::string& name, int64_t confirmedLsn) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = slots_.find(name);
    if (it == slots_.end() || it->second.slotType != "logical" ||
        it->second.invalidated || confirmedLsn < it->second.restartLsn) {
        return false;
    }
    const int64_t previousRestartLsn = it->second.restartLsn;
    it->second.restartLsn = confirmedLsn;
    if (!persistSlotsLocked()) {
        it->second.restartLsn = previousRestartLsn;
        (void)persistSlotsLocked();
        return false;
    }
    LogicalChangeStore::instance().acknowledge(
        name, static_cast<uint64_t>(confirmedLsn));
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

void ReplicationManager::publishLogicalBatch(
    const std::string& database, const LogicalChangeBatch& batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    bool catalogChanged = false;
    for (auto& [name, slot] : slots_) {
        if (slot.slotType != "logical" || slot.database != database ||
            slot.invalidated) {
            continue;
        }
        if (LogicalChangeStore::instance().append(name, batch)) continue;
        slot.invalidated = true;
        slot.active = false;
        LogicalChangeStore::instance().discard(name);
        catalogChanged = true;
    }
    if (catalogChanged) (void)persistSlotsLocked();
}

bool ReplicationManager::persistSlotsLocked() const {
    if (slotStoragePath_.empty()) return true;
    std::ostringstream output;
    output << "DBMS_REPLICATION_SLOTS_V2\n";
    for (const auto& [name, slot] : slots_) {
        output << std::quoted(name) << ' ' << std::quoted(slot.slotType) << ' '
               << std::quoted(slot.plugin) << ' '
               << std::quoted(slot.database) << ' ' << slot.restartLsn << ' '
               << (slot.invalidated ? 1 : 0) << '\n';
    }
    return index_file::writeAtomically(slotStoragePath_, output.str());
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
