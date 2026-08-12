#pragma once

#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <optional>
#include <cstdint>

namespace dbms {

// Replication manager for Phase 8
class ReplicationManager {
public:
    static ReplicationManager& instance();

    // Replication slot management (8.3)
    struct ReplicationSlot {
        std::string name;
        std::string plugin;       // output plugin for logical decoding
        std::string slotType;     // "physical" or "logical"
        int64_t restartLsn = 0;
        bool active = false;
    };

    bool createReplicationSlot(const std::string& name, const std::string& type,
                                const std::string& plugin = "");
    bool dropReplicationSlot(const std::string& name);
    // Return a snapshot; never expose an entry whose lifetime depends on the
    // internal mutex remaining held.
    std::optional<ReplicationSlot> findSlot(const std::string& name) const;
    bool activateReplicationSlot(const std::string& name);
    bool deactivateReplicationSlot(const std::string& name);
    std::vector<ReplicationSlot> listSlots() const;

    // Streaming replication state (8.1, 8.2)
    enum class StandbyMode { None, HotStandby, Recovery };
    void setStandbyMode(StandbyMode mode);
    StandbyMode standbyMode() const;

    // WAL shipping (8.8)
    void setPrimaryConnInfo(const std::string& conninfo);
    std::string primaryConnInfo() const;

    // Sync replication (8.4)
    void setSyncReplication(bool on);
    bool syncReplication() const;

    // Failover/Promote (8.12)
    bool promote();
    bool isActiveStandby() const;

private:
    ReplicationManager() = default;
    static bool validSlotDefinition(const std::string& name,
                                    const std::string& type,
                                    const std::string& plugin);
    mutable std::mutex mutex_;
    std::map<std::string, ReplicationSlot> slots_;
    StandbyMode standbyMode_ = StandbyMode::None;
    std::string primaryConnInfo_;
    bool syncReplication_ = false;
};

} // namespace dbms
