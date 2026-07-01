#pragma once

#include <string>
#include <vector>
#include <map>
#include <mutex>

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
    ReplicationSlot* findSlot(const std::string& name);
    std::vector<ReplicationSlot> listSlots() const;

    // Streaming replication state (8.1, 8.2)
    enum class StandbyMode { None, HotStandy, Recovery };
    void setStandbyMode(StandbyMode mode) { standbyMode_ = mode; }
    StandbyMode standbyMode() const { return standbyMode_; }

    // WAL shipping (8.8)
    void setPrimaryConnInfo(const std::string& conninfo) { primaryConnInfo_ = conninfo; }
    const std::string& primaryConnInfo() const { return primaryConnInfo_; }

    // Sync replication (8.4)
    void setSyncReplication(bool on) { syncReplication_ = on; }
    bool syncReplication() const { return syncReplication_; }

    // Failover/Promote (8.12)
    bool promote();
    bool isActiveStandby() const { return standbyMode_ != StandbyMode::None; }

private:
    ReplicationManager() = default;
    mutable std::mutex mutex_;
    std::map<std::string, ReplicationSlot> slots_;
    StandbyMode standbyMode_ = StandbyMode::None;
    std::string primaryConnInfo_;
    bool syncReplication_ = false;
};

} // namespace dbms
