#pragma once

#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <optional>
#include <cstdint>

namespace dbms {

struct LogicalChangeBatch;

// Replication manager for Phase 8
class ReplicationManager {
public:
    static ReplicationManager& instance();

    // Configure and load the cluster-wide slot state file.  Loading replaces
    // the in-memory catalog and resets all slots to inactive.
    bool configureSlotStorage(const std::string& path, std::string& error);

    // Replication slot management (8.3)
    struct ReplicationSlot {
        std::string name;
        std::string plugin;       // output plugin for logical decoding
        std::string slotType;     // "physical" or "logical"
        std::string database;     // logical slots are bound to one database
        int64_t restartLsn = 0;
        bool active = false;
    };

    bool createReplicationSlot(const std::string& name, const std::string& type,
                                const std::string& plugin = "",
                                const std::string& database = "");
    bool dropReplicationSlot(const std::string& name);
    // Return a snapshot; never expose an entry whose lifetime depends on the
    // internal mutex remaining held.
    std::optional<ReplicationSlot> findSlot(const std::string& name) const;
    bool activateReplicationSlot(const std::string& name);
    bool deactivateReplicationSlot(const std::string& name);
    // Advance a slot's confirmed restart LSN (logical decoding flow).
    bool advanceSlotLsn(const std::string& name, int64_t newRestartLsn);
    std::vector<ReplicationSlot> listSlots() const;
    // Publish a committed batch while holding the same manager lock used by
    // slot drop, so drop+discard cannot race with an old slot snapshot.
    void publishLogicalBatch(const std::string& database,
                             const LogicalChangeBatch& batch);

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
                                    const std::string& plugin,
                                    const std::string& database);
    bool persistSlotsLocked() const;
    mutable std::mutex mutex_;
    std::map<std::string, ReplicationSlot> slots_;
    std::string slotStoragePath_;
    StandbyMode standbyMode_ = StandbyMode::None;
    std::string primaryConnInfo_;
    bool syncReplication_ = false;
};

} // namespace dbms
