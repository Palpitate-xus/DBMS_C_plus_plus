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
        // Once retained output overflows, the stream has a gap and must not
        // be consumed.  The flag is durable; recovery requires drop/recreate.
        bool invalidated = false;
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
    // Persist a logical slot's confirmed LSN before releasing retained
    // changes.  On persistence failure both the LSN and stream stay intact.
    bool confirmLogicalSlotLsn(const std::string& name,
                               int64_t confirmedLsn);
    std::vector<ReplicationSlot> listSlots() const;
    // Publish a committed batch while holding the same manager lock used by
    // slot drop, so drop+discard cannot race with an old slot snapshot.
    void publishLogicalBatch(const std::string& database,
                             const LogicalChangeBatch& batch);

    // Physical replication is not implemented.  These compatibility-facing
    // setters are fail-closed capability gates: only the disabled/default
    // state is accepted, so callers cannot mistake an in-memory flag for a
    // working standby, WAL receiver, or synchronous commit path.
    enum class StandbyMode { None, HotStandby, Recovery };
    bool setStandbyMode(StandbyMode mode);
    StandbyMode standbyMode() const;

    // Non-empty primary connection configuration is rejected until a WAL
    // receiver exists.
    bool setPrimaryConnInfo(const std::string& conninfo);
    std::string primaryConnInfo() const;

    // Enabling synchronous replication is rejected until commit wait and
    // remote write/flush/apply acknowledgement are implemented.
    bool setSyncReplication(bool on);
    bool syncReplication() const;

    // Promotion succeeds only for a real active standby; while the capability
    // gate above is closed this therefore returns false.
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
