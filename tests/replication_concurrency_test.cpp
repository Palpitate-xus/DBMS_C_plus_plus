// ReplicationManager must expose snapshots and synchronize all shared state.

#include "replication/ReplicationManager.h"
#include "replication/LogicalDecoder.h"

#include <atomic>
#include <cassert>
#include <iostream>
#include <thread>

using namespace dbms;

int main() {
    auto& manager = ReplicationManager::instance();
    const std::string slot = "concurrent_slot";
    (void)manager.deactivateReplicationSlot(slot);
    (void)manager.dropReplicationSlot(slot);
    assert(manager.createReplicationSlot(slot, "physical"));

    std::thread stateWriter([&] {
        for (int i = 0; i < 500; ++i) {
            manager.setSyncReplication((i & 1) != 0);
            manager.setStandbyMode((i & 1) != 0
                ? ReplicationManager::StandbyMode::HotStandby
                : ReplicationManager::StandbyMode::None);
            manager.setPrimaryConnInfo("host=primary" + std::to_string(i));
        }
    });
    std::thread slotWriter([&] {
        for (int i = 0; i < 500; ++i) {
            if (manager.activateReplicationSlot(slot)) {
                assert(manager.findSlot(slot)->active);
                assert(manager.deactivateReplicationSlot(slot));
            }
            auto snapshot = manager.findSlot(slot);
            if (snapshot) assert(snapshot->name == slot);
            (void)manager.listSlots();
        }
    });
    stateWriter.join();
    slotWriter.join();

    assert(manager.standbyMode() == ReplicationManager::StandbyMode::None);
    assert(!manager.isActiveStandby());
    assert(!manager.syncReplication());
    assert(manager.primaryConnInfo().empty());
    assert(manager.findSlot(slot).has_value());
    assert(manager.deactivateReplicationSlot(slot) || !manager.findSlot(slot)->active);
    assert(manager.dropReplicationSlot(slot));

    // Publishing and dropping the same logical slot must be serializable.
    // Whichever operation acquires the manager lock first, no retained batch
    // may remain after drop returns and both threads finish.
    const std::string logicalSlot = "concurrent_logical_slot";
    LogicalChangeBatch batch;
    batch.changes.push_back(
        {LogicalChange::Op::Insert, "t", "", "1", 1, 1});
    for (uint64_t round = 1; round <= 200; ++round) {
        batch.xid = round;
        batch.commitLsn = round;
        assert(manager.createReplicationSlot(
            logicalSlot, "logical", "dbms_test_decoding", "concurrency_db"));
        std::atomic<bool> start{false};
        std::atomic<bool> dropped{false};
        std::thread publisher([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            manager.publishLogicalBatch("concurrency_db", batch);
        });
        std::thread dropper([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            dropped.store(manager.dropReplicationSlot(logicalSlot),
                          std::memory_order_release);
        });
        start.store(true, std::memory_order_release);
        publisher.join();
        dropper.join();
        assert(dropped.load(std::memory_order_acquire));
        assert(!manager.findSlot(logicalSlot));
        assert(LogicalChangeStore::instance().depth(logicalSlot) == 0);
    }
    assert(manager.setStandbyMode(ReplicationManager::StandbyMode::None));
    std::cout << "[REPLICATION CONCURRENCY] synchronized snapshot API OK\n";
    return 0;
}
