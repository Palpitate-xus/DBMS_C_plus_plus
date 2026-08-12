// ReplicationManager must expose snapshots and synchronize all shared state.

#include "replication/ReplicationManager.h"

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

    assert(manager.findSlot(slot).has_value());
    assert(manager.deactivateReplicationSlot(slot) || !manager.findSlot(slot)->active);
    assert(manager.dropReplicationSlot(slot));
    manager.setStandbyMode(ReplicationManager::StandbyMode::None);
    std::cout << "[REPLICATION CONCURRENCY] synchronized snapshot API OK\n";
    return 0;
}
