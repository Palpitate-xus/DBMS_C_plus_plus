// ============================================================================
// Replication test — Phase 8
// ============================================================================

#include "replication/ReplicationManager.h"
#include <cassert>
#include <iostream>

using namespace dbms;

static void test_replication_slots() {
    auto& mgr = ReplicationManager::instance();

    // Create slots
    assert(mgr.createReplicationSlot("slot1", "physical"));
    assert(mgr.createReplicationSlot("slot2", "logical", "test_decoding"));
    assert(!mgr.createReplicationSlot("slot1", "physical"));  // duplicate

    // Find
    auto* s1 = mgr.findSlot("slot1");
    assert(s1);
    assert(s1->slotType == "physical");
    assert(s1->name == "slot1");

    auto* s2 = mgr.findSlot("slot2");
    assert(s2);
    assert(s2->plugin == "test_decoding");

    // List
    auto slots = mgr.listSlots();
    assert(slots.size() == 2);

    // Drop
    assert(mgr.dropReplicationSlot("slot1"));
    assert(!mgr.findSlot("slot1"));
    assert(!mgr.dropReplicationSlot("slot1"));  // already dropped

    std::cout << "[REPLICATION] slots OK" << std::endl;
}

static void test_standby_mode() {
    auto& mgr = ReplicationManager::instance();
    assert(mgr.standbyMode() == ReplicationManager::StandbyMode::None);

    mgr.setStandbyMode(ReplicationManager::StandbyMode::HotStandy);
    assert(mgr.standbyMode() == ReplicationManager::StandbyMode::HotStandy);
    assert(mgr.isActiveStandby());

    // Promote
    assert(mgr.promote());
    assert(mgr.standbyMode() == ReplicationManager::StandbyMode::None);
    assert(!mgr.isActiveStandby());

    std::cout << "[REPLICATION] standby/promote OK" << std::endl;
}

static void test_wal_shipping_config() {
    auto& mgr = ReplicationManager::instance();
    mgr.setPrimaryConnInfo("host=primary port=5432 user=replicator");
    assert(mgr.primaryConnInfo() == "host=primary port=5432 user=replicator");

    mgr.setSyncReplication(true);
    assert(mgr.syncReplication());

    std::cout << "[REPLICATION] WAL shipping config OK" << std::endl;
}

int main() {
    test_replication_slots();
    test_standby_mode();
    test_wal_shipping_config();
    std::cout << "[REPLICATION] all passed" << std::endl;
    return 0;
}
