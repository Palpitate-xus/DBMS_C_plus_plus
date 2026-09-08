// ============================================================================
// Replication test — Phase 8
// ============================================================================

#include "replication/ReplicationManager.h"
#include "replication/LogicalDecoder.h"
#include <cassert>
#include <iostream>

using namespace dbms;

static void test_replication_slots() {
    auto& mgr = ReplicationManager::instance();

    // Create slots
    assert(mgr.createReplicationSlot("slot1", "physical"));
    assert(mgr.createReplicationSlot(
        "slot2", "logical", "test_decoding", "testdb"));
    assert(!mgr.createReplicationSlot("slot1", "physical"));  // duplicate
    assert(!mgr.createReplicationSlot("bad/name", "physical"));
    assert(!mgr.createReplicationSlot("bad_type", "unknown"));
    assert(!mgr.createReplicationSlot("physical_plugin", "physical", "plugin"));
    assert(!mgr.createReplicationSlot("logical_no_plugin", "logical"));

    // Find
    auto s1 = mgr.findSlot("slot1");
    assert(s1);
    assert(s1->slotType == "physical");
    assert(s1->name == "slot1");

    auto s2 = mgr.findSlot("slot2");
    assert(s2);
    assert(s2->plugin == "test_decoding");
    assert(s2->database == "testdb");

    LogicalChangeBatch batch;
    batch.xid = 1;
    batch.commitLsn = 1;
    batch.changes.push_back(
        {LogicalChange::Op::Insert, "t", "", "1", 1, 1});
    mgr.publishLogicalBatch("otherdb", batch);
    assert(LogicalChangeStore::instance().depth("slot2") == 0);
    mgr.publishLogicalBatch("testdb", batch);
    assert(LogicalChangeStore::instance().depth("slot2") == 1);

    assert(mgr.activateReplicationSlot("slot1"));
    assert(mgr.findSlot("slot1")->active);
    assert(!mgr.dropReplicationSlot("slot1"));
    assert(mgr.deactivateReplicationSlot("slot1"));

    // List
    auto slots = mgr.listSlots();
    assert(slots.size() == 2);

    // Drop
    assert(mgr.dropReplicationSlot("slot1"));
    assert(!mgr.findSlot("slot1").has_value());
    assert(!mgr.dropReplicationSlot("slot1"));  // already dropped
    assert(mgr.dropReplicationSlot("slot2"));

    std::cout << "[REPLICATION] slots OK" << std::endl;
}

static void test_standby_mode() {
    auto& mgr = ReplicationManager::instance();
    assert(mgr.standbyMode() == ReplicationManager::StandbyMode::None);

    mgr.setStandbyMode(ReplicationManager::StandbyMode::HotStandby);
    assert(mgr.standbyMode() == ReplicationManager::StandbyMode::HotStandby);
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
