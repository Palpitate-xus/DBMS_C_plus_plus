// ============================================================================
// Phase 8-10 tests: Replication, Process Manager, Large Objects
// ============================================================================

#include "test_utils.h"
#include "replication/ReplicationManager.h"
#include "process/ProcessManager.h"
#include "storage/LargeObject.h"
#include <cassert>
#include <filesystem>
#include <iostream>

// === Phase 8: ReplicationManager ===
static void test_streaming_replication() {
    auto& repl = dbms::ReplicationManager::instance();
    repl.setStandbyMode(dbms::ReplicationManager::StandbyMode::HotStandy);
    assert(repl.isActiveStandby());
    assert(repl.standbyMode() == dbms::ReplicationManager::StandbyMode::HotStandy);
    repl.setStandbyMode(dbms::ReplicationManager::StandbyMode::None);
    assert(!repl.isActiveStandby());
    std::cout << "[P8] streaming replication OK" << std::endl;
}

static void test_sync_replication() {
    auto& repl = dbms::ReplicationManager::instance();
    repl.setSyncReplication(true);
    assert(repl.syncReplication());
    repl.setSyncReplication(false);
    assert(!repl.syncReplication());
    std::cout << "[P8] sync replication OK" << std::endl;
}

static void test_wal_shipping() {
    auto& repl = dbms::ReplicationManager::instance();
    repl.setPrimaryConnInfo("host=primary port=5432 user=replicator");
    assert(!repl.primaryConnInfo().empty());
    std::cout << "[P8] WAL shipping OK" << std::endl;
}

static void test_replication_slots() {
    auto& repl = dbms::ReplicationManager::instance();
    assert(repl.createReplicationSlot("test_slot", "physical"));
    assert(repl.createReplicationSlot("log_slot", "logical", "test_decoding"));

    auto* s = repl.findSlot("test_slot");
    assert(s != nullptr);
    assert(s->slotType == "physical");

    auto slots = repl.listSlots();
    assert(slots.size() == 2);

    assert(repl.dropReplicationSlot("test_slot"));
    assert(repl.findSlot("test_slot") == nullptr);
    std::cout << "[P8] replication slots OK" << std::endl;
}

static void test_promote() {
    auto& repl = dbms::ReplicationManager::instance();
    repl.setStandbyMode(dbms::ReplicationManager::StandbyMode::HotStandy);
    assert(repl.promote());
    assert(repl.standbyMode() == dbms::ReplicationManager::StandbyMode::None);
    std::cout << "[P8] failover promote OK" << std::endl;
}

// === Phase 9: Process Manager ===
static void test_process_manager() {
    auto& pm = dbms::ProcessManager::instance();

    assert(pm.spawnBackend(dbms::BackendType::ClientBackend, "mydb", "user1"));
    assert(pm.spawnBackend(dbms::BackendType::BgWriter));
    assert(pm.activeBackends() >= 2);

    auto backends = pm.listBackends();
    assert(backends.size() == 2);
    assert(backends[0].type == dbms::BackendType::ClientBackend);
    assert(backends[1].type == dbms::BackendType::BgWriter);

    pm.shutdown();
    assert(pm.activeBackends() == 0);
    std::cout << "[P9] process manager OK" << std::endl;
}

// === Phase 10: Large Object ===
static void test_large_object() {
    std::string dbPath = testDbPath("lo_test_db");
    cleanupTestDb("lo_test_db");

    dbms::LargeObjectManager loMgr(dbPath);

    int lo = loMgr.create();
    assert(lo == 1);

    assert(loMgr.write(lo, 0, "Hello World"));
    assert(loMgr.read(lo) == "Hello World");
    assert(loMgr.write(lo, 6, "DBMS"));
    assert(loMgr.read(lo).rfind("DBMS") != std::string::npos);
    assert(loMgr.size(lo) >= 9);
    assert(loMgr.truncate(lo, 5));
    assert(loMgr.size(lo) == 5);
    assert(loMgr.drop(lo));
    assert(loMgr.size(lo) == 0);

    std::filesystem::remove_all(dbPath);
    std::cout << "[P10] large object OK" << std::endl;
}

int main() {
    test_streaming_replication();
    test_sync_replication();
    test_wal_shipping();
    test_replication_slots();
    test_promote();
    test_process_manager();
    test_large_object();
    std::cout << "[P8_10] all passed" << std::endl;
    return 0;
}
