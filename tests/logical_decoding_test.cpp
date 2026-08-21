// ============================================================================
// logical_decoding_test — P2-5 logical decoding / publications:
//   LogicalDecoder formats pgoutput (framed) and test_decoding (text)
//   PublicationCatalog create/drop/list/publishes with persistence
//   LogicalChangeStore append/peek/acknowledge including retention bounds
//   end-to-end: publication + logical slot, DML buffered, commit streams
//   to the slot, rollback discards, peek renders via the plugin,
//   acknowledge advances the slot restart LSN
// ============================================================================

#include "replication/LogicalDecoder.h"
#include "replication/ReplicationManager.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "Session.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

using namespace dbms;

namespace fs = std::filesystem;

static void test_output_plugins() {
    LogicalChangeBatch batch;
    batch.xid = 42;
    batch.commitLsn = 1000;
    LogicalChange ins;
    ins.op = LogicalChange::Op::Insert;
    ins.table = "t1";
    ins.newRow = "1|alice";
    LogicalChange upd;
    upd.op = LogicalChange::Op::Update;
    upd.table = "t1";
    upd.oldRow = "1|alice";
    upd.newRow = "1|bob";
    LogicalChange del;
    del.op = LogicalChange::Op::Delete;
    del.table = "t1";
    del.oldRow = "1|bob";
    batch.changes = {ins, upd, del};

    std::string text;
    assert(LogicalDecoder::format("test_decoding", batch, text));
    assert(text.find("table t1: INSERT: 1|alice") != std::string::npos);
    assert(text.find("table t1: UPDATE: old-key 1|alice new-tuple 1|bob") != std::string::npos);
    assert(text.find("table t1: DELETE: old-key 1|bob") != std::string::npos);
    assert(text.find("xid 42") != std::string::npos);

    std::string binary;
    assert(LogicalDecoder::format("pgoutput", batch, binary));
    // Begins with 'B' + xid (little endian), ends with 'C' + commit LSN.
    assert(!binary.empty() && binary[0] == 'B');
    uint64_t xid = 0;
    for (int i = 0; i < 8; ++i)
        xid |= static_cast<uint64_t>(static_cast<unsigned char>(binary[1 + i])) << (8 * i);
    assert(xid == 42);
    assert(binary.size() >= 9);
    assert(binary[binary.size() - 9] == 'C');
    // Unknown plugin rejected.
    assert(!LogicalDecoder::format("nope", batch, binary));
    // Registry lists both plugins.
    auto plugins = LogicalDecoder::availablePlugins();
    assert(std::find(plugins.begin(), plugins.end(), "pgoutput") != plugins.end());
    assert(std::find(plugins.begin(), plugins.end(), "test_decoding") != plugins.end());
    std::cout << "[LOGICAL] output plugins OK" << std::endl;
}

static void test_publication_catalog() {
    const std::string db = testDbPath("logical_pub");
    cleanupTestDb(db);
    fs::create_directories(db);
    auto& cat = PublicationCatalog::instance();

    Publication pub;
    pub.name = "mypub";
    pub.owner = "admin";
    pub.tables = {"orders", "customers"};
    std::string error;
    assert(cat.create(db, pub, error));
    assert(error.empty());
    // Duplicate rejected.
    assert(!cat.create(db, pub, error));
    assert(error.find("already exists") != std::string::npos);

    // Persistence: list reloads from files.
    auto pubs = cat.list(db);
    assert(pubs.size() == 1);
    assert(pubs[0].name == "mypub");
    assert(pubs[0].owner == "admin");
    assert(pubs[0].tables.size() == 2);
    assert(pubs[0].publishInsert && pubs[0].publishUpdate && pubs[0].publishDelete);
    assert(!pubs[0].publishAllTables);

    assert(cat.publishes(db, "orders"));
    assert(cat.publishes(db, "customers"));
    assert(!cat.publishes(db, "audit"));

    // FOR ALL TABLES publication.
    Publication all;
    all.name = "allpub";
    all.owner = "admin";
    all.publishAllTables = true;
    assert(cat.create(db, all, error));
    assert(cat.publishes(db, "anything"));

    assert(cat.drop(db, "mypub", error));
    assert(!cat.exists(db, "mypub"));
    assert(!cat.drop(db, "mypub", error));
    assert(cat.drop(db, "allpub", error));
    fs::remove_all(db);
    std::cout << "[LOGICAL] publication catalog OK" << std::endl;
}

static void test_change_store() {
    auto& store = LogicalChangeStore::instance();
    LogicalChangeBatch b1;
    b1.xid = 1;
    b1.commitLsn = 100;
    b1.changes.push_back({LogicalChange::Op::Insert, "t", "", "1|a", 1, 100});
    LogicalChangeBatch b2;
    b2.xid = 2;
    b2.commitLsn = 200;
    b2.changes.push_back({LogicalChange::Op::Delete, "t", "1|a", "", 2, 200});
    store.append("slot_x", b1);
    store.append("slot_x", b2);
    assert(store.depth("slot_x") == 2);

    // Peek from 0 sees both batches.
    auto peek = store.peek("slot_x", 0, 10);
    assert(peek.batches.size() == 2);
    assert(peek.nextLsn == 200);
    assert(peek.hitEnd);  // everything consumed within the limit
    // A tight limit stops early and reports more data available.
    peek = store.peek("slot_x", 0, 1);
    assert(peek.batches.size() == 1);
    assert(!peek.hitEnd);

    // Resume from 100: only the second batch.
    peek = store.peek("slot_x", 100, 10);
    assert(peek.batches.size() == 1);
    assert(peek.batches[0].xid == 2);
    assert(peek.nextLsn == 200);

    // Acknowledge up to 100 drops the first.
    store.acknowledge("slot_x", 100);
    assert(store.depth("slot_x") == 1);
    peek = store.peek("slot_x", 0, 10);
    assert(peek.batches.size() == 1);
    assert(peek.batches[0].xid == 2);

    // Retention bound: the oldest batch is dropped beyond kMaxRetained.
    for (uint64_t i = 0; i < LogicalChangeStore::kMaxRetained + 8; ++i) {
        LogicalChangeBatch b;
        b.xid = 100 + i;
        b.commitLsn = 1000 + i;
        b.changes.push_back({LogicalChange::Op::Insert, "t", "", "x", b.xid, b.commitLsn});
        store.append("slot_x", b);
    }
    assert(store.depth("slot_x") <= LogicalChangeStore::kMaxRetained);
    store.acknowledge("slot_x", 1000000);
    assert(store.depth("slot_x") == 0);
    peek = store.peek("slot_x", 0, 10);
    assert(peek.batches.empty() && peek.hitEnd);
    std::cout << "[LOGICAL] change store OK" << std::endl;
}

static void test_end_to_end_streaming() {
    const std::string db = testDbPath("logical_e2e");
    if (g_engine.databaseExists(db)) g_engine.dropDatabase(db);
    cleanupTestDb("logical_e2e");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    Session s;
    s.currentDB = db;
    s.username = "admin";
    s.permission = 1;

    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE src_t (id INT, v VARCHAR(32))", s));

    // Publication + slot.  (Publication after data would also work; the
    // catalog only gates future commits.)
    Publication pub;
    pub.name = "e2epub";
    pub.owner = "admin";
    pub.tables = {"src_t"};
    std::string error;
    assert(PublicationCatalog::instance().create(db, pub, error));
    auto& repl = ReplicationManager::instance();
    assert(repl.createReplicationSlot("e2e_slot", "logical", "test_decoding"));

    // Committed transaction streams into the slot.
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "src_t", {{"id", "1"}, {"v", "one"}}) == DBStatus::OK);
    assert(g_engine.insert(db, "src_t", {{"id", "2"}, {"v", "two"}}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 1);

    auto slot = repl.findSlot("e2e_slot");
    assert(slot && slot->slotType == "logical");
    auto peek = LogicalChangeStore::instance().peek("e2e_slot", slot->restartLsn, 100);
    assert(peek.batches.size() == 1);
    assert(peek.batches[0].changes.size() == 2);
    assert(peek.batches[0].changes[0].table == "src_t");
    assert(peek.batches[0].changes[0].op == LogicalChange::Op::Insert);
    std::string text;
    assert(LogicalDecoder::format("test_decoding", peek.batches[0], text));
    assert(text.find("INSERT") != std::string::npos);

    // Rollback discards buffered changes.
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "src_t", {{"id", "3"}, {"v", "three"}}) == DBStatus::OK);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 1);

    // Unpublished table changes never reach the slot.
    assert(!ddl.executeSql("CREATE TABLE other_t (id INT)", s));
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(db, "other_t", {{"id", "9"}}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 1);

    // Acknowledge advances the restart LSN and drains the stream.
    LogicalChangeStore::instance().acknowledge("e2e_slot", peek.nextLsn);
    assert(repl.advanceSlotLsn("e2e_slot", static_cast<int64_t>(peek.nextLsn)));
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 0);
    assert(repl.advanceSlotLsn("e2e_slot", 0) == false);  // never rewind

    assert(repl.dropReplicationSlot("e2e_slot"));
    g_engine.dropDatabase(db);
    cleanupTestDb(db);
    std::cout << "[LOGICAL] end-to-end streaming OK" << std::endl;
}

int main() {
    test_output_plugins();
    test_publication_catalog();
    test_change_store();
    test_end_to_end_streaming();
    std::cout << "[LOGICAL] all tests passed" << std::endl;
    return 0;
}
