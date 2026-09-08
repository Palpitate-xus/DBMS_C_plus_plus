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
#include <fstream>
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
    LogicalChange trunc;
    trunc.op = LogicalChange::Op::Truncate;
    trunc.table = "t1";
    batch.changes = {ins, upd, del, trunc};

    std::string text;
    assert(LogicalDecoder::format("test_decoding", batch, text));
    assert(text.find("table t1: INSERT: 1|alice") != std::string::npos);
    assert(text.find("table t1: UPDATE: old-key 1|alice new-tuple 1|bob") != std::string::npos);
    assert(text.find("table t1: DELETE: old-key 1|bob") != std::string::npos);
    assert(text.find("table t1: TRUNCATE") != std::string::npos);
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
    assert(binary.size() >= 10 && binary[binary.size() - 10] == 'T');
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
    pub.publishUpdate = false;
    pub.publishTruncate = false;
    std::string error;
    assert(cat.create(db, pub, error));
    assert(error.empty());
    // Duplicate rejected.
    assert(!cat.create(db, pub, error));
    assert(error.find("already exists") != std::string::npos);

    // Publication names become filenames.  Every catalog entry point must
    // reject path-like names before touching either the catalog directory or
    // a sibling path.
    const fs::path escapedPath = "publication_escape.publication";
    fs::remove(escapedPath);
    Publication unsafe = pub;
    unsafe.name = "../publication_escape";
    assert(!cat.create(db, unsafe, error));
    assert(error.find("invalid publication name") != std::string::npos);
    assert(!fs::exists(escapedPath));
    assert(!cat.update(db, unsafe, error));
    assert(!cat.exists(db, unsafe.name));

    // Persistence: list reloads from files.
    auto pubs = cat.list(db);
    assert(pubs.size() == 1);
    assert(pubs[0].name == "mypub");
    assert(pubs[0].owner == "admin");
    assert(pubs[0].tables.size() == 2);
    assert(pubs[0].publishInsert && !pubs[0].publishUpdate &&
           pubs[0].publishDelete && !pubs[0].publishTruncate);
    assert(!pubs[0].publishAllTables);

    assert(cat.publishes(db, "orders"));
    assert(cat.publishes(db, "customers"));
    assert(!cat.publishes(db, "audit"));
    assert(cat.publishes(db, "orders", LogicalChange::Op::Insert));
    assert(!cat.publishes(db, "orders", LogicalChange::Op::Update));
    assert(cat.publishes(db, "orders", LogicalChange::Op::Delete));
    assert(!cat.publishes(db, "orders", LogicalChange::Op::Truncate));

    // Legacy files had no truncate flag.  Loading them must leave the new
    // operation disabled instead of silently broadening an old publication.
    {
        std::ofstream legacy(fs::path(db) / "legacy.publication");
        legacy << "admin 1 1 1 0\norders\n";
    }
    pubs = cat.list(db);
    const auto legacy = std::find_if(
        pubs.begin(), pubs.end(), [](const Publication& candidate) {
            return candidate.name == "legacy";
        });
    assert(legacy != pubs.end());
    assert(!legacy->publishTruncate && !legacy->publishAllTables);

    // FOR ALL TABLES publication.
    Publication all;
    all.name = "allpub";
    all.owner = "admin";
    all.publishAllTables = true;
    assert(cat.create(db, all, error));
    assert(cat.publishes(db, "anything"));

    // Rename is atomic and refuses to overwrite another publication.
    assert(!cat.rename(db, "mypub", "allpub", error));
    assert(cat.exists(db, "mypub") && cat.exists(db, "allpub"));
    assert(!cat.rename(db, "mypub", "../publication_escape", error));
    assert(cat.exists(db, "mypub") && !fs::exists(escapedPath));
    assert(cat.rename(db, "mypub", "renamed_pub", error));
    assert(!cat.exists(db, "mypub") && cat.exists(db, "renamed_pub"));
    pubs = cat.list(db);
    const auto renamed = std::find_if(
        pubs.begin(), pubs.end(), [](const Publication& candidate) {
            return candidate.name == "renamed_pub";
        });
    assert(renamed != pubs.end() && renamed->owner == "admin");

    Publication firstDrop;
    firstDrop.name = "drop_first";
    firstDrop.owner = "admin";
    Publication secondDrop = firstDrop;
    secondDrop.name = "drop_second";
    assert(cat.create(db, firstDrop, error));
    assert(cat.create(db, secondDrop, error));
    std::vector<std::string> missing;
    assert(!cat.dropMany(
        db, {"drop_first", "../publication_escape"}, true,
        missing, error));
    assert(cat.exists(db, "drop_first") && !fs::exists(escapedPath));
    assert(!cat.dropMany(
        db, {"drop_first", "missing", "drop_second"}, false,
        missing, error));
    assert(missing == std::vector<std::string>{"missing"});
    assert(cat.exists(db, "drop_first") && cat.exists(db, "drop_second"));
    assert(cat.dropMany(
        db, {"drop_first", "missing", "drop_second"}, true,
        missing, error));
    assert(missing == std::vector<std::string>{"missing"});
    assert(!cat.exists(db, "drop_first") && !cat.exists(db, "drop_second"));

    assert(cat.drop(db, "renamed_pub", error));
    assert(!cat.exists(db, "mypub"));
    assert(!cat.drop(db, "../publication_escape", error));
    assert(!fs::exists(escapedPath));
    assert(!cat.drop(db, "renamed_pub", error));
    assert(cat.drop(db, "allpub", error));
    assert(cat.drop(db, "legacy", error));
    fs::remove_all(db);
    std::cout << "[LOGICAL] publication catalog OK" << std::endl;
}

static void test_publication_tracks_table_rename() {
    const std::string db = testDbPath("logical_pub_rename");
    if (g_engine.databaseExists(db)) g_engine.dropDatabase(db);
    cleanupTestDb("logical_pub_rename");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    Session s;
    s.currentDB = db;
    s.username = "admin";
    s.permission = 1;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE published_t (id INT)", s));

    Publication pub;
    pub.name = "rename_pub";
    pub.owner = "admin";
    pub.tables = {"published_t"};
    std::string error;
    auto& publications = PublicationCatalog::instance();
    assert(publications.create(db, pub, error));

    assert(!ddl.executeSql(
        "ALTER TABLE published_t RENAME TO renamed_t", s));
    assert(!publications.publishes(db, "published_t"));
    assert(publications.publishes(db, "renamed_t"));
    const auto persisted = publications.list(db);
    assert(persisted.size() == 1);
    assert(persisted[0].tables == std::vector<std::string>{"renamed_t"});

    // Reusing the old relation name must not subscribe an unrelated table.
    assert(!ddl.executeSql("CREATE TABLE published_t (id INT)", s));
    assert(!publications.publishes(db, "published_t"));

    assert(!ddl.executeSql("DROP TABLE renamed_t", s));
    assert(!publications.publishes(db, "renamed_t"));
    const auto afterDrop = publications.list(db);
    assert(afterDrop.size() == 1 && afterDrop[0].tables.empty());
    assert(!ddl.executeSql("CREATE TABLE renamed_t (id INT)", s));
    assert(!publications.publishes(db, "renamed_t"));

    Publication allTables;
    allTables.name = "rename_all_pub";
    allTables.owner = "admin";
    allTables.publishAllTables = true;
    assert(publications.create(db, allTables, error));
    assert(publications.publishes(db, "renamed_t"));
    assert(!ddl.executeSql("DROP TABLE renamed_t", s));
    const auto afterAllTablesDrop = publications.list(db);
    const auto allTablesEntry = std::find_if(
        afterAllTablesDrop.begin(), afterAllTablesDrop.end(),
        [](const Publication& candidate) {
            return candidate.name == "rename_all_pub";
        });
    assert(allTablesEntry != afterAllTablesDrop.end());
    assert(allTablesEntry->publishAllTables && allTablesEntry->tables.empty());
    assert(!ddl.executeSql("CREATE TABLE renamed_t (id INT)", s));
    assert(publications.publishes(db, "renamed_t"));

    assert(publications.drop(db, "rename_all_pub", error));
    assert(publications.drop(db, "rename_pub", error));
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb("logical_pub_rename");
    std::cout << "[LOGICAL] publication follows table rename OK" << std::endl;
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

    // The embedded API's implicit transaction follows the same commit path.
    assert(g_engine.insert(
               db, "src_t", {{"id", "4"}, {"v", "four"}}) ==
           DBStatus::OK);
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 1);
    auto autocommitPeek = LogicalChangeStore::instance().peek(
        "e2e_slot", peek.nextLsn, 100);
    assert(autocommitPeek.batches.size() == 1);
    assert(autocommitPeek.batches[0].changes.size() == 1);
    assert(autocommitPeek.batches[0].changes[0].newRow == "4|four");
    LogicalChangeStore::instance().acknowledge(
        "e2e_slot", autocommitPeek.nextLsn);
    assert(repl.advanceSlotLsn(
        "e2e_slot", static_cast<int64_t>(autocommitPeek.nextLsn)));

    // Publication operation flags filter DML capture independently of table
    // membership.  Keep INSERT enabled while disabling UPDATE and DELETE.
    pub.publishUpdate = false;
    pub.publishDelete = false;
    assert(PublicationCatalog::instance().update(db, pub, error));
    assert(g_engine.insert(
               db, "src_t", {{"id", "5"}, {"v", "five"}}) ==
           DBStatus::OK);
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 1);
    assert(g_engine.update(
               db, "src_t", {{"v", "updated"}}, {"=id 5"}) ==
           DBStatus::OK);
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 1);
    assert(g_engine.remove(db, "src_t", {"=id 5"}) == DBStatus::OK);
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 1);
    auto filteredPeek = LogicalChangeStore::instance().peek(
        "e2e_slot", autocommitPeek.nextLsn, 100);
    assert(filteredPeek.batches.size() == 1);
    assert(filteredPeek.batches[0].changes.size() == 1);
    assert(filteredPeek.batches[0].changes[0].op ==
           LogicalChange::Op::Insert);
    assert(filteredPeek.batches[0].changes[0].newRow == "5|five");
    LogicalChangeStore::instance().acknowledge(
        "e2e_slot", filteredPeek.nextLsn);
    assert(repl.advanceSlotLsn(
        "e2e_slot", static_cast<int64_t>(filteredPeek.nextLsn)));

    // TRUNCATE is emitted once the DDL transaction commits and carries no
    // row image.
    assert(!ddl.executeSql("TRUNCATE TABLE src_t", s));
    assert(LogicalChangeStore::instance().depth("e2e_slot") == 1);
    auto truncatePeek = LogicalChangeStore::instance().peek(
        "e2e_slot", filteredPeek.nextLsn, 100);
    assert(truncatePeek.batches.size() == 1);
    assert(truncatePeek.batches[0].changes.size() == 1);
    assert(truncatePeek.batches[0].changes[0].op ==
           LogicalChange::Op::Truncate);
    assert(truncatePeek.batches[0].changes[0].oldRow.empty());
    assert(truncatePeek.batches[0].changes[0].newRow.empty());
    assert(LogicalDecoder::format(
        "test_decoding", truncatePeek.batches[0], text));
    assert(text.find("TRUNCATE") != std::string::npos);
    LogicalChangeStore::instance().acknowledge(
        "e2e_slot", truncatePeek.nextLsn);
    assert(repl.advanceSlotLsn(
        "e2e_slot", static_cast<int64_t>(truncatePeek.nextLsn)));

    assert(repl.dropReplicationSlot("e2e_slot"));
    g_engine.dropDatabase(db);
    cleanupTestDb(db);
    std::cout << "[LOGICAL] end-to-end streaming OK" << std::endl;
}

static void test_prepared_transaction_streaming() {
    const std::string db = testDbPath("logical_prepared");
    const std::string slotName = "logical_prepared_slot";
    if (g_engine.databaseExists(db)) g_engine.dropDatabase(db);
    cleanupTestDb("logical_prepared");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);

    TableSchema table;
    table.tablename = "prepared_rows";
    table.append(makeIntColumn("id", false, 2, true));
    table.append(makeVarCharColumn("value", false, 32));
    assert(g_engine.createTable(db, table) == DBStatus::OK);

    Publication pub;
    pub.name = "logical_prepared_pub";
    pub.owner = "admin";
    pub.tables = {table.tablename};
    std::string error;
    assert(PublicationCatalog::instance().create(db, pub, error));

    auto& replication = ReplicationManager::instance();
    assert(replication.createReplicationSlot(
        slotName, "logical", "test_decoding"));
    auto& changes = LogicalChangeStore::instance();
    StorageEngine completingBackend;

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(
               db, table.tablename,
               {{"id", "1"}, {"value", "prepared value"}}) ==
           DBStatus::OK);
    const uint64_t committedXid = g_engine.currentTxnId();
    assert(g_engine.prepareTransaction("logical_prepared_commit") ==
           DBStatus::OK);
    assert(changes.depth(slotName) == 0);
    assert(completingBackend.commitPrepared("logical_prepared_commit") ==
           DBStatus::OK);
    assert(changes.depth(slotName) == 1);

    auto peek = changes.peek(slotName, 0, 10);
    assert(peek.batches.size() == 1);
    assert(peek.batches[0].xid == committedXid);
    assert(peek.batches[0].changes.size() == 1);
    assert(peek.batches[0].changes[0].op == LogicalChange::Op::Insert);
    assert(peek.batches[0].changes[0].table == table.tablename);
    assert(peek.batches[0].changes[0].newRow == "1|prepared value");
    changes.acknowledge(slotName, peek.nextLsn);
    assert(changes.depth(slotName) == 0);

    // PREPARE must detach the originating backend's logical buffer. An
    // otherwise-empty transaction must not republish the prepared change.
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(changes.depth(slotName) == 0);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.insert(
               db, table.tablename,
               {{"id", "2"}, {"value", "rolled back value"}}) ==
           DBStatus::OK);
    assert(g_engine.prepareTransaction("logical_prepared_rollback") ==
           DBStatus::OK);
    assert(completingBackend.rollbackPrepared("logical_prepared_rollback") ==
           DBStatus::OK);
    assert(changes.depth(slotName) == 0);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(changes.depth(slotName) == 0);

    assert(replication.dropReplicationSlot(slotName));
    assert(PublicationCatalog::instance().drop(db, pub.name, error));
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb("logical_prepared");
    std::cout << "[LOGICAL] prepared commit/rollback streaming OK" << std::endl;
}

int main() {
    test_output_plugins();
    test_publication_catalog();
    test_publication_tracks_table_rename();
    test_change_store();
    test_end_to_end_streaming();
    test_prepared_transaction_streaming();
    std::cout << "[LOGICAL] all tests passed" << std::endl;
    return 0;
}
