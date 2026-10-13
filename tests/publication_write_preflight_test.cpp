#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "replication/LogicalDecoder.h"
#include "replication/ReplicationManager.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    namespace fs = std::filesystem;
    TypeRegistry::instance().bootstrap();
    const std::string testName = "publication_write_preflight";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "rows";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("value", true, 40));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    Publication publication;
    publication.name = "published_rows";
    publication.owner = "test";
    publication.tables = {"rows"};
    std::string error;
    assert(PublicationCatalog::instance().create(database, publication, error));
    const std::string slot = "publication_preflight_slot";
    auto& replication = ReplicationManager::instance();
    assert(replication.createReplicationSlot(slot, "logical", "dbms_test_decoding", database));
    assert(g_engine.insert(database, "rows", {{"id", "1"}, {"value", "original"}}) == DBStatus::OK);
    LogicalChangeStore::instance().discard(slot);
    const auto original = g_engine.query(database, "rows", {}, {"id", "value"});
    assert(original.size() == 1);
    const fs::path incomplete = fs::path(database) / "incomplete.publication";
    const auto damage = [&] {
        std::ofstream out(incomplete);
        out << "INCOMPLETE\n";
        assert(out.good());
    };
    damage();
    for (int operation = 0; operation < 4; ++operation) {
        DBStatus status;
        if (operation == 0) status = g_engine.insert(database, "rows", {{"id", "2"}, {"value", "new"}});
        else if (operation == 1) status = g_engine.update(database, "rows", {{"value", "changed"}}, {"=id 1"});
        else if (operation == 2) status = g_engine.remove(database, "rows", {"=id 1"});
        else status = g_engine.truncateTable(database, "rows");
        assert(status == DBStatus::IO_ERROR);
        assert(!g_engine.inTransaction());
        assert(g_engine.query(database, "rows", {}, {"id", "value"}) == original);
        assert(LogicalChangeStore::instance().depth(slot) == 0);
        assert(fs::is_regular_file(incomplete));
    }
    fs::remove(incomplete);
    assert(g_engine.insert(database, "rows", {{"id", "2"}, {"value", "healthy"}}) == DBStatus::OK);
    assert(g_engine.update(database, "rows", {{"value", "changed"}}, {"=id 2"}) == DBStatus::OK);
    assert(g_engine.remove(database, "rows", {"=id 2"}) == DBStatus::OK);
    assert(LogicalChangeStore::instance().depth(slot) == 3);
    LogicalChangeStore::instance().discard(slot);

    // A failed later statement retains the earlier transaction's row and
    // logical buffer, rather than aborting or silently omitting the update.
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.insert(database, "rows", {{"id", "3"}, {"value", "earlier"}}) == DBStatus::OK);
    damage();
    assert(g_engine.update(database, "rows", {{"value", "lost"}}, {"=id 3"}) == DBStatus::IO_ERROR);
    assert(g_engine.inTransaction());
    fs::remove(incomplete);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    const auto committed = LogicalChangeStore::instance().peek(slot, 0, 10);
    assert(committed.batches.size() == 1 && committed.batches[0].changes.size() == 1);
    assert(committed.batches[0].changes[0].op == LogicalChange::Op::Insert);
    LogicalChangeStore::instance().discard(slot);
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    assert(g_engine.truncateTable(database, "rows") == DBStatus::OK);
    g_engine.bufferLogicalTruncate(database, "rows");
    assert(g_engine.commitTransaction() == DBStatus::OK);
    const auto truncated = LogicalChangeStore::instance().peek(slot, 0, 10);
    assert(truncated.batches.size() == 1 && truncated.batches[0].changes.size() == 1);
    assert(truncated.batches[0].changes[0].op == LogicalChange::Op::Truncate);
    assert(replication.dropReplicationSlot(slot));
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[PUBLICATION PREFLIGHT] four failures, healthy DML, earlier work and TRUNCATE passed\n";
}
