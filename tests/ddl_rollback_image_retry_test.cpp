#include "commands/DdlTransaction.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

int main() {
    using namespace dbms;
    const auto database = testDbPath("ddl_rollback_image_retry");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "rows";
    table.append(makeIntColumn("id", false, 2, true));
    assert(g_engine.createTable(database, table) == DBStatus::OK);
    assert(g_engine.insert(database, "rows", {{"id", "7"}}) == DBStatus::OK);
    Session session;
    session.currentDB = database;
    DdlTransaction transaction(session);
    assert(transaction.enableSnapshotRollback() && transaction.begin());
    const fs::path image = database + ".txn_backup." + std::to_string(g_engine.currentTxnId());
    const fs::path saved = image.string() + ".saved";
    assert(fs::is_directory(image));
    transaction.markSnapshotDirty();
    assert(g_engine.alterTableAddColumn(database, "rows", makeIntColumn("added", true, 2)) == DBStatus::OK);
    assert(g_engine.getTableSchema(database, "rows").len == 2);

    // Temporarily unavailable, otherwise valid normal backup directory.
    // Restore the very same image for the explicit healthy retry.
    fs::rename(image, saved);
    transaction.rollback();
    assert(transaction.isActive() && g_engine.inTransaction());
    assert(fs::is_directory(saved));
    assert(g_engine.beginTransaction(database) == DBStatus::IO_ERROR);
    fs::rename(saved, image);
    transaction.rollback();
    assert(!transaction.isActive() && !g_engine.inTransaction());
    assert(!fs::exists(image));
    assert(g_engine.getTableSchema(database, "rows").len == 1);
    assert(g_engine.query(database, "rows", {}, {"id"}) == std::vector<std::string>{"7 "});

    bool healthy = false;
    int calls = 0;
    bool primaryCaught = false;
    try {
        DdlTransaction unwinding(session);
        assert(unwinding.begin());
        // This undo precedes the captured image and must remain available
        // even after physical restoration succeeds but a later undo fails.
        g_engine.registerDdlUndo([&] {
            ++calls;
            if (!healthy) throw std::runtime_error("secondary undo error");
            return true;
        });
        assert(unwinding.enableSnapshotRollback());
        unwinding.markSnapshotDirty();
        assert(g_engine.alterTableAddColumn(database, "rows", makeIntColumn("again", true, 2)) == DBStatus::OK);
        throw std::runtime_error("primary statement error");
    } catch (const std::runtime_error& error) {
        primaryCaught = std::string(error.what()) == "primary statement error";
    }
    assert(primaryCaught && g_engine.inTransaction() && g_engine.hasTransactionBackup());
    const fs::path laterImage = database + ".txn_backup." + std::to_string(g_engine.currentTxnId());
    assert(fs::is_directory(laterImage));
    healthy = true;
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    // The wrapper has unwound, but the engine still owns the image and undo.
    g_engine.discardTransactionBackup(database);
    assert(calls == 2 && !fs::exists(laterImage));
    assert(g_engine.getTableSchema(database, "rows").len == 1);
    assert(g_engine.query(database, "rows", {}, {"id"}) == std::vector<std::string>{"7 "});
    assert(g_engine.insert(database, "rows", {{"id", "8"}}) == DBStatus::OK);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "[DDL IMAGE RETRY] missing image, later undo error, preserved primary, healthy retry OK\n";
}
