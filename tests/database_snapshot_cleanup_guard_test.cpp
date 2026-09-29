#include "TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>

int main() {
    namespace fs = std::filesystem;
    const std::string source = testDbPath("snapshot_cleanup_src");
    fs::remove_all(source);
    dbms::StorageEngine engine;
    assert(engine.createDatabase(source) == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "items";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 0, true));
    assert(engine.createTable(source, table) == dbms::DBStatus::OK);

    assert(engine.beginTransaction(source) == dbms::DBStatus::OK);
    assert(engine.insert(source, "items", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    const auto commitXid = engine.currentTxnId();
    assert(engine.prepareTransaction("cleanup_guard_commit") ==
           dbms::DBStatus::OK);
    const std::string commitAlias = source + ".txn_backup." +
                                    std::to_string(commitXid);
    assert(engine.createDatabase(commitAlias) == dbms::DBStatus::OK);
    std::ofstream(commitAlias + "/keep.txt") << "another database";
    assert(engine.commitPrepared("cleanup_guard_commit") ==
           dbms::DBStatus::OK);
    assert(engine.databaseExists(commitAlias));
    assert(fs::exists(commitAlias + "/keep.txt"));

    assert(engine.beginTransaction(source) == dbms::DBStatus::OK);
    assert(engine.insert(source, "items", {{"id", "2"}}) ==
           dbms::DBStatus::OK);
    const auto rollbackXid = engine.currentTxnId();
    assert(engine.prepareTransaction("cleanup_guard_rollback") ==
           dbms::DBStatus::OK);
    const std::string rollbackAlias = source + ".txn_backup." +
                                      std::to_string(rollbackXid);
    assert(engine.createDatabase(rollbackAlias) == dbms::DBStatus::OK);
    std::ofstream(rollbackAlias + "/keep.txt") << "another database";
    assert(engine.rollbackPrepared("cleanup_guard_rollback") ==
           dbms::DBStatus::OK);
    assert(engine.databaseExists(rollbackAlias));
    assert(fs::exists(rollbackAlias + "/keep.txt"));

    assert(engine.beginTransaction(source, true) == dbms::DBStatus::OK);
    const std::string statementAlias = source + ".ddl_statement_backup." +
        std::to_string(engine.currentTxnId()) + ".fake";
    assert(engine.createDatabase(statementAlias) == dbms::DBStatus::OK);
    std::ofstream(statementAlias + "/keep.txt") << "another database";
    engine.discardDdlStatementBackup(statementAlias);
    assert(engine.databaseExists(statementAlias));
    assert(fs::exists(statementAlias + "/keep.txt"));
    assert(engine.rollbackTransaction() == dbms::DBStatus::OK);

    assert(engine.dropDatabase(statementAlias) == dbms::DBStatus::OK);
    assert(engine.dropDatabase(rollbackAlias) == dbms::DBStatus::OK);
    assert(engine.dropDatabase(commitAlias) == dbms::DBStatus::OK);
    assert(engine.dropDatabase(source) == dbms::DBStatus::OK);
}
