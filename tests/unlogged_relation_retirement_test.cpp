#include "commands/TableManage.h"
#include "storage/HeapWalIdentity.h"
#include "storage/PageAllocator.h"
#include "access/IndexFileUtil.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <set>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;
static const std::string database = "__t_unlogged_retirement";

static TableSchema schema(bool unlogged = false) {
    TableSchema result;
    result.tablename = "items";
    result.isUnlogged = unlogged;
    result.append(makeIntColumn("id", false, 4, true));
    return result;
}

static void writer(const std::string& scenario) {
    StorageEngine engine;
    assert(engine.createDatabase(database) == DBStatus::OK);
    const bool bornUnlogged = scenario == "born-unlogged";
    assert(engine.createTable(database, schema(bornUnlogged)) == DBStatus::OK);
    WALManager* wal = engine.getWAL(database);
    assert(wal);
    if (bornUnlogged) assert(wal->currentWriteLsn() == 0);
    assert(engine.insert(database, "items", {{"id", "9"}}) == DBStatus::OK);
    const auto identity = engine.getTableSchema(database, "items").physicalRelationId;
    assert(identity != 0);
    const bool transactional = scenario == "commit" || scenario == "abort";
    if (transactional) {
        assert(engine.beginTransaction(database) == DBStatus::OK);
        engine.preserveTransactionBackupOnRollback(true);
        assert(engine.createTransactionBackup());
        engine.restoreTransactionBackupBeforeRowUndo(true);
        engine.markTransactionBackupDirty();
    }
    if (!bornUnlogged)
        assert(engine.alterTableSetLogged(database, "items", false) == DBStatus::OK);
    assert(engine.getTableSchema(database, "items").isUnlogged);
    if (scenario == "intent-only") {
        const auto intent = wal->XLogInsert(RM_SMGR_ID, XLOG_SMGR_RELATION_RETIRE,
            0, heap_wal_identity::event(identity, "items"));
        assert(intent != INVALID_LSN && wal->XLogFlush(intent));
        assert(std::filesystem::remove(std::filesystem::path(database) / "items.dt"));
        assert(std::filesystem::remove(std::filesystem::path(database) / "items.stc"));
        assert(index_file::writeAtomically(std::filesystem::path(database) / "tlist.lst", ""));
        _exit(0);
    }
    assert(engine.dropTable(database, "items") == DBStatus::OK);
    unsigned intents = 0, completions = 0, records = 0, commits = 0;
    const uint64_t xid = transactional ? engine.currentTxnId() : 0;
    for (Lsn lsn = wal->earliestAvailableLsn();;) {
        const auto record = wal->ReadRecord(lsn);
        if (!record) break;
        ++records;
        heap_wal_identity::Event event;
        if (heap_wal_identity::event(*record, event) && event.relationId == identity &&
            (record->info() == XLOG_SMGR_RELATION_RETIRE ||
             record->info() == XLOG_SMGR_RELATION_RETIRE_COMPLETE)) {
            assert(event.name == "items" && record->header.xl_xid == xid);
            intents += record->info() == XLOG_SMGR_RELATION_RETIRE;
            completions += record->info() == XLOG_SMGR_RELATION_RETIRE_COMPLETE;
        }
        if (bornUnlogged) {
            // INSERT owns a legitimate implicit transaction. Its terminal
            // COMMIT contains no row/index data and is distinct from the
            // lifecycle pair; UNLOGGED values/birth remain absent from WAL.
            if (record->rmid() == RM_XACT_ID) {
                assert(record->info() == XLOG_XACT_COMMIT);
                ++commits;
            } else {
                assert(record->rmid() == RM_SMGR_ID);
                assert(record->info() == XLOG_SMGR_RELATION_RETIRE ||
                       record->info() == XLOG_SMGR_RELATION_RETIRE_COMPLETE);
            }
        }
        assert(record->header.xl_tot_len > 0);
        lsn += record->header.xl_tot_len;
    }
    std::cerr << "[UNLOGGED RETIREMENT] " << scenario << " intent=" << intents
              << " completion=" << completions << '\n';
    assert(intents == 1 && completions == 1);
    if (bornUnlogged) assert(records == 3 && commits == 1);
    if (scenario == "reuse" || transactional) {
        assert(engine.createTable(database, schema()) == DBStatus::OK);
        assert(engine.getTableSchema(database, "items").physicalRelationId > identity);
        assert(engine.insert(database, "items", {{"id", "100"}}) == DBStatus::OK);
        assert(engine.getPageAllocator(database, "items")->flush());
        if (scenario == "abort") _exit(0);
        if (scenario == "commit") assert(engine.commitTransaction() == DBStatus::OK);
        assert(engine.beginTransaction(database) == DBStatus::OK);
        assert(engine.insert(database, "items", {{"id", "101"}}) == DBStatus::OK);
        assert(engine.getPageAllocator(database, "items")->flush());
    }
    _exit(0);
}

static void scenario(const char* executable, const std::string& name) {
    const auto child = fork();
    assert(child >= 0);
    if (child == 0) {
        execl(executable, executable, "--writer", name.c_str(), static_cast<char*>(nullptr));
        _exit(127);
    }
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    if (name == "intent-only") {
        bool failedClosed = false;
        try { StorageEngine recovered; }
        catch (const std::runtime_error&) { failedClosed = true; }
        assert(failedClosed);
        assert(!std::filesystem::exists(std::filesystem::path(database) / "items.dt"));
        return;
    }
    StorageEngine recovered;
    assert(recovered.recoverAllDatabases());
    if (name == "drop" || name == "born-unlogged") {
        assert(!recovered.tableExists(database, "items"));
    } else {
        const auto rows = recovered.query(database, "items", {}, {"id"});
        const std::set<std::string> expected = name == "abort"
            ? std::set<std::string>{"9 "} : std::set<std::string>{"100 "};
        assert(rows.size() == 1 && std::set<std::string>(rows.begin(), rows.end()) == expected);
        assert(!recovered.getTableSchema(database, "items").isUnlogged);
    }
}

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--writer") writer(argv[2]);
    char executable[4096];
    const auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    assert(length > 0);
    executable[length] = '\0';
    // Keep all scenarios in one warmed parent: fresh-process controls must
    // not conceal a durable XID or recovery-horizon regression.
    for (const char* name : {"drop", "reuse", "commit", "abort", "born-unlogged", "intent-only"}) {
        assert(std::filesystem::create_directory(name));
        const auto original = std::filesystem::current_path();
        std::filesystem::current_path(name);
        scenario(executable, name);
        std::filesystem::current_path(original);
        std::cout << "[UNLOGGED RETIREMENT] " << name << " passed\n";
    }
}
