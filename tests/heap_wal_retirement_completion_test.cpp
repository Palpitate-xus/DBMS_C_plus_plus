#include "commands/TableManage.h"
#include "storage/HeapWalIdentity.h"
#include "access/IndexFileUtil.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;
static const std::string database = "__t_heap_retirement_completion";

static void interruptedDrop(const std::string& scenario) {
    StorageEngine engine;
    assert(engine.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, table) == DBStatus::OK);
    assert(engine.insert(database, "items", {{"id", "99"}}) == DBStatus::OK);
    const auto identity = engine.getTableSchema(database, "items").physicalRelationId;
    assert(identity != 0);
    if (scenario == "complete") {
        assert(engine.dropTable(database, "items") == DBStatus::OK);
        WALManager* wal = engine.getWAL(database);
        assert(wal);
        unsigned intents = 0, completions = 0;
        for (Lsn lsn = wal->earliestAvailableLsn();;) {
            const auto record = wal->ReadRecord(lsn);
            if (!record) break;
            heap_wal_identity::Event event;
            if (heap_wal_identity::event(*record, event) && event.relationId == identity) {
                assert(event.name == "items" && record->header.xl_xid == 0);
                intents += record->info() == XLOG_SMGR_RELATION_RETIRE;
                completions += record->info() == XLOG_SMGR_RELATION_RETIRE_COMPLETE;
            }
            assert(record->header.xl_tot_len > 0);
            lsn += record->header.xl_tot_len;
        }
        assert(intents == 1 && completions == 1);
        _exit(0);
    }
    const bool abortedOwner = scenario == "abort" || scenario == "abort-catalog" ||
                              scenario == "abort-complete";
    if (abortedOwner) {
        assert(engine.beginTransaction(database) == DBStatus::OK);
        engine.preserveTransactionBackupOnRollback(true);
        assert(engine.createTransactionBackup());
        engine.restoreTransactionBackupBeforeRowUndo(true);
        engine.markTransactionBackupDirty();
    }
    // Model the exact existing public DROP prefix: durable slot-0x21
    // lifecycle WAL, physical unlink, but no completed metadata publication.
    // No successful DROP or terminal COMMIT is invented by this fault test.
    WALManager* wal = engine.getWAL(database);
    assert(wal);
    const uint64_t xid = abortedOwner ? engine.currentTxnId() : 0;
    if (scenario != "completion-only") {
        const Lsn intent = wal->XLogInsert(RM_SMGR_ID, XLOG_SMGR_RELATION_RETIRE,
            xid, heap_wal_identity::event(identity, "items"));
        assert(intent != INVALID_LSN && wal->XLogFlush(intent));
    }
    assert(std::filesystem::remove(std::filesystem::path(database) / "items.dt"));
    assert(std::filesystem::remove(std::filesystem::path(database) / "items.stc"));
    if (scenario != "intent" && scenario != "abort") {
        // The last ordinary DROP metadata publication is independently
        // durable, but intent WAL alone still cannot prove that the complete
        // operation has crossed its success/commit boundary.
        assert(index_file::writeAtomically(
            std::filesystem::path(database) / "tlist.lst", ""));
    }
    if (scenario == "completion-only" || scenario == "completion-wrong-name" ||
        scenario == "completion-wrong-xid" || scenario == "abort-complete") {
        const Lsn completion = wal->XLogInsert(RM_SMGR_ID,
            XLOG_SMGR_RELATION_RETIRE_COMPLETE,
            scenario == "completion-wrong-xid" ? xid + 1 : xid,
            heap_wal_identity::event(identity,
                scenario == "completion-wrong-name" ? "other" : "items"));
        assert(completion != INVALID_LSN && wal->XLogFlush(completion));
    }
    _exit(0);
}

static void runScenario(const char* executable, const std::string& scenario) {
    const auto child = fork();
    assert(child >= 0);
    if (child == 0) {
        execl(executable, executable, "--writer", scenario.c_str(), static_cast<char*>(nullptr));
        _exit(127);
    }
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    const bool abortedOwner = scenario == "abort" || scenario == "abort-catalog" ||
                              scenario == "abort-complete";
    if (scenario == "complete") {
        StorageEngine recovered;
        assert(!recovered.tableExists(database, "items"));
    } else if (!abortedOwner) {
        bool failedClosed = false;
        try {
            StorageEngine recovered;
        } catch (const std::runtime_error&) {
            failedClosed = true;
        }
        std::cerr << "[RETIREMENT COMPLETION] " << scenario << " failed_closed="
                  << failedClosed << '\n';
        assert(failedClosed);
        assert(!std::filesystem::exists(std::filesystem::path(database) / "items.dt"));
        assert(!std::filesystem::exists(std::filesystem::path(database) / "items.stc"));
    } else {
        StorageEngine recovered;
        assert(recovered.tableExists(database, "items"));
        assert(recovered.query(database, "items", {}, {"id"}) ==
               std::vector<std::string>{"99 "});
    }
}

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--writer") interruptedDrop(argv[2]);
    char executable[4096];
    const auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    assert(length > 0);
    executable[length] = '\0';
    if (argc == 3 && std::string(argv[1]) == "--scenario") {
        runScenario(executable, argv[2]);
        return 0;
    }
    for (const char* scenario : {"intent", "intent-catalog", "completion-only",
             "completion-wrong-name", "completion-wrong-xid", "complete",
             "abort", "abort-catalog", "abort-complete"}) {
        assert(std::filesystem::create_directory(scenario));
        const auto child = fork();
        assert(child >= 0);
        if (child == 0) {
            assert(chdir(scenario) == 0);
            execl(executable, executable, "--scenario", scenario, static_cast<char*>(nullptr));
            _exit(127);
        }
        int status = 0;
        assert(waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
    std::cout << "[RETIREMENT COMPLETION] incomplete intent protects data, actual aborted owner restores rows\n";
}
