// Crash recovery test: verify redo/undo via WAL page images.

#include "access/BPTree.h"
#include "TableManage.h"
#include "Config.h"
#include "storage/DataFileHeader.h"
#include "storage/PageAllocator.h"
#include "storage/WAL.h"
#include <filesystem>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <thread>
#include <iostream>
#include <cassert>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>

dbms::Config g_config;

using namespace dbms;

static bool rowContains(const std::vector<std::string>& rows, const std::string& substring) {
    for (const auto& row : rows) {
        if (row.find(substring) != std::string::npos) return true;
    }
    return false;
}

static void setupTable(StorageEngine& engine, const std::string& dbname) {
    assert(engine.createDatabase(dbname) == DBStatus::OK);
    TableSchema tbl;
    tbl.tablename = "t";
    tbl.formatVersion = 2;
    tbl.append(makeIntColumn("id", false, 0, true));
    tbl.append(makeVarCharColumn("name", false, 50, false));
    assert(engine.createTable(dbname, tbl) == DBStatus::OK);
}

// A loser can allocate several heap pages that have not reached disk. WAL
// retains before-images for those pages in descending undo order; the test
// observes the actual on-disk extent and WAL block IDs before sending SIGKILL.
static void testUnflushedMultiPageUndo() {
    const std::string db = "redo_unflushed_gap_db";
    std::filesystem::remove_all(db);
    {
        StorageEngine engine;
        setupTable(engine, db);
        assert(engine.beginTransaction(db) == DBStatus::OK);
        assert(engine.insert(db, "t", {{"id", "9"}, {"name", "durable"}}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    struct Witness { char ready; uint32_t persistedPages; uint32_t memoryPages; uint32_t largestBefore; };
    int pipefd[2]{};
    assert(::pipe(pipefd) == 0);
    const pid_t child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        ::close(pipefd[0]);
        StorageEngine engine;
        // Let the first worker wait finish, then keep it asleep during this
        // bounded fixture. The test still verifies the actual durable extent.
        engine.setBackgroundIntervals(3600000, 3600000);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        assert(engine.beginTransaction(db) == DBStatus::OK);
        const uint64_t xid = engine.currentTxnId();
        for (int id = 1000; id < 1400; ++id) {
            assert(engine.insert(db, "t", {{"id", std::to_string(id)},
                                           {"name", std::string(40, 'x')}}) == DBStatus::OK);
        }
        auto* allocator = engine.getPageAllocator(db, "t");
        auto* wal = engine.getWAL(db);
        assert(allocator && wal && wal->XLogFlush(wal->currentWriteLsn()));
        DataFileHeader disk{};
        std::ifstream in(engine.dbPath(db) / "t.dt", std::ios::binary);
        assert(in.read(reinterpret_cast<char*>(&disk), sizeof(disk)));
        assert(disk.magic == DATA_FILE_MAGIC && disk.formatVersion == DATA_FILE_FORMAT_VERSION &&
               disk.headerChecksum == computeDataFileHeaderChecksum(disk));
        uint32_t highest = 0;
        for (Lsn pos = wal->earliestAvailableLsn(); pos < wal->currentWriteLsn();) {
            const auto record = wal->ReadRecord(pos);
            assert(record && record->header.xl_tot_len > 0);
            if (record->rmid() == RM_HEAP_ID && record->info() == XLOG_HEAP_PAGE_BEFORE &&
                record->header.xl_xid == xid && record->data.size() >= 9) {
                uint32_t nameLength = 0;
                std::memcpy(&nameLength, record->data.data(), sizeof(nameLength));
                if (nameLength == 1 && record->data[4] == 't') {
                    uint32_t block = 0;
                    std::memcpy(&block, record->data.data() + 5, sizeof(block));
                    highest = std::max(highest, block);
                }
            }
            pos += record->header.xl_tot_len;
        }
        Witness proof{'R', disk.numPages, allocator->numPages(), highest};
        assert(proof.largestBefore > proof.persistedPages && proof.largestBefore < proof.memoryPages);
        assert(::write(pipefd[1], &proof, sizeof(proof)) == static_cast<ssize_t>(sizeof(proof)));
        for (;;) ::pause();
    }
    ::close(pipefd[1]);
    Witness proof{};
    assert(::read(pipefd[0], &proof, sizeof(proof)) == static_cast<ssize_t>(sizeof(proof)));
    ::close(pipefd[0]);
    assert(proof.ready == 'R' && proof.largestBefore > proof.persistedPages);
    assert(::kill(child, SIGKILL) == 0);
    int status = 0;
    assert(::waitpid(child, &status, 0) == child && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);
    {
        StorageEngine engine;
        const auto rows = engine.query(db, "t", {}, {"id", "name"});
        assert(rows.size() == 1 && rowContains(rows, "durable"));
        int64_t rid = 0;
        auto* pk = engine.getPKIndex(db, "t");
        assert(pk && pk->search("9", rid) && !pk->search("1000", rid) && !pk->search("1399", rid));
        assert(engine.checkpoint(db));
    }
    std::filesystem::remove_all(db);
    std::filesystem::remove_all(db + ".txn_backup");
    std::cout << "[REDO CRASH] missing uncommitted heap pages recovered OK\n";
}

int main() {
    std::string dbname = "redo_crash_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    // Establish committed state before exercising a real hard crash.  The
    // old test only destroyed StorageEngine normally, which ran destructors
    // and did not cover the process-death path in the crash matrix.
    {
        StorageEngine engine;
        setupTable(engine, dbname);
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "9";
        vals["name"] = "committed-before-crash";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    int readyPipe[2]{};
    assert(::pipe(readyPipe) == 0);
    const pid_t child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        ::close(readyPipe[0]);
        char status = 'E';
        StorageEngine engine;
        if (engine.beginTransaction(dbname) == DBStatus::OK) {
            std::map<std::string, std::string> vals{
                {"id", "1"}, {"name", "uncommitted"}};
            if (engine.insert(dbname, "t", vals) == DBStatus::OK) {
                vals["id"] = "3";
                vals["name"] = "uncommitted-again";
                if (engine.insert(dbname, "t", vals) == DBStatus::OK) {
                    status = 'R';
                }
            }
        }
        const ssize_t statusBytes =
            ::write(readyPipe[1], &status, sizeof(status));
        if (statusBytes != static_cast<ssize_t>(sizeof(status))) {
            ::_exit(3);
        }
        if (status != 'R') ::_exit(2);
        for (;;) ::pause();
    }
    ::close(readyPipe[1]);
    char childReady = 0;
    assert(::read(readyPipe[0], &childReady, sizeof(childReady)) ==
           static_cast<ssize_t>(sizeof(childReady)));
    ::close(readyPipe[0]);
    assert(childReady == 'R');
    assert(::kill(child, SIGKILL) == 0);
    int childStatus = 0;
    assert(::waitpid(child, &childStatus, 0) == child);
    assert(WIFSIGNALED(childStatus) && WTERMSIG(childStatus) == SIGKILL);

    {
        StorageEngine engine;
        auto rows = engine.query(dbname, "t", {}, {"id", "name"});
        assert(rowContains(rows, "committed-before-crash"));
        assert(!rowContains(rows, "uncommitted"));
        assert(!rowContains(rows, "uncommitted-again"));
        int64_t rid = 0;
        auto* pk = engine.getPKIndex(dbname, "t");
        assert(pk != nullptr);
        assert(pk->search("9", rid));
        assert(!pk->search("1", rid));
        assert(!pk->search("3", rid));
        assert(engine.checkpoint(dbname));
        std::cout << "[REDO CRASH] uncommitted insert rolled back by recovery OK\n";
    }

    // Scenario 2: committed insert, crash, recover -> row visible.
    {
        StorageEngine engine;
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["id"] = "2";
        vals["name"] = "committed";
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    {
        StorageEngine engine;
        auto rows = engine.query(dbname, "t", {}, {"id", "name"});
        assert(rowContains(rows, "committed"));
        int64_t rid = 0;
        auto* pk = engine.getPKIndex(dbname, "t");
        assert(pk != nullptr);
        assert(pk->search("2", rid));
        std::cout << "[REDO CRASH] committed insert recovered OK\n";
    }

    // Scenario 3: committed delete, crash, recover -> row gone.
    {
        StorageEngine engine;
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.remove(dbname, "t", {"id = '2'"}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
    }

    {
        StorageEngine engine;
        auto rows = engine.query(dbname, "t", {}, {"id", "name"});
        assert(!rowContains(rows, "committed"));
        int64_t rid = 0;
        auto* pk = engine.getPKIndex(dbname, "t");
        assert(pk != nullptr);
        assert(!pk->search("2", rid));
        std::cout << "[REDO CRASH] committed delete recovered OK\n";
    }

    testUnflushedMultiPageUndo();

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[REDO CRASH] all passed\n";
    return 0;
}
