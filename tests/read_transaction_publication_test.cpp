// test_link_flags: -Wl,--wrap=fsync
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "storage/CommitLog.h"
#include "storage/WAL.h"
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
extern "C" int __real_fsync(int);
namespace {
std::atomic<bool> observeSync{false};
std::atomic<unsigned> syncCalls{0};
}
extern "C" int __wrap_fsync(int descriptor) {
    if (observeSync.load()) ++syncCalls;
    return __real_fsync(descriptor); // Count real successful/failing calls; never suppress one.
}
namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Rows = std::map<std::string, StorageEngine::SqlRow>;
const std::string database = "__t_read_publication";
unsigned checks = 0, failures = 0;
void need(bool value, const char* why) { if (!value) throw std::runtime_error(why); }
void check(bool value, const char* why) {
    ++checks; if (!value) { ++failures; std::cout << "READ_COMMIT_FAIL " << why << '\n'; }
}
std::string body(char character) { return std::string(70000, character) + " é中🙂 \n\t"; }
Rows initial() {
    return {{"1", {{"id", "1"}, {"body", body('a')}, {"note", "first"}}},
            {"2", {{"id", "2"}, {"body", "é中🙂"}, {"note", Cell{}}}},
            {"3", {{"id", "3"}, {"body", Cell{}}, {"note", "nullable"}}},
            {"4", {{"id", "4"}, {"body", ""}, {"note", Cell{}}}},
            {"5", {{"id", "5"}, {"body", "NULL"}, {"note", "literal"}}},
            {"6", {{"id", "6"}, {"body", "__TOAST__1"}, {"note", Cell{}}}}};
}
Rows finalRows() {
    auto rows = initial(); rows["1"]["body"] = body('b'); rows["1"]["note"] = Cell{};
    rows["2"]["note"] = "peer"; return rows;
}
std::string bytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary); need(bool(in), "actual regular file opens");
    return std::string(std::istreambuf_iterator<char>(in), {});
}
std::map<std::string, std::string> clogImage() {
    std::map<std::string, std::string> result; const auto root = g_engine.dbPath(database) / "pg_xact";
    for (const auto& entry : std::filesystem::directory_iterator(root))
        if (entry.is_regular_file() && entry.path().filename().string().front() != '.')
            result.emplace(entry.path().filename().string(), bytes(entry.path()));
    return result;
}
Rows scan(const std::string& phase) {
    const auto schema = g_engine.getTableSchema(database, "items"); need(schema.len == 3, "complete actual schema");
    TableScanOp cursor(&g_engine, database, "items"); need(cursor.open(), "actual scan opens");
    Rows rows; std::string data;
    while (cursor.next(data)) {
        StorageEngine::SqlRow row;
        for (size_t i = 0; i < schema.len; ++i)
            row[schema.cols[i].dataName] = cursor.lastColumnIsNull(i) ? Cell{} :
                Cell{g_engine.extractColumnValue(data, schema, i, database)};
        need(row["id"].has_value(), "actual nonNULL ID"); auto key = *row["id"];
        need(rows.emplace(key, std::move(row)).second, "actual complete unique row multiplicity");
    }
    need(!cursor.hasError(), "scan complete without error"); cursor.close();
    std::ofstream out("observed-" + phase + ".rows", std::ios::binary);
    const auto number = [&](uint32_t n) { out.write(reinterpret_cast<const char*>(&n), sizeof(n)); };
    const auto value = [&](const std::string& v) { number(v.size()); out.write(v.data(), v.size()); };
    number(rows.size());
    for (const auto& [id, row] : rows) {
        value(id); number(row.size());
        for (const auto& [name, cell] : row) { value(name); if (cell) value(*cell); else number(UINT32_MAX); }
    }
    out.flush(); need(bool(out), "whole observed values independently recoverable"); return rows;
}
void readCommit(const char* label, IsolationLevel isolation, bool readOnly, bool readRows) {
    need(g_engine.setIsolationLevel(isolation), "actual selected isolation");
    need(g_engine.beginTransaction(database) == DBStatus::OK, "actual native BEGIN");
    if (readOnly) need(g_engine.setReadOnly(true), "actual READ ONLY mode");
    if (readRows) check(scan(label) == initial(), "every actual read value and NULL exact");
    auto* wal = g_engine.getWAL(database); need(wal != nullptr, "retained actual WAL");
    const auto xid = g_engine.currentTxnId(); const auto before = wal->currentWriteLsn(); const auto clog = clogImage();
    syncCalls = 0; observeSync = true; const auto status = g_engine.commitTransaction(); observeSync = false;
    const auto after = wal->currentWriteLsn(); const bool unchanged = clogImage() == clog;
    check(status == DBStatus::OK && !g_engine.inTransaction(), "actual successful read terminal");
    check(before == after, "no COMMIT WAL for a transaction with no persistent effects");
    check(unchanged, "no CLOG file replacement for an unused transaction ID");
    check(syncCalls == 0, "no real fsync at a terminal with nothing to publish");
    std::cout << "READ_COMMIT case=" << label << " xid=" << xid << " wal_before=" << before
              << " wal_after=" << after << " clog_unchanged=" << unchanged
              << " real_fsync=" << syncCalls << " status=" << int(status) << '\n';
}
void setup() {
    need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "actual own database");
    readCommit("zero-wal", IsolationLevel::READ_COMMITTED, false, false);
    TableSchema schema; schema.tablename = "items"; schema.append(makeIntColumn("id", false, 4, true));
    schema.append(makeTextColumn("body", true)); schema.append(makeVarCharColumn("note", true, 64));
    need(g_engine.createTable(database, schema) == DBStatus::OK, "actual heap");
    need(g_engine.beginTransaction(database) == DBStatus::OK, "seed BEGIN");
    for (const auto& [id, row] : initial()) need(g_engine.insertRow(database, "items", row) == DBStatus::OK, "actual seed row");
    need(g_engine.commitTransaction() == DBStatus::OK, "durable seed COMMIT");
    // Native UNLOGGED DDL and sequence writes can change files without heap
    // WAL. They must retain the conservative publication path as well.
    need(g_engine.beginTransaction(database) == DBStatus::OK, "native DDL BEGIN");
    TableSchema unlogged; unlogged.tablename = "unlogged_aux"; unlogged.isUnlogged = true;
    unlogged.append(makeIntColumn("id", false, 4, true));
    need(g_engine.createTable(database, unlogged) == DBStatus::OK, "actual native UNLOGGED DDL");
    auto* ddlWal = g_engine.getWAL(database); const auto ddlStart = ddlWal->currentWriteLsn();
    syncCalls = 0; observeSync = true; const auto ddlStatus = g_engine.commitTransaction(); observeSync = false;
    check(ddlStatus == DBStatus::OK && ddlWal->currentWriteLsn() > ddlStart && syncCalls > 0,
          "non-WAL native DDL retains durable terminal");
    need(g_engine.createSequence(database, "counter", 10, 1) == DBStatus::OK, "actual native sequence");
    for (int mode = 0; mode < 2; ++mode) {
        need(g_engine.beginTransaction(database) == DBStatus::OK, "sequence BEGIN");
        check((mode ? g_engine.setval(database, "counter", 50, false) :
                      g_engine.nextval(database, "counter")) == (mode ? 50 : 10), "actual sequence effect");
        auto* sequenceWal = g_engine.getWAL(database); const auto sequenceStart = sequenceWal->currentWriteLsn();
        syncCalls = 0; observeSync = true; const auto sequenceStatus = g_engine.commitTransaction(); observeSync = false;
        check(sequenceStatus == DBStatus::OK && sequenceWal->currentWriteLsn() > sequenceStart && syncCalls > 0,
              "sequence side effect retains durable terminal");
    }
    readCommit("plain", IsolationLevel::READ_COMMITTED, false, true);
    readCommit("readonly", IsolationLevel::REPEATABLE_READ, true, true);
    readCommit("serializable", IsolationLevel::SERIALIZABLE, true, true);
    readCommit("empty", IsolationLevel::READ_COMMITTED, false, false);
    need(g_engine.setIsolationLevel(IsolationLevel::REPEATABLE_READ), "restore real repeatable snapshot");
    need(g_engine.beginTransaction(database) == DBStatus::OK, "read then write BEGIN");
    check(scan("before-write") == initial(), "actual pre-write full snapshot");
    need(g_engine.updateRows(database, "items", {{"body", body('b')}, {"note", Cell{}}}, {"=id 1"}) == DBStatus::OK,
         "actual mutation after reading");
    auto* wal = g_engine.getWAL(database); const auto xid = g_engine.currentTxnId(); const auto start = wal->currentWriteLsn();
    syncCalls = 0; observeSync = true; const auto status = g_engine.commitTransaction(); observeSync = false;
    check(status == DBStatus::OK && wal->currentWriteLsn() > start && syncCalls > 0,
          "writing transaction retains real durable COMMIT and sync");
    check(g_engine.getCommitLog(database)->getStatus(xid) == CommitLog::Status::Committed, "writer CLOG committed");
    std::cout << "WRITE_COMMIT xid=" << xid << " wal_before=" << start << " wal_after=" << wal->currentWriteLsn()
              << " real_fsync=" << syncCalls << '\n';
    auto expected = initial(); expected["1"]["body"] = body('b'); expected["1"]["note"] = Cell{};
    need(g_engine.beginTransaction(database) == DBStatus::OK, "actual reader overlaps a peer writer");
    check(scan("peer-before") == expected, "reader initial snapshot exact");
    const auto readerXid = g_engine.currentTxnId(); std::exception_ptr peerFailure;
    std::thread peer([&] {
        try {
            need(g_engine.beginTransaction(database) == DBStatus::OK, "peer actual BEGIN");
            need(g_engine.updateRows(database, "items", {{"note", "peer"}}, {"=id 2"}) == DBStatus::OK, "peer real DML");
            need(g_engine.commitTransaction() == DBStatus::OK, "peer real durable COMMIT");
        } catch (...) { peerFailure = std::current_exception(); }
    });
    peer.join(); if (peerFailure) std::rethrow_exception(peerFailure);
    check(scan("peer-after") == expected, "peer COMMIT cannot change retained repeatable read");
    const auto peerEnd = wal->currentWriteLsn(); need(g_engine.commitTransaction() == DBStatus::OK, "reader terminal after peer");
    check(wal->currentWriteLsn() > peerEnd, "changed shared WAL takes original publication fallback");
    std::cout << "PEER_READER_COMMIT xid=" << readerXid << " wal_before=" << peerEnd
              << " wal_after=" << wal->currentWriteLsn() << '\n';
    check(scan("final") == finalRows(), "all committed data and NULLs exact");
}
int child(const std::string& self, const char* mode) {
    std::cout.flush(); const pid_t pid = fork(); need(pid >= 0, "fork own fresh child");
    if (!pid) { execl(self.c_str(), self.c_str(), mode, static_cast<char*>(nullptr)); _exit(127); }
    int status; pid_t waited; do { waited = waitpid(pid, &status, 0); } while (waited < 0 && errno == EINTR);
    need(waited == pid, "exact child reaped"); const int code = WIFEXITED(status) ? WEXITSTATUS(status) : -WTERMSIG(status);
    std::cout << "READ_COMMIT_CHILD mode=" << mode << " pid=" << pid << " actual_exit=" << code
              << " raw_wait=" << status << '\n'; return code;
}
}
int main(int argc, char** argv) {
    try {
        g_engine.setBackgroundIntervals(60000, 60000);
        if (argc == 2) {
            if (std::string(argv[1]) == "--setup") setup();
            else {
                need(std::string(argv[1]) == "--cold", "known child mode");
                check(scan("cold") == finalRows(), "fresh process complete values and NULLs");
                check(g_engine.getTableSchema(database, "unlogged_aux").isUnlogged, "fresh process native UNLOGGED schema");
                check(g_engine.nextval(database, "counter") == 50, "fresh process full sequence state");
            }
            std::cout << "READ_COMMIT_CHECKED=" << checks << " FAILED=" << failures << '\n'; return failures ? 1 : 0;
        }
        need(argc == 1, "known arguments"); const auto self = std::filesystem::canonical("/proc/self/exe").string();
        const int warm = child(self, "--setup"); const int cold = child(self, "--cold");
        std::cout << "READ_COMMIT_COMPLETE warm=" << warm << " cold=" << cold
                  << " full_suite=false real_fsync_passthrough=true\n"; return warm || cold ? 1 : 0;
    } catch (const std::exception& error) { std::cerr << "READ_COMMIT_FATAL " << error.what() << '\n'; return 2; }
}
