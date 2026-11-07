#include "commands/TableManage.h"
#include "common/DbError.h"
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>
using namespace dbms;
namespace {
unsigned checked = 0;
const std::string database = "__t_identity_codec";
void need(bool good, const char* reason) { ++checked; if (!good) throw std::runtime_error(reason); }
std::string bytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary); need(bool(in), "actual schema opens");
    return std::string(std::istreambuf_iterator<char>(in), {});
}
void store(const std::filesystem::path& path, const std::string& raw) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc); out.write(raw.data(), raw.size());
    out.flush(); need(bool(out), "whole independent schema variation written");
}
void word(std::string& raw, size_t offset, uint32_t value) {
    need(offset + sizeof(value) <= raw.size(), "actual codec uint32 boundary");
    std::memcpy(raw.data() + offset, &value, sizeof(value));
}
struct ColdResult {
    int actualExit = 0;
    std::string diagnostic;
    bool matches(unsigned expected) const {
        if (actualExit == 0) return true; // Child checked the expected schema result.
        // The linked global production engine may validate this database
        // before main(). A corrupt schema must also be allowed to abort
        // startup. Retain that real signal and exact diagnostic; it is not
        // a successful engine startup or a normal cold-read exit.
        return expected == 0 && actualExit == -SIGABRT &&
            diagnostic.find("WAL crash recovery failed; startup aborted to protect data") != std::string::npos;
    }
};
ColdResult cold(const std::string& self, unsigned expected) {
    int errorPipe[2]; need(pipe(errorPipe) == 0, "own cold diagnostic pipe");
    std::cout.flush(); std::cerr.flush(); const pid_t pid = fork(); need(pid >= 0, "fork own new cold process");
    const auto text = std::to_string(expected);
    if (!pid) {
        close(errorPipe[0]);
        if (dup2(errorPipe[1], STDERR_FILENO) < 0) _exit(126);
        close(errorPipe[1]);
        execl(self.c_str(), self.c_str(), "--read", text.c_str(), static_cast<char*>(nullptr)); _exit(127);
    }
    close(errorPipe[1]);
    std::string diagnostic; char chunk[4096]; ssize_t count;
    while ((count = read(errorPipe[0], chunk, sizeof(chunk))) != 0) {
        if (count < 0) { if (errno == EINTR) continue; throw std::runtime_error("cold diagnostic read failed"); }
        diagnostic.append(chunk, static_cast<size_t>(count));
    }
    close(errorPipe[0]);
    int status; pid_t got; do { got = waitpid(pid, &status, 0); } while (got < 0 && errno == EINTR);
    need(got == pid, "exact child reaped"); const int code = WIFEXITED(status) ? WEXITSTATUS(status) : -WTERMSIG(status);
    std::cout << "IDENTITY_CODEC_COLD pid=" << pid << " expected_oid=" << expected
              << " actual_exit=" << code << " raw_wait=" << status << '\n';
    std::cerr << diagnostic;
    return {code, std::move(diagnostic)};
}
}
int main(int argc, char** argv) {
    try {
        if (argc == 3) {
            need(std::string(argv[1]) == "--read", "known child mode");
            const unsigned expected = static_cast<unsigned>(std::stoul(argv[2]));
            StorageEngine engine; engine.setBackgroundIntervals(60000, 60000);
            TableSchema table;
            try { table = engine.getTableSchema(database, "bound_codec"); }
            catch (const DbError& error) {
                need(expected == 0 && error.sqlState() == "XX001", "only schema corruption is accepted rejection");
                return 0;
            }
            if (expected == 0) need(table.len == 0, "corrupt binding metadata fails closed");
            else {
                need(table.len == 2 && table.cols[0].identitySequenceOid == expected, "actual cold OID binding");
                need(table.cols[0].identityKind == 'a' && table.cols[0].isAutoIncrement && table.cols[0].isPrimaryKey,
                     "all actual old physical flags retained");
                need(table.cols[1].identitySequenceOid == 0, "nonidentity column stays unbound");
            }
            return 0;
        }
        need(argc == 1, "known arguments"); std::filesystem::path schemaPath;
        {
            StorageEngine engine; engine.setBackgroundIntervals(60000, 60000);
            need(engine.createDatabase(database, "utf8") == DBStatus::OK, "own codec-only database");
            TableSchema legacy; legacy.tablename = "legacy_codec";
            auto column = makeIntColumn("id", false, 4, true); column.identityKind = 'a'; column.isAutoIncrement = true;
            legacy.append(column); legacy.append(makeTextColumn("note", true));
            need(engine.createTable(database, legacy) == DBStatus::OK, "actual legacy schema persisted");
            need(bytes((engine.dbPath(database) / (legacy.tablename + ".stc"))).find("ISQ1") == std::string::npos,
                 "legacy bytes do not invent a named sequence");
            TableSchema bound = legacy; bound.tablename = "bound_codec"; bound.cols[0].identitySequenceOid = 14001;
            // Synthetic identity tests only the persisted codec. It is not a
            // materialized sequence or a SQL CREATE/INSERT acceptance claim.
            need(engine.createTable(database, bound) == DBStatus::OK, "actual codec schema persisted");
            schemaPath = (engine.dbPath(database) / (bound.tablename + ".stc"));
        }
        const auto original = bytes(schemaPath); const auto at = original.rfind("ISQ1");
        need(at != std::string::npos && original.size() == at + 6 + 8, "one complete bounded trailer");
        const auto self = std::filesystem::canonical("/proc/self/exe").string();
        need(cold(self, 14001).matches(14001), "complete binding cold roundtrip");
        for (size_t cut = at; cut < original.size(); ++cut) {
            store(schemaPath, original.substr(0, cut)); need(cold(self, 0).matches(0), "every partial trailer is rejected cold");
        }
        std::vector<std::string> bad;
        auto raw = original; raw[at] = 'X'; bad.push_back(raw);
        raw = original; raw[at + 4] = 3; bad.push_back(raw);
        raw = original; word(raw, at + 6, 0); bad.push_back(raw);
        raw = original; word(raw, at + 10, 14002); bad.push_back(raw);
        bad.push_back(original + "tail");
        for (const auto& raw : bad) { store(schemaPath, raw); need(cold(self, 0).matches(0), "invalid complete trailer rejected cold"); }
        store(schemaPath, original); need(cold(self, 14001).matches(14001), "restore complete valid synthetic schema");
        std::cout << "[IDENTITY SEQUENCE SCHEMA] checked=" << checked << " synthetic_oid_codec_only=true\n";
        return 0;
    } catch (const std::exception& error) { std::cerr << "IDENTITY_CODEC_FAIL " << error.what() << '\n'; return 1; }
}
