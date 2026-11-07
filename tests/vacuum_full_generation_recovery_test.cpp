#include "access/IndexFileUtil.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
extern char** environ;
namespace {
using namespace dbms;
using Rows = std::map<std::string, StorageEngine::SqlRow>;
const std::string database = "__t_full_generation_recovery";
void need(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
std::string bytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    need(bool(input), "open complete recorded bytes");
    std::string result(std::istreambuf_iterator<char>(input), {});
    need(!input.bad(), "read complete recorded bytes"); return result;
}
TableSchema schema() {
    TableSchema result; result.tablename = "items"; result.formatVersion = 2;
    result.append(makeIntColumn("id", false, 4, true));
    result.append(makeVarCharColumn("body", false, 1500));
    result.append(makeTextColumn("large", true)); return result;
}
StorageEngine::SqlRow row(unsigned id) {
    std::string body(1100, static_cast<char>('a' + id % 26));
    body += "_" + std::to_string(id) + " é中🙂 \t";
    return {{"id", std::to_string(id)}, {"body", body},
        {"large", id % 3 == 0 ? std::optional<std::string>{} :
          std::optional<std::string>{std::string(70003, static_cast<char>('A' + id % 26)) + " é中🙂 \t"}}};
}
std::string name(const std::string& mode) { return mode == "repeat-rename" ? "moved" : "items"; }
Rows model(const std::string& mode, bool final) {
    Rows result;
    if (mode != "empty") for (unsigned id = 80; id < 96; ++id)
        result.emplace(std::to_string(id), row(id));
    if (final) result.emplace("999", row(999));
    return result;
}
Rows scan(const std::string& table) {
    const auto s = g_engine.getTableSchema(database, table);
    need(s.len == 3, "actual three-column schema");
    TableScanOp op(&g_engine, database, table); need(op.open(), "actual complete scan opens");
    Rows result; std::string raw;
    while (op.next(raw)) {
        StorageEngine::SqlRow value;
        for (size_t j = 0; j < s.len; ++j) value[s.cols[j].dataName] =
            op.lastColumnIsNull(j) ? std::optional<std::string>{} :
            std::optional<std::string>{g_engine.extractColumnValue(raw, s, j, database)};
        need(value["id"].has_value(), "actual nonNULL primary key");
        const auto id = *value["id"];
        need(result.emplace(id, std::move(value)).second, "no duplicate visible primary keys");
    }
    need(!op.hasError(), "complete scan has no hidden failure"); op.close(); return result;
}
void init() {
    need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "create actual database");
    need(g_engine.createTable(database, schema()) == DBStatus::OK, "create actual empty table");
}
void setup(const std::string& mode) {
    if (mode == "legacy") {
        need(g_engine.getTableSchema(database, "items").physicalRelationId == 0,
             "legacy fixture is loaded before its first heap WAL");
    } else init();
    if (mode != "empty") {
        need(g_engine.beginTransaction(database) == DBStatus::OK, "begin real population");
        for (unsigned id = 0; id < 96; ++id)
            need(g_engine.insertRow(database, "items", row(id)) == DBStatus::OK, "populate full values");
        need(g_engine.commitTransaction() == DBStatus::OK, "commit real population");
        need(g_engine.remove(database, "items", {"<id 80"}) == DBStatus::OK, "delete old physical rows");
    }
    need(scan("items") == model(mode, false), "all original values before FULL");
    uint64_t identity = g_engine.getTableSchema(database, "items").physicalRelationId;
    const auto before = std::filesystem::file_size(std::filesystem::path(database) / "items.dt");
    for (unsigned pass = 0; pass < (mode == "repeat-rename" ? 3u : mode == "empty" ? 2u : 1u); ++pass) {
        if (mode == "repeat-rename" && pass == 2)
            need(g_engine.alterTableRenameTable(database, "items", "moved") == DBStatus::OK,
                 "rename actual twice-rewritten relation");
        const std::string table = mode == "repeat-rename" && pass == 2 ? "moved" : "items";
        bool succeeded = false; const auto count = g_engine.vacuumFull(database, table, &succeeded);
        need(succeeded && count == (mode == "empty" ? 0u : 16u), "actual FULL success and exact count");
        need(scan(table) == model(mode, false), "all full values after each generation");
        const auto current = g_engine.getTableSchema(database, table).physicalRelationId;
        std::cout << "GENERATION_STEP mode=" << mode << " pass=" << pass
                  << " old=" << identity << " new=" << current << '\n' << std::flush;
        need(current > identity, "each FULL durably allocates a fresh physical generation");
        identity = current;
    }
    const auto after = std::filesystem::file_size(std::filesystem::path(database) / (name(mode) + ".dt"));
    if (mode != "empty") need(after < before, "real physical compaction survives repeated rewrite");
    need(g_engine.insertRow(database, name(mode), row(999)) == DBStatus::OK, "commit actual later generation DML");
    need(scan(name(mode)) == model(mode, true), "all final values before close");
    std::cout << "GENERATION_READY mode=" << mode << " before=" << before << " compacted=" << after
              << " final_identity=" << identity << '\n' << std::flush;
}
int child(const std::string& executable, const std::string& mode, const std::string& phase) {
    std::vector<std::string> entries;
    for (char** e = environ; *e; ++e)
        if (std::string(*e).rfind("DBMS_DATA_DIR=", 0) != 0) entries.emplace_back(*e);
    entries.push_back("DBMS_DATA_DIR=" + std::filesystem::absolute(mode).string());
    std::vector<char*> env; for (auto& e : entries) env.push_back(e.data()); env.push_back(nullptr);
    char* args[] = {const_cast<char*>(executable.c_str()), const_cast<char*>("--phase"),
        const_cast<char*>(phase.c_str()), const_cast<char*>(mode.c_str()), nullptr};
    std::cout.flush(); std::cerr.flush();
    const auto pid = ::fork(); need(pid >= 0, "fork exact owned child");
    if (pid == 0) {
        if (::chdir(mode.c_str()) != 0) ::_exit(126);
        ::execve(executable.c_str(), args, env.data()); ::_exit(127);
    }
    int status = 0; pid_t waited;
    do { waited = ::waitpid(pid, &status, 0); } while (waited == -1 && errno == EINTR);
    need(waited == pid, "reap exact owned child");
    const int code = WIFEXITED(status) ? WEXITSTATUS(status) : WIFSIGNALED(status) ? -WTERMSIG(status) : 255;
    std::cout << "GENERATION_CHILD mode=" << mode << " phase=" << phase << " pid=" << pid
              << " actual_exit=" << code << " raw_wait=" << status << '\n' << std::flush;
    return code;
}
}
int main(int argc, char** argv) {
    try {
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        if (argc == 4 && std::string(argv[1]) == "--phase") {
            const std::string phase = argv[2], mode = argv[3];
            if (phase == "init") init();
            else if (phase == "setup") setup(mode);
            else {
                need(phase == "cold1" || phase == "cold2", "known cold phase");
                need(scan(name(mode)) == model(mode, true), "all full cold values and NULLs");
                need(g_engine.getTableSchema(database, name(mode)).physicalRelationId != 0,
                     "new generation persists across cold recovery");
                need(!std::filesystem::exists(std::filesystem::path(database) /
                     ("." + name(mode) + ".vacuum_full_pending")), "no pending rewrite remains");
            }
            return 0;
        }
        need(argc == 1, "known parent arguments");
        const auto executable = std::filesystem::read_symlink("/proc/self/exe").string();
        unsigned executed = 0, failures = 0, notrun = 0;
        for (const std::string mode : {"legacy", "repeat-rename", "empty"}) {
            need(std::filesystem::create_directory(mode), "fresh own scenario directory");
            if (mode == "legacy") {
                ++executed;
                if (child(executable, mode, "init") != 0) { ++failures; notrun += 3; continue; }
                // Empty-table fixture only: no identity-bearing heap image has
                // ever existed. Production never downgrades a live schema.
                const auto path = std::filesystem::path(mode) / database / "items.stc";
                auto raw = bytes(path); uint32_t magic = 0; int32_t version = 0;
                need(raw.size() >= 16, "complete modern fixture schema");
                std::memcpy(&version, raw.data(), 4);
                std::memcpy(&magic, raw.data() + raw.size() - 12, 4);
                need(version == 0x4442000A && magic == 0x31444952, "actual modern schema envelope");
                version = 0x44420009; std::memcpy(raw.data(), &version, 4); raw.resize(raw.size() - 12);
                need(index_file::writeAtomically(path, raw), "publish valid empty 09 fixture");
            }
            for (const std::string phase : {"setup", "cold1", "cold2"}) {
                ++executed;
                if (child(executable, mode, phase) != 0) {
                    ++failures; notrun += phase == "setup" ? 2 : phase == "cold1" ? 1 : 0; break;
                }
            }
        }
        std::cout << "GENERATION_COMPLETE executed=" << executed << " failures=" << failures
                  << " notrun=" << notrun << '\n'; return failures || notrun ? 1 : 0;
    } catch (const DbError& e) {
        std::cerr << "GENERATION_FATAL " << e.sqlState() << ' ' << e.what() << '\n'; return 1;
    } catch (const std::exception& e) {
        std::cerr << "GENERATION_FATAL " << e.what() << '\n'; return 1;
    }
}
