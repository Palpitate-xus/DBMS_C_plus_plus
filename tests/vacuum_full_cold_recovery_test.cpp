#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "access/BPTree.h"
#include <cerrno>
#include <csignal>
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
using Cell = std::optional<std::string>;
using Rows = std::map<std::string, StorageEngine::SqlRow>;
const std::string database = "__t_full_cold_recovery";
void require(bool value, const char* label) {
    if (!value) throw std::runtime_error(label);
}
std::string payload(size_t bytes, uint32_t state) {
    static constexpr char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string result(bytes, '\0');
    for (auto& c : result) {
        state ^= state << 13; state ^= state >> 17; state ^= state << 5;
        c = alphabet[state % (sizeof(alphabet) - 1)];
    }
    return result + " \n\t";
}
StorageEngine::SqlRow row(unsigned id) {
    StorageEngine::SqlRow result{{"id", std::to_string(id)},
        {"body", payload(id == 999 ? 900 : 1000, id + 17)}};
    switch (id % 5) {
        case 0: result["large"] = payload(70000, id + 81); break;
        case 1: result["large"] = Cell{}; break;
        case 2: result["large"] = std::string{}; break;
        case 3: result["large"] = "NULL"; break;
        default: result["large"] = "__TOAST__1"; break;
    }
    for (unsigned j = 0; j < 7; ++j)
        result["n" + std::to_string(j)] = id % (j + 2) == 0 ? Cell{} :
            Cell{"é中🙂_" + std::to_string(id) + "_" + std::to_string(j) + " \t"};
    return result;
}
Rows expected(bool final) {
    Rows result;
    for (unsigned id = 240; id < 256; ++id)
        if (!final || id != 241) result[std::to_string(id)] = row(id);
    if (final) {
        result["250"]["body"] = payload(1300, 91);
        result["250"]["large"] = payload(1048573, 92);
        result["999"] = row(999);
        result["999"]["large"] = payload(70000, 93);
    }
    return result;
}
Rows snapshot() {
    const auto schema = g_engine.getTableSchema(database, "items");
    require(schema.len == 10, "real complete schema");
    TableScanOp scan(&g_engine, database, "items");
    require(scan.open(), "real complete scan opens");
    Rows result; std::string raw;
    while (scan.next(raw)) {
        StorageEngine::SqlRow value;
        for (size_t j = 0; j < schema.len; ++j)
            value[schema.cols[j].dataName] = scan.lastColumnIsNull(j) ? Cell{} :
                Cell{g_engine.extractColumnValue(raw, schema, j, database)};
        require(value.at("id").has_value(), "real nonNULL id");
        const auto id = *value.at("id");
        require(result.emplace(id, std::move(value)).second, "real unique complete rows");
    }
    require(!scan.hasError(), "real complete scan finishes");
    scan.close(); return result;
}
void verifyFinal() {
    require(snapshot() == expected(true), "all exact values and NULLs after FULL and later DML");
    auto* primary = g_engine.getPKIndex(database, "items");
    require(primary != nullptr, "real primary index opens");
    for (const auto& [id, value] : expected(true)) {
        (void)value; int64_t rid = -1;
        require(primary->search(id, rid) && rid >= 0, "all surviving primary keys present");
    }
    for (unsigned id = 0; id < 240; ++id) {
        int64_t rid = -1;
        require(!primary->search(std::to_string(id), rid), "all pre-FULL deleted primary keys absent");
    }
    int64_t rid = -1;
    require(!primary->search("241", rid), "post-FULL deleted primary key absent");
    uintmax_t before = 0;
    std::ifstream input("original_heap_bytes.txt"); input >> before;
    require(input.good() && before > 0, "independent original heap size recorded");
    require(std::filesystem::file_size(std::filesystem::path(database) / "items.dt") < before,
            "compaction remains real after subsequent writes and recovery");
}
void setup(const std::string& mode) {
    require(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "create actual database");
    TableSchema schema; schema.tablename = "items"; schema.formatVersion = 2;
    schema.append(makeIntColumn("id", false, 4, true));
    schema.append(makeVarCharColumn("body", false, 1500));
    schema.append(makeTextColumn("large", true));
    for (unsigned j = 0; j < 7; ++j)
        schema.append(makeTextColumn("n" + std::to_string(j), true));
    require(g_engine.createTable(database, schema) == DBStatus::OK, "create actual physical table");
    require(g_engine.beginTransaction(database) == DBStatus::OK, "begin actual population");
    for (unsigned id = 0; id < 256; ++id)
        require(g_engine.insertRow(database, "items", row(id)) == DBStatus::OK, "insert original complete row");
    require(g_engine.commitTransaction() == DBStatus::OK, "commit actual population");
    require(g_engine.remove(database, "items", {"<id 240"}) == DBStatus::OK,
            "delete actual old-page population");
    require(snapshot() == expected(false), "full original survivor values before rewrite");
    const auto heap = std::filesystem::path(database) / "items.dt";
    const auto before = std::filesystem::file_size(heap);
    {
        std::ofstream output("original_heap_bytes.txt"); output << before << '\n';
        output.close(); require(bool(output), "persist independent original size");
    }
    bool succeeded = false;
    const auto count = g_engine.vacuumFull(database, "items", &succeeded);
    require(succeeded && count == 16, "FULL actually succeeds with exact live count");
    const auto after = std::filesystem::file_size(heap);
    require(after < before, "FULL actually removes old physical pages");
    require(snapshot() == expected(false), "all full values immediately after rewrite");
    require(g_engine.beginTransaction(database) == DBStatus::OK, "begin actual post-rewrite DML");
    require(g_engine.updateRows(database, "items", {{"body", payload(1300, 91)},
              {"large", payload(1048573, 92)}}, {"=id 250"}) == DBStatus::OK,
            "update actual rewritten row and long TEXT");
    auto inserted = row(999); inserted["large"] = payload(70000, 93);
    require(g_engine.insertRow(database, "items", inserted) == DBStatus::OK,
            "insert actual post-rewrite row");
    require(g_engine.remove(database, "items", {"=id 241"}) == DBStatus::OK,
            "delete actual rewritten row");
    require(g_engine.commitTransaction() == DBStatus::OK, "commit actual post-rewrite DML");
    verifyFinal();
    std::cout << "FULL_READY mode=" << mode << " before_bytes=" << before
              << " rewritten_bytes=" << after << " survivors=16 full_values=exact\n" << std::flush;
    if (mode == "killed") {
        require(::kill(::getpid(), SIGKILL) == 0, "kill exact owned setup process");
        ::_exit(127);
    }
    require(mode == "closed", "known normal setup mode");
}
int child(const std::string& executable, const std::string& mode, const std::string& phase) {
    std::vector<std::string> entries;
    for (char** e = environ; *e; ++e)
        if (std::string(*e).rfind("DBMS_DATA_DIR=", 0) != 0) entries.emplace_back(*e);
    entries.push_back("DBMS_DATA_DIR=" + std::filesystem::absolute(mode).string());
    std::vector<char*> env;
    for (auto& e : entries) env.push_back(e.data());
    env.push_back(nullptr);
    char* args[] = {const_cast<char*>(executable.c_str()), const_cast<char*>("--phase"),
        const_cast<char*>(phase.c_str()), const_cast<char*>(mode.c_str()), nullptr};
    std::cout.flush(); std::cerr.flush();
    const auto pid = ::fork(); require(pid >= 0, "fork own exact process");
    if (pid == 0) {
        if (::chdir(mode.c_str()) != 0) ::_exit(126);
        ::execve(executable.c_str(), args, env.data()); ::_exit(127);
    }
    int status = 0; pid_t waited;
    do { waited = ::waitpid(pid, &status, 0); } while (waited == -1 && errno == EINTR);
    require(waited == pid, "reap exact own child");
    const int code = WIFEXITED(status) ? WEXITSTATUS(status) :
        WIFSIGNALED(status) ? -WTERMSIG(status) : 255;
    std::cout << "FULL_CHILD mode=" << mode << " phase=" << phase
              << " pid=" << pid << " actual_exit=" << code << " raw_wait=" << status << '\n' << std::flush;
    return code;
}
}
int main(int argc, char** argv) {
    try {
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        if (argc == 4 && std::string(argv[1]) == "--phase") {
            const std::string phase = argv[2], mode = argv[3];
            if (phase == "setup") setup(mode);
            else {
                require(phase == "cold1" || phase == "cold2", "known actual cold phase");
                verifyFinal(); std::cout << "FULL_COLD_VALUES mode=" << mode
                    << " phase=" << phase << " all_values_NULLs_keys_and_compaction=exact\n";
            }
            return 0;
        }
        require(argc == 1, "known parent arguments");
        const auto executable = std::filesystem::read_symlink("/proc/self/exe").string();
        unsigned failures = 0, executed = 0, notrun = 0;
        for (const std::string mode : {"closed", "killed"}) {
            require(std::filesystem::create_directory(mode), "fresh actual scenario directory");
            const int setupExit = child(executable, mode, "setup"); ++executed;
            if (setupExit != (mode == "killed" ? -SIGKILL : 0)) {
                ++failures; notrun += 2;
                std::cout << "FULL_NOTRUN mode=" << mode << " phases=cold1,cold2 cause=setup_failed\n";
                continue;
            }
            const int cold1 = child(executable, mode, "cold1"); ++executed;
            if (cold1 != 0) {
                ++failures; ++notrun;
                std::cout << "FULL_NOTRUN mode=" << mode << " phase=cold2 cause=cold1_failed\n";
                continue;
            }
            const int cold2 = child(executable, mode, "cold2"); ++executed;
            if (cold2 != 0) ++failures;
        }
        std::cout << "FULL_COLD_COMPLETE executed=" << executed << " failures=" << failures
                  << " notrun=" << notrun << '\n';
        return failures || notrun ? 1 : 0;
    } catch (const DbError& e) {
        std::cerr << "FULL_FATAL " << e.sqlState() << ' ' << e.what() << '\n'; return 1;
    } catch (const std::exception& e) {
        std::cerr << "FULL_FATAL " << e.what() << '\n'; return 1;
    }
}
