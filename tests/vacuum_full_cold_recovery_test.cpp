// test_link_flags: -Wl,--wrap=rename
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "commands/VacuumFullBackup.h"
#include "executor/ExecutionPlan.h"
#include "access/BPTree.h"
#include "storage/PageCrypto.h"
#include <cerrno>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
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
enum class BrinRewriteFault { None, WriteError, Crash };
BrinRewriteFault brinRewriteFault = BrinRewriteFault::None;
bool observedNewSchemaBeforeBrin = false;
enum class ToastRewriteFault { None, IndexWriteError, HeapCrash, IndexCrash };
ToastRewriteFault toastRewriteFault = ToastRewriteFault::None;
bool observedToastPublication = false;
const auto liveSchema = std::filesystem::path(database) / "items.stc";
const auto liveBrin = std::filesystem::path(database) / "items_id.brin";
const auto pendingFull = std::filesystem::path(database) / ".items.vacuum_full_pending";
const std::filesystem::path originalToast = "original-toast";
const auto liveToastHeap = std::filesystem::path(database) / "items.toast.dt";
const auto liveToastIndex = std::filesystem::path(database) / "items.toast.idx";
const auto liveToastHeapTde = std::filesystem::path(liveToastHeap.string() + ".tde");
const auto liveToastIndexTde = std::filesystem::path(liveToastIndex.string() + ".tde");
constexpr const char* mainFiles[] = {"items.dt", "items.dt.tde", "items.idx", "items.idx.tde", "items.stc"};
constexpr const char* toastFiles[] = {"items.toast.dt", "items.toast.dt.tde", "items.toast.idx", "items.toast.idx.tde", "items.toastmeta"};
void require(bool value, const char* label) {
    if (!value) throw std::runtime_error(label);
}
std::string fileBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    require(bool(input), "open actual rewrite witness file");
    std::string bytes{std::istreambuf_iterator<char>(input), {}};
    require(!input.bad() && !bytes.empty(), "read complete rewrite witness file");
    return bytes;
}

void saveRewriteWitness(const std::filesystem::path& source, const std::filesystem::path& target) {
    const auto bytes = fileBytes(source);
    std::ofstream output(target, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), bytes.size());
    output.close();
    require(bool(output), "save original rewrite witness");
}

bool isInterruptedBrinRewrite(const std::string& mode) {
    return mode == "brin-write-error" || mode == "brin-crash";
}
ToastRewriteFault toastFaultForMode(const std::string& mode) {
    if (mode == "toast-index-write-error") return ToastRewriteFault::IndexWriteError;
    if (mode == "toast-heap-crash") return ToastRewriteFault::HeapCrash;
    if (mode == "toast-index-crash") return ToastRewriteFault::IndexCrash;
    return ToastRewriteFault::None;
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
Rows snapshot(size_t columnCount = 10) {
    const auto schema = g_engine.getTableSchema(database, "items");
    require(schema.len == columnCount, "real complete schema");
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
void verifyRestoredBrinRewrite() {
    require(snapshot() == expected(false), "rollback preserves all original visible tuples");
    require(fileBytes(liveSchema) == fileBytes("original-schema.bin"), "rollback restores original schema bytes");
    require(fileBytes(liveBrin) == fileBytes("original-brin.bin"), "rollback restores original BRIN bytes");
    require(!std::filesystem::exists(pendingFull), "completed rollback clears pending rewrite");
    const auto ranges = g_engine.tryBrinPageRanges(database, "items", "id", ">=", "250", false);
    require(ranges && !ranges->empty(), "restored BRIN serves the original heap generation");
}

StorageEngine::SqlRow toastRow(unsigned id, Cell value) {
    return {{"id", std::to_string(id)}, {"large", std::move(value)}};
}

Rows toastSurvivors() {
    return {{"1", toastRow(1, payload(30000, 101))}, {"2", toastRow(2, payload(30000, 102))},
            {"97", toastRow(97, Cell{})}, {"98", toastRow(98, "")}};
}

bool matchesOriginalToastFile(const std::filesystem::path& file) {
    return fileBytes(file) == fileBytes(originalToast / file.filename());
}

void verifyToastMainFiles() {
    for (const auto* file : mainFiles) {
        require(matchesOriginalToastFile(std::filesystem::path(database) / file),
                "TOAST-only FULL preserves exact main files");
    }
}

void verifyRestoredToastRewrite() {
    require(snapshot(2) == toastSurvivors(), "TOAST recovery preserves exact visible values and SQL NULLs");
    verifyToastMainFiles();
    for (const auto* file : toastFiles) {
        require(matchesOriginalToastFile(std::filesystem::path(database) / file),
                "rollback restores the original TOAST pair, TDE companions and counter");
    }
    require(!std::filesystem::exists(pendingFull), "TOAST rollback clears the pending rewrite");
    for (const auto& entry : std::filesystem::directory_iterator(database)) {
        require(entry.path().filename().string().rfind(".items.vacuum_full_backup.", 0) != 0,
                "TOAST rollback removes the backup directory");
    }
}

void setupToastRewrite(ToastRewriteFault fault) {
    require(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "create TOAST rewrite database");
    TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id", false, 4, true));
    schema.append(makeTextColumn("large", true));
    require(g_engine.createTable(database, schema) == DBStatus::OK, "create actual TOAST rewrite table");
    require(g_engine.beginTransaction(database) == DBStatus::OK, "begin TOAST population");
    for (unsigned id = 1; id <= 8; ++id) {
        require(g_engine.insertRow(database, "items", toastRow(id, payload(30000, id + 100))) == DBStatus::OK,
                "insert actual external payload");
    }
    require(g_engine.commitTransaction() == DBStatus::OK, "commit actual TOAST population");
    require(g_engine.beginTransaction(database) == DBStatus::OK, "begin actual deletion");
    require(g_engine.remove(database, "items", {">id 2"}) == DBStatus::OK,
            "delete six tuples with real external payloads");
    require(g_engine.insertRow(database, "items", toastRow(97, Cell{})) == DBStatus::OK &&
            g_engine.insertRow(database, "items", toastRow(98, "")) == DBStatus::OK,
            "insert actual SQL NULL and an empty payload");
    require(g_engine.commitTransaction() == DBStatus::OK, "commit the hot heap population");
    require(snapshot(2) == toastSurvivors(), "verify actual survivor values before rewrite");
    require(g_engine.checkpoint(database), "persist the fixture before injecting failure");

    require(std::filesystem::file_size(liveToastHeap) > 16 * 8192,
            "actual TOAST fixture has a large physical extent");
    require(std::filesystem::file_size(liveToastHeapTde) > PageCrypto::kRecordSize &&
            std::filesystem::file_size(liveToastIndexTde) > PageCrypto::kRecordSize,
            "both TOAST forks have real encrypted pages");
    require(std::filesystem::create_directory(originalToast), "prepare original fork witness directory");
    for (const auto* file : mainFiles)
        saveRewriteWitness(std::filesystem::path(database) / file, originalToast / file);
    for (const auto* file : toastFiles)
        saveRewriteWitness(std::filesystem::path(database) / file, originalToast / file);

    toastRewriteFault = fault;
    bool succeeded = false;
    const auto count = g_engine.vacuumFull(database, "items", &succeeded, false);
    require(fault == ToastRewriteFault::IndexWriteError && observedToastPublication,
            "TOAST-only FULL reached the selected fault before commit");
    require(!succeeded && count == 0, "index publication error aborts TOAST-only FULL");
    verifyRestoredToastRewrite();
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
    if (isInterruptedBrinRewrite(mode)) {
        require(g_engine.createBrinIndex(database, "items", "id", 1) == DBStatus::OK,
                "build original healthy BRIN without a SQL index name");
        saveRewriteWitness(liveSchema, "original-schema.bin");
        saveRewriteWitness(liveBrin, "original-brin.bin");
        brinRewriteFault = mode == "brin-crash" ? BrinRewriteFault::Crash : BrinRewriteFault::WriteError;
    }
    bool succeeded = false;
    const auto count = g_engine.vacuumFull(database, "items", &succeeded);
    if (isInterruptedBrinRewrite(mode)) {
        require(observedNewSchemaBeforeBrin && mode == "brin-write-error",
                "BRIN publication attempted under the new schema before commit");
        require(!succeeded && count == 0, "BRIN rename failure aborts actual FULL");
        verifyRestoredBrinRewrite();
        return;
    }
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

extern "C" int __real_rename(const char* source, const char* target);
extern "C" int __wrap_rename(const char* source, const char* target) {
    if (toastRewriteFault != ToastRewriteFault::None) {
        const auto fault = toastRewriteFault;
        const auto& live = fault == ToastRewriteFault::HeapCrash ? liveToastHeap : liveToastIndex;
        const std::filesystem::path staged(source), destination(target);
        if (staged.filename() == live.filename() && staged.parent_path().filename() == "new" &&
            staged.parent_path().parent_path().filename().string().rfind(".items.vacuum_full_backup.", 0) == 0 &&
            std::filesystem::absolute(destination).lexically_normal() ==
                std::filesystem::absolute(live).lexically_normal()) {
            toastRewriteFault = ToastRewriteFault::None;
            vacuum_full_backup::State receipt;
            require(vacuum_full_backup::decode(fileBytes(pendingFull), receipt) && receipt.toastOnly &&
                    !receipt.undone && receipt.database == database && receipt.table == "items" &&
                    staged.parent_path().parent_path().filename() == receipt.backupLeaf,
                    "matching durable TOAST-only receipt precedes publication");
            verifyToastMainFiles();
            require(matchesOriginalToastFile(liveToastIndex) && matchesOriginalToastFile(liveToastIndexTde),
                    "the original index pair is still installed before its replacement");
            const auto originalBytes = std::filesystem::file_size(originalToast / liveToastHeap.filename());
            if (fault == ToastRewriteFault::HeapCrash) {
                require(matchesOriginalToastFile(liveToastHeap) && matchesOriginalToastFile(liveToastHeapTde),
                        "the original heap pair is still installed before first publication");
            } else {
                require(std::filesystem::file_size(liveToastHeap) < originalBytes &&
                        !matchesOriginalToastFile(liveToastHeapTde),
                        "the compacted encrypted heap pair is installed before index publication");
            }
            observedToastPublication = true;
            if (fault == ToastRewriteFault::IndexWriteError) {
                std::cout << "FULL_TOAST_RENAME_INJECTED compacted_heap=1 original_index=1 error=EIO\n" << std::flush;
                errno = EIO;
                return -1;
            }
            const int result = __real_rename(source, target);
            if (result == 0) {
                require(std::filesystem::file_size(liveToastHeap) < originalBytes &&
                        matchesOriginalToastFile(liveToastIndexTde),
                        "real compacted heap is installed while the old index companion remains");
                if (fault == ToastRewriteFault::HeapCrash) {
                    require(matchesOriginalToastFile(liveToastHeapTde) && matchesOriginalToastFile(liveToastIndex),
                            "interruption after the heap file leaves both original companions and index");
                } else {
                    require(!matchesOriginalToastFile(liveToastIndex),
                            "interruption after the index file leaves a real replacement index");
                }
                std::cout << "FULL_TOAST_RENAME_INTERRUPTED file=" << live.filename().string()
                          << " replacement_published=1 pending_receipt=1\n" << std::flush;
                ::kill(::getpid(), SIGKILL);
                ::_exit(127);
            }
            return result;
        }
    }
    if (brinRewriteFault == BrinRewriteFault::None ||
        std::filesystem::path(target).filename() != liveBrin.filename())
        return __real_rename(source, target);

    const auto fault = brinRewriteFault;
    brinRewriteFault = BrinRewriteFault::None;
    observedNewSchemaBeforeBrin = std::filesystem::is_regular_file(pendingFull) &&
        fileBytes(liveSchema) != fileBytes("original-schema.bin");
    if (!observedNewSchemaBeforeBrin) return __real_rename(source, target);

    if (fault == BrinRewriteFault::WriteError) {
        std::cout << "FULL_BRIN_RENAME_INJECTED new_schema=1 error=EIO\n" << std::flush;
        errno = EIO;
        return -1;
    }
    const int result = __real_rename(source, target);
    if (result == 0) {
        std::cout << "FULL_BRIN_RENAME_INTERRUPTED new_schema=1 replacement_published=1\n" << std::flush;
        ::kill(::getpid(), SIGKILL);
        ::_exit(127);
    }
    return result;
}

int main(int argc, char** argv) {
    try {
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        if (argc == 4 && std::string(argv[1]) == "--phase") {
            const std::string phase = argv[2], mode = argv[3];
            const auto toastFault = toastFaultForMode(mode);
            if (toastFault != ToastRewriteFault::None) {
                require(PageCrypto::enable(std::string(64, 'a')), "enable fixture page encryption");
                if (phase == "setup") setupToastRewrite(toastFault);
                else {
                    require(phase == "cold1" || phase == "cold2", "known TOAST cold phase");
                    verifyRestoredToastRewrite();
                    std::cout << "FULL_COLD_TOAST_ROLLBACK mode=" << mode << " phase=" << phase
                              << " original_main_toast_and_rows=exact\n";
                }
            } else if (phase == "setup") setup(mode);
            else {
                require(phase == "cold1" || phase == "cold2", "known actual cold phase");
                if (isInterruptedBrinRewrite(mode)) {
                    verifyRestoredBrinRewrite();
                    std::cout << "FULL_COLD_BRIN_ROLLBACK mode=" << mode << " phase=" << phase
                              << " original_schema_index_and_rows=exact\n";
                } else {
                    verifyFinal();
                    std::cout << "FULL_COLD_VALUES mode=" << mode << " phase=" << phase
                              << " all_values_NULLs_keys_and_compaction=exact\n";
                }
            }
            return 0;
        }
        require(argc == 1, "known parent arguments");
        const auto executable = std::filesystem::read_symlink("/proc/self/exe").string();
        unsigned failures = 0, executed = 0, notrun = 0;
        for (const std::string mode : {"closed", "killed", "brin-write-error", "brin-crash",
                                      "toast-index-write-error", "toast-heap-crash", "toast-index-crash"}) {
            require(std::filesystem::create_directory(mode), "fresh actual scenario directory");
            const int setupExit = child(executable, mode, "setup"); ++executed;
            const auto toastFault = toastFaultForMode(mode);
            const bool killed = mode == "killed" || mode == "brin-crash" ||
                toastFault == ToastRewriteFault::HeapCrash || toastFault == ToastRewriteFault::IndexCrash;
            if (setupExit != (killed ? -SIGKILL : 0)) {
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
