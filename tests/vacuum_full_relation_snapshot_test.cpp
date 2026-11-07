#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "storage/PageAllocator.h"
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
extern char** environ;
namespace {
std::atomic<bool> armed{false}, forceCopy{false};
std::atomic<unsigned> linkAttempts{0}, linked{0}, rejected{0}, badLinks{0}, unrelatedSyncs{0};
bool backup(const char* path) {
    if (!path || !std::strstr(path, ".items.vacuum_full_backup.")) return false;
    const auto n = std::strlen(path);
    return n >= 4 && std::strcmp(path + n - 4, ".bak") == 0;
}
int linkFile(int fromDir, const char* from, int toDir, const char* to, int flags) {
    const bool watched = armed.load() && backup(to);
    if (watched) {
        ++linkAttempts;
        if (forceCopy.load()) { ++rejected; errno = EXDEV; return -1; }
    }
    const int result = static_cast<int>(::syscall(SYS_linkat, fromDir, from, toDir, to, flags));
    const int error = errno;
    if (watched && result == 0) {
        struct stat source{}, image{};
        if (::fstatat(fromDir, from, &source, 0) != 0 ||
            ::fstatat(toDir, to, &image, 0) != 0 ||
            source.st_dev != image.st_dev || source.st_ino != image.st_ino ||
            source.st_nlink < 2 || image.st_nlink < 2) ++badLinks;
        ++linked;
    }
    errno = error; return result;
}
}
extern "C" int link(const char* from, const char* to) noexcept {
    return linkFile(AT_FDCWD, from, AT_FDCWD, to, 0);
}
extern "C" int linkat(int fromDir, const char* from, int toDir, const char* to, int flags) noexcept {
    return linkFile(fromDir, from, toDir, to, flags);
}
extern "C" int fsync(int fd) noexcept {
    if (armed.load()) {
        char name[64], path[4096];
        std::snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
        const auto n = ::readlink(name, path, sizeof(path) - 1);
        if (n >= 0) {
            path[n] = 0;
            if (std::strstr(path, "/unrelated.dt")) ++unrelatedSyncs;
        }
    }
    return static_cast<int>(::syscall(SYS_fsync, fd));
}
namespace {
using namespace dbms;
using Rows = std::map<std::string, StorageEngine::SqlRow>;
const std::string database = "__t_full_relation_snapshot";
unsigned failures = 0, checks = 0;
void need(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
void check(bool value, const char* message) {
    ++checks;
    if (!value) { ++failures; std::cout << "SCOPE_CHECK_FAIL " << message << '\n'; }
}
void reset() {
    linkAttempts = 0; linked = 0; rejected = 0; badLinks = 0; unrelatedSyncs = 0;
}
void calibrate() {
    const auto directory = std::filesystem::path("probe/.items.vacuum_full_backup.999");
    need(std::filesystem::create_directories(directory), "fresh own interceptor probe");
    std::ofstream("probe/source") << "complete interceptor control\n";
    armed = true; forceCopy = false;
    need(::link("probe/source", (directory / "1.bak").c_str()) == 0,
         "real successful hard-link interceptor control");
    need(linkAttempts == 1 && linked == 1 && badLinks == 0, "real inode/link-count verification active");
    forceCopy = true;
    need(::link("probe/source", (directory / "2.bak").c_str()) == -1 && errno == EXDEV &&
         rejected == 1 && linkAttempts == 2, "real declared fallback interceptor active");
    armed = false; forceCopy = false; reset();
    need(std::filesystem::remove_all("probe") > 0, "remove exact own interceptor probe");
    std::cout << "SCOPE_INTERCEPTOR_CALIBRATION real_link_and_EXDEV=pass\n";
}
TableSchema schema(const std::string& name) {
    TableSchema s; s.tablename = name; s.formatVersion = 2;
    s.append(makeIntColumn("id", false, 4, true));
    s.append(makeVarCharColumn("body", false, 1500));
    s.append(makeTextColumn("large", true));
    s.append(makeTextColumn("note", true)); return s;
}
StorageEngine::SqlRow row(unsigned id) {
    return {{"id", std::to_string(id)},
        {"body", std::string(1100, static_cast<char>('a' + id % 26)) + "_" + std::to_string(id)},
        {"large", id % 5 == 0 ? std::optional<std::string>{std::string(70003, 'A' + id % 26) + " é中🙂 \t"} :
            id % 5 == 1 ? std::optional<std::string>{} : id % 5 == 2 ? std::optional<std::string>{""} :
            std::optional<std::string>{"NULL"}},
        {"note", id % 3 == 0 ? std::optional<std::string>{} :
            std::optional<std::string>{"note_é中🙂_" + std::to_string(id)}}};
}
Rows expected(bool other) {
    Rows result;
    for (unsigned id = other ? 1000 : 240; id < (other ? 1012 : 256); ++id)
        result.emplace(std::to_string(id), row(id));
    return result;
}
Rows scan(const std::string& table) {
    const auto s = g_engine.getTableSchema(database, table);
    need(s.len == 4, "actual complete four-column schema");
    TableScanOp op(&g_engine, database, table); need(op.open(), "actual complete scan opens");
    Rows result; std::string raw;
    while (op.next(raw)) {
        StorageEngine::SqlRow value;
        for (size_t j = 0; j < s.len; ++j) value[s.cols[j].dataName] = op.lastColumnIsNull(j) ?
            std::optional<std::string>{} :
            std::optional<std::string>{g_engine.extractColumnValue(raw, s, j, database)};
        need(value["id"].has_value(), "actual nonNULL key"); const auto id = *value["id"];
        need(result.emplace(id, std::move(value)).second, "all actual keys unique");
    }
    need(!op.hasError(), "actual full scan completes"); op.close(); return result;
}
void verify() {
    need(scan("items") == expected(false), "all target values and NULLs preserved");
    need(scan("unrelated") == expected(true), "all unrelated values and NULLs preserved");
    auto* primary = g_engine.getPKIndex(database, "items"); need(primary != nullptr, "actual rebuilt primary index");
    for (const auto& [key, value] : expected(false)) {
        int64_t rid = -1; need(primary->search(key, rid), "actual surviving key index entry");
        need(g_engine.isColumnNullByRid(database, "items", rid, 3) == !value.at("note").has_value(),
             "actual rewritten RID resolves its NULL bitmap");
    }
}
void setup(const std::string& mode) {
    calibrate();
    need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "create actual own database");
    need(g_engine.createTable(database, schema("items")) == DBStatus::OK &&
         g_engine.createTable(database, schema("unrelated")) == DBStatus::OK, "create real target and unrelated tables");
    need(g_engine.beginTransaction(database) == DBStatus::OK, "begin actual population");
    for (unsigned id = 0; id < 256; ++id)
        need(g_engine.insertRow(database, "items", row(id)) == DBStatus::OK, "populate complete target rows");
    for (const auto& [key, value] : expected(true)) {
        (void)key; need(g_engine.insertRow(database, "unrelated", value) == DBStatus::OK,
                       "populate complete unrelated rows");
    }
    need(g_engine.commitTransaction() == DBStatus::OK, "commit real complete population");
    need(g_engine.remove(database, "items", {"<id 240"}) == DBStatus::OK, "delete actual old-page rows");
    need(g_engine.createIndex(database, "items", "note") == DBStatus::OK &&
         g_engine.createHashIndex(database, "items", "id") == DBStatus::OK &&
         g_engine.createBloomIndex(database, "items", "id") == DBStatus::OK, "create real RID-bearing indexes");
    verify();
    auto* unrelated = g_engine.getPageAllocator(database, "unrelated");
    need(unrelated && unrelated->bufferPool(), "actual unrelated allocator is cached");
    unrelated->markDirty(1);
    need(unrelated->bufferPool()->isPageDirty(1), "real dirty unrelated frame before FULL");
    const auto heap = std::filesystem::path(database) / "items.dt";
    const auto before = std::filesystem::file_size(heap);
    reset(); forceCopy = mode == "forced-copy"; armed = true;
    bool succeeded = false; const auto rows = g_engine.vacuumFull(database, "items", &succeeded);
    armed = false; forceCopy = false;
    need(succeeded && rows == 16, "FULL actually completes with all sixteen live rows");
    need(std::filesystem::file_size(heap) < before, "target heap actually compacted");
    check(unrelatedSyncs == 0, "FULL does not sync the unrelated heap");
    check(unrelated->bufferPool()->isPageDirty(1), "FULL leaves unrelated dirty-frame ownership intact");
    if (mode == "linked")
        check(linkAttempts > 0 && linked == linkAttempts && rejected == 0 && badLinks == 0,
              "all eligible rollback images share their original inode");
    else {
        need(mode == "forced-copy", "known declared snapshot mode");
        check(linkAttempts > 0 && rejected == linkAttempts && linked == 0 && badLinks == 0,
              "unsupported hard links really exercise complete-copy fallback");
    }
    need(unrelated->flush(), "unrelated owner can still durably flush its own frame");
    need(!unrelated->bufferPool()->isPageDirty(1), "unrelated flush really completes");
    verify();
    need(!std::filesystem::exists(std::filesystem::path(database) / ".items.vacuum_full_pending"),
         "no pending rewrite after actual commit");
    std::cout << "SCOPE_READY mode=" << mode << " link_attempts=" << linkAttempts << " linked=" << linked
              << " forced_EXDEV=" << rejected << " unrelated_syncs=" << unrelatedSyncs
              << " checks=" << checks << " failures=" << failures << " full_values_NULLs=exact\n" << std::flush;
}
int child(const std::string& executable, const std::string& mode, const std::string& phase) {
    std::vector<std::string> values;
    for (char** e = environ; *e; ++e)
        if (std::string(*e).rfind("DBMS_DATA_DIR=", 0) != 0) values.emplace_back(*e);
    values.push_back("DBMS_DATA_DIR=" + std::filesystem::absolute(mode).string());
    std::vector<char*> env; for (auto& value : values) env.push_back(value.data()); env.push_back(nullptr);
    char* args[] = {const_cast<char*>(executable.c_str()), const_cast<char*>("--phase"),
        const_cast<char*>(phase.c_str()), const_cast<char*>(mode.c_str()), nullptr};
    std::cout.flush(); std::cerr.flush(); const auto pid = ::fork(); need(pid >= 0, "fork exact own child");
    if (pid == 0) {
        if (::chdir(mode.c_str()) != 0) ::_exit(126);
        ::execve(executable.c_str(), args, env.data()); ::_exit(127);
    }
    int status = 0; pid_t waited;
    do { waited = ::waitpid(pid, &status, 0); } while (waited == -1 && errno == EINTR);
    need(waited == pid, "reap exact own child"); const int code = WIFEXITED(status) ? WEXITSTATUS(status) :
        WIFSIGNALED(status) ? -WTERMSIG(status) : 255;
    std::cout << "SCOPE_CHILD mode=" << mode << " phase=" << phase << " pid=" << pid
              << " actual_exit=" << code << " raw_wait=" << status << '\n' << std::flush; return code;
}
}
int main(int argc, char** argv) {
    try {
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        if (argc == 4 && std::string(argv[1]) == "--phase") {
            const std::string phase = argv[2], mode = argv[3];
            if (phase == "setup") setup(mode);
            else { need(phase == "cold1" || phase == "cold2", "known cold phase"); verify(); }
            return failures ? 1 : 0;
        }
        need(argc == 1, "known parent arguments");
        const auto executable = std::filesystem::read_symlink("/proc/self/exe").string();
        unsigned stages = 0, failed = 0, notrun = 0;
        for (const std::string mode : {"linked", "forced-copy"}) {
            need(std::filesystem::create_directory(mode), "fresh own scenario directory");
            const int setupExit = child(executable, mode, "setup"); ++stages;
            if (setupExit != 0) ++failed;
            if (setupExit != 0 && setupExit != 1) { notrun += 2; continue; }
            // A failed optimization check still leaves a complete valid setup;
            // run the cold data checks rather than hiding those observations.
            const int first = child(executable, mode, "cold1"); ++stages;
            if (first != 0) { ++failed; ++notrun; continue; }
            const int second = child(executable, mode, "cold2"); ++stages;
            if (second != 0) ++failed;
        }
        std::cout << "SCOPE_COMPLETE stages=" << stages << " failed=" << failed << " notrun=" << notrun << '\n';
        return failed || notrun ? 1 : 0;
    } catch (const DbError& error) {
        std::cerr << "SCOPE_FATAL " << error.sqlState() << ' ' << error.what() << '\n'; return 2;
    } catch (const std::exception& error) {
        std::cerr << "SCOPE_FATAL " << error.what() << '\n'; return 2;
    }
}
