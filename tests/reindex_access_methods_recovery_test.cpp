// Actual normal REINDEX, process interruption and fresh-process recovery.
// test_link_flags: -Wl,--wrap=_ZNSt10filesystem6renameERKNS_7__cxx114pathES3_RSt10error_code,--wrap=fsync
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/BrinIndexFormat.h"
#include "access/HashIndex.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "utils/Session.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <spawn.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace fs = std::filesystem;
extern "C" int __real_fsync(int);
extern "C" void realReindexRename(const fs::path&, const fs::path&, std::error_code&) noexcept
    asm("__real__ZNSt10filesystem6renameERKNS_7__cxx114pathES3_RSt10error_code");

namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
constexpr const char* DB = "__t_reindex_am_recovery";
constexpr const char* TABLE = "items";
constexpr int ROWS = 96;
size_t checks = 0;
thread_local bool armed = false, fired = false, insideHook = false;
thread_local std::string action;
fs::path databaseRoot, relationRoot, markerPath, evidenceRoot;

void need(bool good, const std::string& text) {
    ++checks;
    if (!good) throw std::runtime_error(text);
}
std::string bytes(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    need(bool(input), "open actual file: " + path.string());
    std::string value{std::istreambuf_iterator<char>(input), {}};
    need(!input.bad(), "read entire actual file");
    return value;
}
std::string hex(const std::string& value) {
    constexpr char alphabet[] = "0123456789abcdef";
    std::string result;
    result.reserve(value.size() * 2);
    for (unsigned char c : value) { result += alphabet[c >> 4]; result += alphabet[c & 15]; }
    return result;
}
std::string unhex(const std::string& text) {
    need(text.size() % 2 == 0, "hex width");
    const auto digit = [](char c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : -1; };
    std::string result;
    for (size_t i = 0; i < text.size(); i += 2) {
        const int a = digit(text[i]), b = digit(text[i + 1]);
        need(a >= 0 && b >= 0, "hex alphabet"); result += char(a * 16 + b);
    }
    return result;
}
bool under(const fs::path& path, const fs::path& parent) {
    const auto normalized = fs::absolute(path).lexically_normal();
    const auto base = fs::absolute(parent).lexically_normal();
    auto a = normalized.begin(), b = base.begin();
    for (; b != base.end(); ++b, ++a) if (a == normalized.end() || *a != *b) return false;
    return a != normalized.end();
}
bool bare(const fs::path& path) {
    static const std::set<std::string> extensions{ ".hidx", ".bidx", ".fti", ".gin", ".gist", ".spgist", ".brin" };
    return extensions.count(path.extension().string()) != 0;
}
bool ownedTarget(const fs::path& path) {
    return under(path, databaseRoot) && path.filename().string().rfind("items", 0) == 0;
}
struct Swap { bool paired = true; fs::path target, temporary, backup; };
struct Marker { std::string magic; std::vector<Swap> entries; };
Marker actualMarker() {
    std::ifstream input(markerPath);
    Marker result; size_t count = 0;
    need(bool(std::getline(input, result.magic)), "actual durable marker header");
    need(result.magic == "DBMS_REINDEX_SWAP_V1" || result.magic == "DBMS_REINDEX_SWAP_V2", "real V1/V2 producer marker");
    need(bool(input >> count) && count && count <= 4096, "actual marker count");
    for (size_t i = 0; i < count; ++i) {
        int pair = 1; std::string target, temporary, backup;
        if (result.magic == "DBMS_REINDEX_SWAP_V2") need(bool(input >> pair) && (pair == 0 || pair == 1), "V2 actual paired role");
        need(bool(input >> std::quoted(target) >> std::quoted(temporary) >> std::quoted(backup)), "actual marker paths");
        Swap entry{pair != 0, target, temporary, backup};
        need(ownedTarget(entry.target) && under(entry.temporary, databaseRoot) && under(entry.backup, databaseRoot), "actual paths stay in owned DB");
        need(entry.temporary.string().find(".reindex.tmp.") != std::string::npos && entry.backup.string().find(".reindex.old.") != std::string::npos, "real temporary generation names");
        if (bare(entry.target)) need(!entry.paired, "bare AM marker has no paired TDE role");
        result.entries.push_back(std::move(entry));
    }
    std::string extra; need(!(input >> extra) && input.eof(), "no marker trailing fields");
    return result;
}
bool allPublished(const Marker& marker) {
    return std::all_of(marker.entries.begin(), marker.entries.end(), [](const Swap& entry) {
        return fs::is_regular_file(entry.target) && !fs::exists(entry.temporary) &&
            (!entry.paired || (fs::is_regular_file(entry.target.string() + ".tde") && !fs::exists(entry.temporary.string() + ".tde")));
    });
}
void event(const char* name) {
    // Diagnostics are outside timing and do not mutate any index/marker.
    const bool previous = insideHook; insideHook = true;
    std::ofstream out(evidenceRoot / "event.txt", std::ios::app);
    out << name << " pid=" << getpid() << " action=" << action << '\n'; out.flush();
    if (!out) _exit(121);
    if (fs::is_regular_file(markerPath)) {
        std::ifstream input(markerPath, std::ios::binary);
        std::ofstream copy(evidenceRoot / "actual-marker.txt", std::ios::binary | std::ios::trunc);
        copy << input.rdbuf(); copy.flush(); if (!copy) _exit(122);
    }
    insideHook = previous;
}
[[noreturn]] void crash(const char* phase) {
    fired = true; event(phase);
    ::kill(::getpid(), SIGKILL); _exit(123);
}
std::string fdPath(int descriptor) {
    char buffer[4096]; const std::string source = "/proc/self/fd/" + std::to_string(descriptor);
    const auto n = ::readlink(source.c_str(), buffer, sizeof(buffer) - 1);
    return n < 0 ? std::string() : std::string(buffer, size_t(n));
}

struct Row { int id; Cell n, text, body, point, pad; };
Row model(int id) {
    Cell text = id % 11 == 0 ? Cell{} : id % 13 == 0 ? Cell("") : id == 3 ? Cell("NULL") : Cell("key" + std::to_string(id % 3));
    Cell body = id % 9 == 0 ? Cell{} : id % 10 == 0 ? Cell("") : Cell(id % 2 ? "alpha beta" : "beta gamma");
    return {id, id % 7 ? Cell(std::to_string(id - 48)) : Cell{}, text, body,
            id % 8 ? Cell(std::to_string(id % 4) + "," + std::to_string(id % 6)) : Cell{},
            id % 5 ? Cell("payload 界\n|;," + std::to_string(id) + std::string(768, char('a' + id % 23))) : Cell{}};
}
std::vector<Cell> cells(const Row& row) { return {std::to_string(row.id), row.n, row.text, row.body, row.point, row.pad}; }
std::vector<int64_t> sorted(std::vector<int64_t> value) { std::sort(value.begin(), value.end()); return value; }
struct Expected {
    std::map<int, int64_t> rids;
    std::map<std::string, std::string> metadata;
    uint64_t relation = 0;
    uint32_t bloomBits = 0, bloomHashes = 0;
};
fs::path findFile(const std::string& name) {
    std::vector<fs::path> found;
    for (const auto& entry : fs::recursive_directory_iterator(databaseRoot))
        if (entry.is_regular_file() && entry.path().filename() == name) found.push_back(entry.path());
    need(found.size() == 1, "one actual file " + name); return found.front();
}
std::map<std::string, std::string> metadata() {
    std::map<std::string, std::string> result;
    for (const auto& entry : fs::recursive_directory_iterator(databaseRoot)) {
        if (!entry.is_regular_file()) continue;
        const auto name = entry.path().filename().string();
        if (name.rfind("items", 0) != 0) continue;
        const auto extension = entry.path().extension().string();
        if (extension == ".stc" || extension == ".hashidx" || extension == ".bloomidx" ||
            extension == ".params" || name.find("name") != std::string::npos || name.find("meta") != std::string::npos)
            result.emplace(fs::relative(entry.path(), databaseRoot).string(), bytes(entry.path()));
    }
    need(result.count("items.stc") != 0, "actual original physical schema captured"); return result;
}
void noTemporary() {
    for (const auto& entry : fs::recursive_directory_iterator(databaseRoot)) {
        const auto name = entry.path().filename().string();
        need(name.find(".reindex.tmp.") == std::string::npos && name.find(".reindex.old.") == std::string::npos &&
             name.find(".reindex_swap") == std::string::npos, "no unrecovered private artifact: " + name);
    }
    for (const auto& entry : fs::directory_iterator("/proc/self/fd")) {
        const auto target = fdPath(std::stoi(entry.path().filename().string()));
        need(target.find(".reindex.tmp.") == std::string::npos && target.find(".reindex.old.") == std::string::npos, "no own temporary descriptor");
    }
}
void saveExpected(const fs::path& path, const Expected& value) {
    std::ofstream out(path); out << "REINDEX_AM_EXPECTED_V1\n" << value.relation << ' ' << value.bloomBits << ' ' << value.bloomHashes << '\n';
    out << value.rids.size() << '\n'; for (const auto& row : value.rids) out << row.first << ' ' << row.second << '\n';
    out << value.metadata.size() << '\n'; for (const auto& entry : value.metadata) out << std::quoted(entry.first) << ' ' << std::quoted(hex(entry.second)) << '\n';
    out.close(); need(!out.fail(), "complete genuine expected-state output");
}
Expected loadExpected(const fs::path& path) {
    Expected value; std::ifstream in(path); std::string magic; size_t count = 0;
    need(bool(std::getline(in, magic)) && magic == "REINDEX_AM_EXPECTED_V1", "expected-state header");
    need(bool(in >> value.relation >> value.bloomBits >> value.bloomHashes >> count), "expected state fields");
    for (size_t i = 0; i < count; ++i) { int id = 0; int64_t rid = 0; need(bool(in >> id >> rid), "actual expected RID"); need(value.rids.emplace(id, rid).second, "unique expected id"); }
    need(bool(in >> count), "metadata count");
    for (size_t i = 0; i < count; ++i) { std::string name, encoded; need(bool(in >> std::quoted(name) >> std::quoted(encoded)), "actual metadata bytes"); value.metadata.emplace(name, unhex(encoded)); }
    std::string trailing; need(!(in >> trailing) && in.eof(), "complete expected state"); return value;
}
Expected verify(bool mixed, const Expected* original = nullptr) {
    need(!g_engine.inTransaction(), "verification has no current native transaction");
    const auto schema = g_engine.getTableSchema(DB, TABLE);
    need(schema.len == 6 && schema.physicalRelationId, "genuine complete physical schema");
    Expected result; result.relation = schema.physicalRelationId; result.metadata = metadata();
    need(g_engine.forEachRow(DB, TABLE, [&](uint32_t page, uint16_t slot, const char* data, size_t length) {
        const auto rid = StorageEngine::encodeRid(page, slot); const std::string raw(data, length);
        const int id = std::stoi(g_engine.extractColumnValue(raw, schema, 0, DB));
        need(id >= 1 && id <= ROWS && result.rids.emplace(id, rid).second, "actual unique complete heap row");
        const auto bitmap = detail::storedRowNullBitmap(g_engine, DB, TABLE, schema, rid);
        need(bitmap && bitmap->size() == schema.len, "actual heap-owned NULL bitmap");
        const auto wanted = cells(model(id));
        std::cout << "REINDEX_AM_ROW " << id << " rid=" << rid;
        for (size_t col = 0; col < schema.len; ++col) {
            bool isNull = false; const auto actual = g_engine.extractColumnValue(raw, schema, col, DB, true, &isNull);
            need(isNull == !wanted[col] && isNull == (*bitmap)[col], "true NULL distinct from empty");
            need(actual == wanted[col].value_or(""), "full stored cell exact " + schema.cols[col].dataName);
            std::cout << ' ' << col << ':' << isNull << ':' << hex(actual);
        }
        std::cout << '\n';
    }), "whole actual heap scan");
    need(result.rids.size() == ROWS, "all independent model rows present");
    if (original) need(result.rids == original->rids && result.metadata == original->metadata && result.relation == original->relation, "cold data/RIDs/metadata/configuration identity unchanged");
    auto* primary = g_engine.getPKIndex(DB, TABLE); need(primary && primary->isOpen(), "real primary generation");
    relationRoot = fs::canonical(primary->filePath().parent_path());
    std::vector<int64_t> allPrimary;
    need(primary->allValuesChecked(allPrimary) && allPrimary.size() == ROWS, "complete primary membership");
    std::vector<int64_t> wantedPrimary;
    for (const auto& row : result.rids) wantedPrimary.push_back(row.second);
    need(sorted(allPrimary) == sorted(wantedPrimary), "primary has no extra or missing actual RID");
    for (const auto& row : result.rids) { int64_t rid = 0; need(primary->search(std::to_string(row.first), rid) && rid == row.second, "real primary RID"); }
    auto* secondary = g_engine.getSecondaryIndex(DB, TABLE, "txt"); need(secondary && secondary->isOpen(), "actual Btree control");
    for (const auto& key : {std::string("key0"), std::string("key1"), std::string("key2"), std::string("NULL")}) {
        std::vector<int64_t> wanted, actual;
        for (int id = 1; id <= ROWS; ++id) if (model(id).text == Cell(key)) wanted.push_back(result.rids.at(id));
        need(secondary->searchMultiChecked(key, actual) && sorted(actual) == sorted(wanted), "Btree control duplicates exact");
    }
    std::vector<int64_t> wantedSecondary, allSecondary;
    for (int id = 1; id <= ROWS; ++id) if (model(id).text && !model(id).text->empty()) wantedSecondary.push_back(result.rids.at(id));
    need(secondary->allValuesChecked(allSecondary) && sorted(allSecondary) == sorted(wantedSecondary), "original Btree nonempty-key policy complete, no extra RID");
    if (mixed) {
        need(!fs::exists(databaseRoot / "items.specialized_index_dirty"), "healthy generation, not dirty heap-fallback disguise");
        auto* hash = g_engine.getHashIndex(DB, TABLE, "txt"); auto* bloom = g_engine.getBloomIndex(DB, TABLE, "txt");
        need(hash && bloom && hash->isOpen() && bloom->isOpen(), "real hash/bloom generation");
        result.bloomBits = bloom->bitCount(); result.bloomHashes = bloom->hashCount();
        if (original) need(result.bloomBits == original->bloomBits && result.bloomHashes == original->bloomHashes, "real Bloom persisted parameters unchanged");
        for (const auto& key : {std::string(), std::string("NULL"), std::string("key0"), std::string("key1"), std::string("key2"), std::string("missing")}) {
            std::vector<int64_t> wanted;
            for (int id = 1; id <= ROWS; ++id) if (model(id).text == Cell(key)) wanted.push_back(result.rids.at(id));
            need(sorted(hash->search(key)) == sorted(wanted), "hash includes genuine empty but excludes NULL");
            need(sorted(bloom->search(key)) == sorted(wanted), "Bloom exact side map includes genuine empty but excludes NULL");
        }
        // Text-empty GiST encoding remains a separate strict known gap.
        // This recovery fixture covers the genuine nullable numeric GiST
        // representation; it does not whitelist missing empty-text entries.
        for (const auto& key : {std::string("-40"), std::string("0"), std::string("40"), std::string("999")}) {
            const auto gist = g_engine.giSTSearchOverlap(DB, TABLE, "n", key, key);
            for (int id = 1; id <= ROWS; ++id) if (model(id).n == Cell(key))
                need(std::find(gist.begin(), gist.end(), result.rids.at(id)) != gist.end(), "GiST no missing actual numeric equality candidate");
        }
        for (const auto& token : {std::string("alpha"), std::string("beta"), std::string("gamma"), std::string("missing")}) {
            std::vector<int64_t> wanted;
            for (int id = 1; id <= ROWS; ++id) { const auto value = model(id).body; if (value && value->find(token) != std::string::npos) wanted.push_back(result.rids.at(id)); }
            need(sorted(g_engine.fullTextSearch(DB, TABLE, "body", token)) == sorted(wanted), "fulltext complete token RID map");
            need(sorted(g_engine.ginSearch(DB, TABLE, "body", token)) == sorted(wanted), "GIN complete token RID map");
        }
        for (const auto& point : {std::string("0,0"), std::string("1,1")}) {
            const auto candidates = g_engine.spGiSTSearch(DB, TABLE, "location", "=", point);
            for (int id = 1; id <= ROWS; ++id) if (model(id).point == Cell(point))
                need(std::find(candidates.begin(), candidates.end(), result.rids.at(id)) != candidates.end(), "SPGiST actual matching point RID");
        }
        const auto brinPath = findFile("items_n.brin"); brin_index_format::TypedIndex index;
        need(brin_index_format::decodeTyped(bytes(brinPath), index) && index.pagesPerRange == 3 && index.physicalRelation == result.relation, "actual V3 CRC and original BRIN width3/relation");
        const auto ranges = g_engine.tryBrinPageRanges(DB, TABLE, "n", ">=", "40");
        need(bool(ranges), "current summary genuinely usable");
        for (int id = 1; id <= ROWS; ++id) if (model(id).n && std::stoi(*model(id).n) >= 40) {
            uint32_t page = 0; uint16_t slot = 0; StorageEngine::decodeRid(result.rids.at(id), page, slot);
            need(std::any_of(ranges->begin(), ranges->end(), [&](auto range) { return range.first <= page && page <= range.second; }), "BRIN contains every genuine matching page");
        }
        const std::vector<std::tuple<std::string, std::string, std::string>> names{
            {"rx_hash", "hash", "txt"}, {"rx_bloom", "bloom", "txt"}, {"rx_fti", "fulltext", "body"},
            {"rx_gin", "gin", "body"}, {"rx_gist", "gist", "n"}, {"rx_spgist", "spgist", "location"}, {"rx_brin", "brin", "n"}};
        for (const auto& item : names) { auto info = g_engine.getNamedIndex(DB, TABLE, std::get<0>(item)); need(info && info->accessMethod == std::get<1>(item) && info->key == std::get<2>(item), "actual method/name/key metadata preserved"); }
        for (const auto& extension : {".hidx", ".bidx", ".fti", ".gin", ".gist", ".spgist", ".brin"}) {
            const std::string column = std::string(extension) == ".fti" || std::string(extension) == ".gin" ? "body" : std::string(extension) == ".spgist" ? "location" : std::string(extension) == ".brin" || std::string(extension) == ".gist" ? "n" : "txt";
            const auto path = findFile("items_" + column + extension);
            need(!fs::exists(path.string() + ".tde"), "bare AM stays bare; no synthetic TDE sidecar");
        }
    }
    noTemporary(); return result;
}

void worker(const std::string& phase, const fs::path& root, bool mixed) {
    const auto cluster = fs::canonical(fs::current_path()); const char* env = std::getenv("DBMS_DATA_DIR");
    need(env && fs::equivalent(cluster, env) && cluster.parent_path() == root, "owned cwd/env established before global engine constructor");
    TypeRegistry::instance().bootstrap(); g_config.autoAnalyzeEnabled = false; g_config.autoVacuumEnabled = false;
    g_engine.setBackgroundIntervals(60000, 60000);
    Session session; session.username = "admin"; session.permission = 1; session.currentDB = DB;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct RestoreSession { Session* old; ~RestoreSession() { setCurrentSession(old); } } restore{previous};
    evidenceRoot = root; databaseRoot = fs::absolute(g_engine.dbPath(DB)).lexically_normal(); markerPath = databaseRoot / ".items.reindex_swap";
    const auto expectedPath = root.parent_path() / (mixed ? "mixed.expected" : "legacy.expected");
    if (phase == "seed") {
        need(g_engine.createDatabase(DB, "utf8") == DBStatus::OK, "real private database"); DdlExecutor ddl;
        need(!ddl.executeSql("CREATE TABLE items(id integer PRIMARY KEY,n bigint,txt text COLLATE \"C\",body text,location point,pad text)", session), "actual table DDL");
        need(g_engine.beginTransaction(DB) == DBStatus::OK, "actual seed transaction");
        for (int id = 1; id <= ROWS; ++id) { const auto row = model(id); need(g_engine.insertRow(DB, TABLE, {{"id", std::to_string(id)}, {"n", row.n}, {"txt", row.text}, {"body", row.body}, {"location", row.point}, {"pad", row.pad}}) == DBStatus::OK, "actual stored fixture row"); }
        need(g_engine.commitTransaction() == DBStatus::OK, "actual fixture durable commit");
        need(!ddl.executeSql("CREATE INDEX rx_btree ON items(txt)", session), "actual Btree control DDL");
        if (mixed) for (const auto& sql : {"CREATE INDEX rx_hash ON items USING hash(txt)", "CREATE INDEX rx_bloom ON items USING bloom(txt)", "CREATE INDEX rx_fti ON items USING fulltext(body)", "CREATE INDEX rx_gin ON items USING gin(body)", "CREATE INDEX rx_gist ON items USING gist(n)", "CREATE INDEX rx_spgist ON items USING spgist(location)", "CREATE INDEX rx_brin ON items USING brin(n) WITH(pages_per_range=3)"}) need(!ddl.executeSql(sql, session), "actual AM SQL definition");
        // Fixed fixture step, not a retry: seed a known complete generation
        // through the real new REINDEX before testing interrupted replacements.
        need(g_engine.reindex(DB, TABLE) == DBStatus::OK, "seed normal complete REINDEX");
        const auto expected = verify(mixed); saveExpected(expectedPath, expected);
        need(g_engine.checkpoint(DB), "normal seed checkpoint before process closes"); return;
    }
    need(g_engine.recoverAllDatabases(), "real public startup recovery");
    const auto expected = loadExpected(expectedPath);
    if (phase == "cold") { verify(mixed, &expected); need(g_engine.checkpoint(DB), "normal recovered checkpoint"); return; }
    verify(mixed, &expected);
    action = phase; fired = false; armed = true;
    const auto status = g_engine.reindex(DB, TABLE);
    armed = false;
    const bool failure = phase == "fail-private-fsync" || phase == "fail-live-rename";
    need(status == (failure ? DBStatus::IO_ERROR : DBStatus::OK), "actual ordinary REINDEX status");
    need(failure ? fired : phase == "normal" && !fired, "exact intended event fired, no fake interruption pass");
    verify(mixed, &expected); need(g_engine.checkpoint(DB), "normal post-control checkpoint");
}

void cloneClosed(const fs::path& from, const fs::path& to) {
    need(!fs::exists(to), "fresh clone destination");
    for (const auto& entry : fs::recursive_directory_iterator(from)) need(!entry.is_symlink(), "closed fixture has no external symlink");
    fs::copy(from, to, fs::copy_options::recursive);
}
void spawn(const fs::path& executable, const fs::path& root, const fs::path& cluster,
           const std::string& phase, bool mixed, bool expectKill) {
    std::vector<std::string> envStrings;
    for (char** cursor = environ; *cursor; ++cursor) if (std::string(*cursor).rfind("DBMS_", 0) != 0) envStrings.emplace_back(*cursor);
    envStrings.push_back("DBMS_DATA_DIR=" + cluster.string());
    std::vector<char*> env; for (auto& value : envStrings) env.push_back(value.data()); env.push_back(nullptr);
    std::vector<std::string> arguments{executable.string(), "--worker", phase, root.string(), mixed ? "mixed" : "legacy"};
    std::vector<char*> argv; for (auto& value : arguments) argv.push_back(value.data()); argv.push_back(nullptr);
    posix_spawn_file_actions_t actions; need(posix_spawn_file_actions_init(&actions) == 0, "spawn file actions");
    need(posix_spawn_file_actions_addchdir_np(&actions, cluster.c_str()) == 0, "cwd set before exec/global engine");
    const auto out = root / (phase + ".stdout"), err = root / (phase + ".stderr");
    need(posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, out.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600) == 0 &&
         posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, err.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600) == 0, "fresh complete child streams");
    posix_spawnattr_t attributes; need(posix_spawnattr_init(&attributes) == 0, "spawn attributes");
    need(posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETPGROUP) == 0 && posix_spawnattr_setpgroup(&attributes, 0) == 0, "owned child process group");
    pid_t pid = 0; const int error = posix_spawn(&pid, executable.c_str(), &actions, &attributes, argv.data(), env.data());
    posix_spawn_file_actions_destroy(&actions); posix_spawnattr_destroy(&attributes);
    need(error == 0 && pid > 0, "actual child spawned");
    int status = 0; bool reaped = false;
    struct Cleanup { pid_t pid; bool& reaped; ~Cleanup() { if (!reaped) { ::kill(pid, SIGKILL); int status = 0; while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {} } } } cleanup{pid, reaped};
    std::cout << "REINDEX_AM_CHILD phase=" << phase << " pid=" << pid << " stdout=" << out << " stderr=" << err << '\n' << std::flush;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    for (;;) { const auto waited = waitpid(pid, &status, WNOHANG); if (waited == pid) { reaped = true; break; }
        need(waited == 0 || (waited < 0 && errno == EINTR), "wait exact owned child");
        need(std::chrono::steady_clock::now() < deadline, "owned worker timeout is failure, never a normal pass");
        std::this_thread::sleep_for(std::chrono::milliseconds(5)); }
    std::cout << "REINDEX_AM_CHILD_TERMINAL phase=" << phase << " status=" << status << " intentional_kill=" << expectKill << '\n' << std::flush;
    need(expectKill ? WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL : WIFEXITED(status) && WEXITSTATUS(status) == 0, "exact intended child terminal state");
    need(::kill(pid, 0) == -1 && errno == ESRCH, "actual reaped PID absent");
}
void parent(const fs::path& executable) {
    std::string pattern = (fs::temp_directory_path() / "dbms-reindex-am-XXXXXX").string();
    std::vector<char> name(pattern.begin(), pattern.end()); name.push_back(0);
    const char* created = mkdtemp(name.data()); need(created != nullptr, "fresh owned experiment root");
    const auto root = fs::canonical(created); std::cout << "REINDEX_AM_OWNED_ROOT " << root << '\n' << std::flush;
    const auto mixedSeed = root / "mixed-seed", legacySeed = root / "legacy-seed";
    fs::create_directory(mixedSeed); fs::create_directory(legacySeed);
    const auto mixedCluster = mixedSeed / "cluster", legacyCluster = legacySeed / "cluster";
    fs::create_directory(mixedCluster); fs::create_directory(legacyCluster);
    spawn(executable, mixedSeed, mixedCluster, "seed", true, false);
    spawn(executable, legacySeed, legacyCluster, "seed", false, false);
    size_t cases = 0, intentional = 0;
    for (const auto& phase : {"normal", "kill-marker-durable", "kill-bare-backup", "kill-bare-publish", "kill-all-published-durable", "fail-private-fsync", "fail-live-rename"}) {
        const auto caseRoot = root / phase; fs::create_directory(caseRoot);
        const auto cluster = caseRoot / "cluster"; cloneClosed(mixedCluster, cluster);
        const bool killed = std::string(phase).rfind("kill-", 0) == 0;
        spawn(executable, caseRoot, cluster, phase, true, killed);
        if (killed) {
            ++intentional; const auto actual = bytes(caseRoot / "actual-marker.txt");
            need(actual.rfind("DBMS_REINDEX_SWAP_V2\n", 0) == 0, "actual mixed producer used V2 before intentional kill");
            need(fs::is_regular_file(cluster / DB / ".items.reindex_swap"), "interrupted marker exists before cold recovery");
        }
        spawn(executable, caseRoot, cluster, "cold", true, false); ++cases;
    }
    const auto legacy = root / "legacy-kill-marker-durable"; fs::create_directory(legacy);
    const auto cluster = legacy / "cluster"; cloneClosed(legacyCluster, cluster);
    spawn(executable, legacy, cluster, "kill-marker-durable", false, true);
    need(bytes(legacy / "actual-marker.txt").rfind("DBMS_REINDEX_SWAP_V1\n", 0) == 0, "all-paired normal producer retains exact legacy V1 role");
    spawn(executable, legacy, cluster, "cold", false, false); ++cases; ++intentional;
    need(cases == 8 && intentional == 5, "fixed complete recovery cohort");
    std::cout << "REINDEX_AM_RECOVERY_COMPLETE controls=" << cases << " intentional_sigkill=" << intentional
              << " normal_workers=13 workers=18 physical_rows=" << ROWS << " fabricated_markers=0 owned=" << root << '\n';
}
} // namespace

extern "C" int __wrap_fsync(int descriptor) {
    if (armed && !fired && !insideHook && action == "fail-private-fsync") {
        const fs::path path(fdPath(descriptor));
        if (under(path, databaseRoot) && path.extension() != ".tde" &&
            path.string().find(".reindex.tmp.") != std::string::npos && !fs::exists(markerPath)) {
            fired = true; event("private-temp-fsync-EIO"); errno = EIO; return -1;
        }
    }
    const int result = __real_fsync(descriptor);
    if (result == 0 && armed && !fired && !insideHook && fs::is_regular_file(markerPath)) {
        const fs::path path(fdPath(descriptor));
        if (path == databaseRoot || path == relationRoot) {
            const auto marker = actualMarker();
            bool anyBackup = false; for (const auto& entry : marker.entries) anyBackup = anyBackup || fs::exists(entry.backup);
            if (action == "kill-marker-durable" && !anyBackup) crash("marker-directory-fsync-complete");
            if (action == "kill-all-published-durable" && path == relationRoot && anyBackup && allPublished(marker)) crash("all-targets-directory-fsync-complete");
        }
    }
    return result;
}
extern "C" void wrappedReindexRename(const fs::path& from, const fs::path& to, std::error_code& error) noexcept
    asm("__wrap__ZNSt10filesystem6renameERKNS_7__cxx114pathES3_RSt10error_code");
extern "C" void wrappedReindexRename(const fs::path& from, const fs::path& to, std::error_code& error) noexcept {
    try {
        const bool active = armed && !fired && !insideHook && fs::is_regular_file(markerPath);
        const bool backup = active && ownedTarget(from) && to.string().find(".reindex.old.") != std::string::npos;
        const bool publication = active && from.string().find(".reindex.tmp.") != std::string::npos && ownedTarget(to);
        if (backup && action == "fail-live-rename") { fired = true; event("live-target-rename-EIO"); error = std::make_error_code(std::errc::io_error); return; }
        realReindexRename(from, to, error);
        if (error || !active) return;
        if (backup && bare(from) && action == "kill-bare-backup") crash("actual-bare-backup-rename-complete");
        if (publication && bare(to) && action == "kill-bare-publish") crash("actual-bare-publication-rename-complete");
    } catch (...) { _exit(124); }
}
int main(int argc, char** argv) {
    try {
        if (argc == 5 && std::string(argv[1]) == "--worker") {
            const std::string mode = argv[4]; need(mode == "mixed" || mode == "legacy", "fixed actual fixture mode");
            worker(argv[2], fs::canonical(argv[3]), mode == "mixed");
            std::cout << "REINDEX_AM_WORKER_CHECKS=" << checks << " FAILED=0\n"; return 0;
        }
        need(argc == 1, "fixed no-argument native entry"); parent(fs::canonical("/proc/self/exe"));
        std::cout << "Tests run: " << checks << "\nTests failed: 0\n"; return 0;
    } catch (const std::exception& error) {
        armed = false; std::cerr << "REINDEX_AM_RECOVERY_FAIL checks=" << checks << " reason=" << error.what() << '\n';
        std::cout << "Tests run: " << checks << "\nTests failed: 1\n"; return 1;
    }
}
