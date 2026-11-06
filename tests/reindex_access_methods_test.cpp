// Genuine REINDEX TABLE: file publication, definitions, heap ownership and
// active-writer maintenance across the currently supported access methods.
// Fixed cohort: 17 ordinary scenarios, including 11 real peer transactions
// and one normal SAVEPOINT restoration. Native-only declaration loss is NOT A PASS.
#include "access/BPTree.h"
#include "access/BloomIndex.h"
#include "access/BrinIndexFormat.h"
#include "access/GiSTIndexFormat.h"
#include "access/GinIndexFormat.h"
#include "access/HashIndex.h"
#include "access/SPGiSTIndexFormat.h"
#include "catalog/CatalogService.h"
#include "catalog/catalog.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "utils/Session.h"
#include "test_utils.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <exception>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using namespace dbms;
namespace fs = std::filesystem;
using Cell = std::optional<std::string>;
const auto db = testDbPath("reindex_access_methods");
constexpr const char* table = "am";
size_t checks = 0, failures = 0, cases = 0;

void need(bool value, const std::string& label) {
    ++checks;
    if (!value) {
        ++failures;
        std::cerr << "REINDEX_AM_CHECK_FAIL " << label << '\n';
    }
}
void require(bool value, const std::string& label) {
    need(value, label);
    if (!value) throw std::runtime_error("unsafe fixture continuation: " + label);
}
std::string bytes(const fs::path& path) {
    if (!fs::is_regular_file(path)) return {};
    std::ifstream input(path, std::ios::binary);
    if (!input) return {};
    std::string result{std::istreambuf_iterator<char>(input), {}};
    require(!input.bad(), "complete owned file read");
    return result;
}
void write(const fs::path& path, const std::string& value) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(value.data(), static_cast<std::streamsize>(value.size()));
    output.close();
    require(!output.fail(), "complete owned fixture file write");
}
uint64_t number(const std::string& value, size_t offset, size_t width) {
    require(offset <= value.size() && width <= value.size() - offset && width <= 8,
            "bounded independent integer read");
    uint64_t result = 0;
    for (size_t i = 0; i < width; ++i)
        result |= uint64_t(static_cast<unsigned char>(value[offset + i])) << (8 * i);
    return result;
}
uint32_t crc(const std::string& value, size_t length) {
    uint32_t result = 0xffffffffu;
    for (size_t i = 0; i < length; ++i) {
        result ^= static_cast<unsigned char>(value[i]);
        for (unsigned bit = 0; bit < 8; ++bit)
            result = (result >> 1) ^ ((result & 1) ? 0x82f63b78u : 0);
    }
    return result ^ 0xffffffffu;
}
bool checkedCrc(const std::string& value) {
    return value.size() >= 4 && number(value, value.size() - 4, 4) == crc(value, value.size() - 4);
}
std::vector<int64_t> sorted(std::vector<int64_t> value) {
    std::sort(value.begin(), value.end()); return value;
}
bool contains(const std::vector<int64_t>& values, int64_t rid) {
    return std::find(values.begin(), values.end(), rid) != values.end();
}
std::string upper(std::string value) {
    for (char& c : value) c = char(std::toupper(static_cast<unsigned char>(c)));
    return value;
}
struct Row {
    int id;
    Cell text, n;
    bool pointNull = false;
    std::string pad;
};
using Model = std::map<int, Row>;
struct PhysicalRow {
    int64_t rid;
    std::string raw;
    std::vector<std::string> values;
    std::vector<bool> nulls;
    bool operator==(const PhysicalRow& other) const {
        return rid == other.rid && raw == other.raw && values == other.values && nulls == other.nulls;
    }
};
using Heap = std::map<int, PhysicalRow>;
Row seedRow(int id) {
    Cell text;
    switch (id % 8) {
    case 1: text = ""; break;
    case 2: text = "NULL"; break;
    case 3: break;
    case 4: text = "alpha 世界"; break;
    case 5: text = "alpha beta"; break;
    case 6: text = "omega"; break;
    case 7: text = "toast " + std::string(6000, 'z'); break;
    default: text = "alpha"; break;
    }
    return {id, text, id % 8 == 3 ? Cell{} : Cell(std::to_string(9007199254740992LL + id)),
            id % 8 == 3, std::string(768, char('a' + id % 23))};
}
void insert(const Row& row) {
    const Cell point = row.pointNull ? Cell{} : Cell("(" + std::to_string(row.id) + "," + std::to_string(row.id) + ")");
    require(g_engine.insertRow(db, table,
        {{"id", std::to_string(row.id)}, {"txt", row.text}, {"n", row.n},
         {"n3", row.n}, {"n128", row.n}, {"p", point}, {"pad", row.pad}}) == DBStatus::OK,
        "actual owned nullable/empty/TOAST seed row");
}
Heap heap(StorageEngine& engine, const Model& expected) {
    auto& locks = engine.getLockManager();
    locks.setResourceNamespace(db);
    require(locks.lockShared(table), "actual heap cursor relation lock");
    struct Unlock { LockManager& locks; ~Unlock() { locks.unlock(table); } } unlock{locks};
    const auto schema = engine.getTableSchema(db, table);
    require(schema.len == 7, "actual complete heap declaration");
    auto cursor = engine.beginHeapScan(db, table, schema);
    HeapScanRow input;
    Heap result;
    while (cursor->next(input)) {
        std::vector<std::string> values;
        std::vector<bool> nulls;
        detail::decodeStoredRowValues(engine, db, input.data, schema, input.nulls, values, nulls);
        require(values.size() == 7 && nulls == input.nulls, "same-tuple captured physical NULL bitmap");
        const int id = std::stoi(values[0]);
        require(result.emplace(id, PhysicalRow{input.rid, input.data, values, nulls}).second,
                "unique actual heap occurrence");
        const auto wanted = expected.find(id);
        need(wanted != expected.end(), "no unexpected heap row");
        if (wanted == expected.end()) continue;
        const auto& row = wanted->second;
        need(!nulls[0] && nulls[1] == !row.text && (!row.text || values[1] == *row.text),
             "full real text/SQL NULL/empty/literal NULL/UTF8/TOAST value");
        for (size_t column : {size_t(2), size_t(3), size_t(4)})
            need(nulls[column] == !row.n && (!row.n || values[column] == *row.n),
                 "all exact bigint values above 2^53 and SQL NULLs");
        need(nulls[5] == row.pointNull && !nulls[6] && values[6] == row.pad,
             "complete point NULL and full physical padding");
        uint32_t page = 0; uint16_t slot = 0;
        StorageEngine::decodeRid(input.rid, page, slot);
        need(page > 0 && StorageEngine::encodeRid(page, slot) == input.rid, "genuine nonfabricated heap RID");
        std::cout << "REINDEX_AM_ROW id=" << id << " rid=" << input.rid << " nulls=";
        for (bool bit : nulls) std::cout << bit;
        std::cout << " text_bytes=" << values[1].size() << " n=" << values[2] << '\n';
    }
    need(result.size() == expected.size(), "complete actual row count with every value checked");
    return result;
}
struct Definition {
    std::string name, method, key, suffix;
    bool checksummed;
};
const std::vector<Definition> definitions{
    {"am_n", "btree", "n", "_n.idx", false},
    {"am_pair", "composite", "am_pair", ".idx_am_pair", false},
    {"am_expr", "btree", "", "", false},
    {"am_partial", "btree", "id", "_id.idx", false},
    {"am_hash", "hash", "txt", "_txt.hidx", true},
    {"am_bloom", "bloom", "txt", "_txt.bidx", true},
    {"am_fti", "fulltext", "txt", "_txt.fti", false},
    {"am_gin", "gin", "txt", "_txt.gin", true},
    {"am_gist", "gist", "txt", "_txt.gist", true},
    {"am_spgist", "spgist", "p", "_p.spgist", true},
    {"am_brin1", "brin", "n", "_n.brin", true},
    {"am_brin3", "brin", "n3", "_n3.brin", true},
    {"am_brin128", "brin", "n128", "_n128.brin", true}
};
std::string expressionKey;
fs::path root() { return g_engine.dbPath(db); }
fs::path file(const Definition& definition) {
    return root() / (std::string(table) + (definition.name == "am_expr" ? "_" + expressionKey + ".idx" : definition.suffix));
}
struct Identity {
    std::map<std::string, std::string> files;
    std::map<std::string, uint32_t> classes;
};
Identity identities() {
    Identity result;
    for (const auto& extension : {".idxnames", ".secidx", ".hashidx", ".bloomidx", ".stc"}) {
        const auto path = root() / (std::string(table) + extension);
        require(fs::is_regular_file(path), "actual persistent definition file");
        result.files[path.filename().string()] = bytes(path);
    }
    auto& catalog = g_engine.catalogService().get(db);
    const auto ns = catalog.getNamespaceByName("public");
    require(bool(ns), "actual catalog namespace");
    const auto target = catalog.getClassByName(table, ns->oid);
    require(target && target->relkind == 'r', "actual catalog target identity");
    result.classes["table:am"] = target->oid;
    for (const auto& definition : definitions) {
        const auto named = g_engine.getNamedIndex(db, table, definition.name);
        require(named && named->accessMethod == definition.method &&
                named->key == (definition.name == "am_expr" ? expressionKey : definition.key), "genuine named AM/key definition");
        const auto index = catalog.getClassByName(definition.name, ns->oid);
        require(index && index->relkind == 'i', "real catalog index class identity");
        result.classes[definition.name] = index->oid;
    }
    for (const auto& entry : catalog.listClasses())
        if (entry.relnamespace == ns->oid && entry.relkind == 'i')
            result.classes[entry.relname] = entry.oid; // Includes the genuine PK class when published.
    return result;
}
void sameIdentity(const Identity& original) {
    const auto current = identities();
    need(current.files == original.files, "index/schema definition bytes unchanged");
    need(current.classes == original.classes, "every genuine catalog index OID unchanged");
}
struct Generation { uint64_t device = 0, inode = 0; bool exists = false; };
Generation generation(const fs::path& path) {
    struct stat status {};
    if (::lstat(path.c_str(), &status) != 0) return {};
    return {uint64_t(status.st_dev), uint64_t(status.st_ino), S_ISREG(status.st_mode)};
}
bool newGeneration(Generation before, Generation after) {
    return after.exists && (!before.exists || before.device != after.device || before.inode != after.inode);
}
bool noArtifacts() {
    for (const auto& entry : fs::recursive_directory_iterator(root())) {
        const auto name = entry.path().filename().string();
        if (name.find(".reindex.tmp.") != std::string::npos || name.find(".reindex.old.") != std::string::npos ||
            name.find(".reindex_swap") != std::string::npos || name.find(".reindex_am") != std::string::npos) return false;
    }
    return true;
}
void closeLiveTrees() {
    if (auto* index = g_engine.getPKIndex(db, table)) index->close();
    for (const auto& key : {std::string("n"), std::string("id"), expressionKey})
        if (auto* index = g_engine.getSecondaryIndex(db, table, key)) index->close();
    if (auto* index = g_engine.getCompositeIndexTree(db, table, "am_pair")) index->close();
    if (auto* index = g_engine.getHashIndex(db, table, "txt")) require(index->close(), "close actual hash cache");
    if (auto* index = g_engine.getBloomIndex(db, table, "txt")) require(index->close(), "close actual bloom cache");
}
bool alpha(const Row& row) { return row.text && row.text->rfind("alpha", 0) == 0; }
std::map<std::string, std::vector<int64_t>> expectedText(const Heap& physical, const Model& model) {
    std::map<std::string, std::vector<int64_t>> result;
    for (const auto& entry : model) if (entry.second.text) result[*entry.second.text].push_back(physical.at(entry.first).rid);
    return result;
}
void verifyTrees(StorageEngine& engine, const Model& model, const Heap& physical) {
    auto* primary = engine.getPKIndex(db, table);
    need(primary && primary->isOpen(), "actual primary replacement readable");
    if (primary) for (const auto& row : physical) {
        int64_t actual = -1;
        need(primary->search(std::to_string(row.first), actual) && actual == row.second.rid, "all actual primary mappings");
    }
    if (primary) {
        std::vector<int64_t> actual, expected;
        for (const auto& row : physical) expected.push_back(row.second.rid);
        need(primary->allValuesChecked(actual) && sorted(actual) == sorted(expected), "complete primary membership, no extra/deleted RID");
    }
    for (const auto& key : {std::string("n"), std::string("id"), expressionKey, std::string("am_pair")}) {
        const bool composite = key == "am_pair";
        auto* index = composite ? engine.getCompositeIndexTree(db, table, key) : engine.getSecondaryIndex(db, table, key);
        need(index && index->isOpen(), "actual secondary/expression/partial/composite readable: " + key);
        if (!index) continue;
        std::map<std::string, std::vector<int64_t>> expected;
        for (const auto& pair : model) {
            const auto& row = pair.second;
            std::string encoded;
            if (key == "id") { if (row.id < 4) continue; encoded = std::to_string(row.id); }
            else if (key == expressionKey) encoded = upper(row.text.value_or(""));
            else if (key == "n") encoded = row.n.value_or("");
            else { encoded = row.n.value_or(""); if (!encoded.empty()) encoded += '\1'; encoded += std::to_string(row.id); }
            if (!encoded.empty()) expected[encoded].push_back(physical.at(pair.first).rid);
        }
        std::vector<int64_t> allExpected;
        for (const auto& entry : expected) {
            std::vector<int64_t> actual;
            need(index->searchMultiChecked(entry.first, actual) && sorted(actual) == sorted(entry.second), "complete checked Btree membership: " + key);
            allExpected.insert(allExpected.end(), entry.second.begin(), entry.second.end());
        }
        std::vector<int64_t> allActual;
        need(index->allValuesChecked(allActual) && sorted(allActual) == sorted(allExpected), "no extra or lost Btree entries: " + key);
    }
}
std::optional<std::map<std::string, std::vector<int64_t>>> ginImage(const std::string& image) {
    if (image.size() < 24 || image.substr(0, 8) != std::string("\0DBMSG\0002", 8) || !checkedCrc(image)) return {};
    if (number(image, 8, 4) != 2) return {};
    size_t offset = 20;
    const auto count = number(image, 12, 8);
    std::map<std::string, std::vector<int64_t>> result;
    for (uint64_t i = 0; i < count; ++i) {
        if (offset > image.size() - 4 || image.size() - 4 - offset < 8) return {};
        const auto length = number(image, offset, 8); offset += 8;
        if (length > image.size() - 4 - offset) return {};
        auto key = image.substr(offset, size_t(length)); offset += size_t(length);
        if (image.size() - 4 - offset < 8) return {};
        const auto n = number(image, offset, 8); offset += 8;
        if (n > (image.size() - 4 - offset) / 8) return {};
        auto& rids = result[key];
        for (uint64_t j = 0; j < n; ++j) { rids.push_back(int64_t(number(image, offset, 8))); offset += 8; }
    }
    if (offset != image.size() - 4) return {};
    return result;
}
void verifyIndexes(StorageEngine& engine, const Model& model, const Heap& physical,
                   const std::array<size_t, 3>& widths, uint32_t bloomHashes) {
    verifyTrees(engine, model, physical);
    const auto text = expectedText(physical, model);
    for (const auto& kind : {std::string("hash"), std::string("bloom")}) {
        const auto path = root() / (std::string(table) + "_txt." + (kind == "hash" ? "hidx" : "bidx"));
        need(fs::is_regular_file(path) && checkedCrc(bytes(path)), "independent physical checksum: " + kind);
        if (kind == "hash") {
            HashIndex direct(path); const bool opened = direct.openExisting(); need(opened, "real hash file independent reopen");
            if (opened) {
                need(direct.size() == text.size(), "encoded hash has exactly the real non-NULL keys");
                for (const auto& entry : text) need(sorted(direct.search(entry.first)) == sorted(entry.second), "encoded hash full NULL/empty/UTF8/TOAST membership");
            }
            direct.close();
            auto* cached = engine.getHashIndex(db, table, "txt"); need(cached, "actual engine hash receiver");
            if (cached) for (const auto& entry : text) need(sorted(cached->search(entry.first)) == sorted(entry.second), "engine hash complete real RID membership");
        } else {
            BloomIndex direct(path); const bool opened = direct.openExisting(); need(opened, "real bloom file independent reopen");
            if (opened) {
                need(direct.hashCount() == bloomHashes, "actual retained/fallback bloom hash configuration");
                need(direct.size() == text.size(), "encoded bloom has exactly the real non-NULL keys");
                for (const auto& entry : text) need(sorted(direct.search(entry.first)) == sorted(entry.second), "encoded bloom includes every real non-NULL key, including empty");
            }
            direct.close();
            auto* cached = engine.getBloomIndex(db, table, "txt"); need(cached, "actual engine bloom receiver");
            if (cached) for (const auto& entry : text) need(sorted(cached->search(entry.first)) == sorted(entry.second), "engine bloom complete real RID membership");
        }
    }
    const auto gin = ginImage(bytes(root() / "am_txt.gin"));
    need(bool(gin), "independent encoded GIN complete frame and CRC");
    std::map<std::string, std::vector<int64_t>> fti;
    { std::istringstream input(bytes(root() / "am_txt.fti")); std::string line;
      while (std::getline(input, line)) { std::istringstream item(line); std::string key; int64_t rid; if (!(item >> key)) continue; while (item >> rid) fti[key].push_back(rid); } }
    std::vector<gist_index_format::Entry> gist;
    const auto gistBytes = bytes(root() / "am_txt.gist");
    const bool gistValid = checkedCrc(gistBytes) && gist_index_format::decodeV2(gistBytes, gist);
    need(gistValid, "actual GiST full encoded frame and independent CRC");
    std::vector<spgist_index_format::Entry> points;
    const auto pointBytes = bytes(root() / "am_p.spgist");
    const bool pointValid = checkedCrc(pointBytes) && spgist_index_format::decodeV2(pointBytes, points);
    need(pointValid, "actual SPGiST full encoded frame and independent CRC");
    for (const auto& pair : model) {
        const auto& row = pair.second; const auto rid = physical.at(pair.first).rid;
        if (alpha(row)) {
            need(contains(fti["alpha"], rid), "FTI actual file contains expected real RID");
            need(gin && gin->count("alpha") && contains(gin->at("alpha"), rid), "GIN actual file contains expected real RID");
            need(contains(engine.fullTextSearch(db, table, "txt", "alpha"), rid), "FTI native candidate contains every true match");
            need(contains(engine.ginSearch(db, table, "txt", "alpha"), rid), "GIN native candidate contains every true match");
        }
        if (row.text) {
            // Empty text is intentionally checked through the actual native
            // lossy receiver. Encoding's separate known-positive diagnostic
            // is not silently relabelled as repaired by this coverage check.
            need(contains(engine.giSTSearchOverlap(db, table, "txt", *row.text, *row.text), rid), "GiST lossy native receiver includes all true matches, including empty text");
            if (!row.text->empty()) need(gistValid && std::any_of(gist.begin(), gist.end(), [&](const auto& entry) {
                return entry.rid == uint64_t(rid) && entry.low == *row.text && entry.high == *row.text;
            }), "GiST nonempty encoded actual value and real RID");
        }
        if (!row.pointNull) {
            need(pointValid && std::any_of(points.begin(), points.end(), [&](const auto& entry) {
                return entry.rid == uint64_t(rid) && entry.x == row.id && entry.y == row.id;
            }), "SPGiST encoded real point/RID");
            need(contains(engine.spGiSTSearch(db, table, "p", "=", std::to_string(row.id) + "," + std::to_string(row.id)), rid), "SPGiST actual native receiver");
        }
    }
    const std::array<std::string, 3> columns{"n", "n3", "n128"};
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto image = bytes(root() / (std::string(table) + "_" + columns[i] + ".brin"));
        brin_index_format::TypedIndex index;
        const bool valid = checkedCrc(image) && brin_index_format::decodeTyped(image, index);
        need(valid, "actual BRIN V3 full frame and independent CRC");
        if (valid) need(index.pagesPerRange == widths[i], "BRIN actual retained/fallback pages_per_range");
        for (const auto& pair : model) {
            const auto& row = pair.second;
            const auto ranges = engine.tryBrinPageRanges(db, table, columns[i], row.n ? "=" : "IS NULL", row.n.value_or(""));
            need(bool(ranges), "actual BRIN native proof, not heap fallback");
            uint32_t page = 0; uint16_t slot = 0; StorageEngine::decodeRid(physical.at(pair.first).rid, page, slot);
            if (ranges) need(std::any_of(ranges->begin(), ranges->end(), [&](auto range) { return range.first <= page && page <= range.second; }), "BRIN candidate contains every true matching real heap page");
        }
    }
}
using Files = std::map<fs::path, std::optional<std::string>>;
Files images() {
    Files result;
    const auto capture = [&](const fs::path& path) {
        result[path] = fs::is_regular_file(path) ? std::optional<std::string>(bytes(path)) : std::nullopt;
    };
    for (const auto& definition : definitions) capture(file(definition));
    capture(root() / "am.idx");
    capture(root() / "am.idx.tde");
    for (const auto& definition : definitions) if (definition.method == "btree" || definition.method == "composite")
        capture(fs::path(file(definition).string() + ".tde"));
    return result;
}
void restoreImages(const Files& saved) {
    closeLiveTrees();
    for (const auto& image : saved) {
        if (fs::is_directory(image.first)) fs::remove(image.first);
        if (image.second) write(image.first, *image.second);
        else fs::remove(image.first); // Original absence is never a fabricated empty sidecar.
    }
}
DBStatus rebuild(const std::string& label, bool replacementRequired = true) {
    ++cases;
    std::map<fs::path, Generation> generations;
    for (const auto& definition : definitions) generations[file(definition)] = generation(file(definition));
    generations[root() / "am.idx"] = generation(root() / "am.idx");
    DBStatus result = DBStatus::IO_ERROR;
    try { result = g_engine.reindex(db, table); }
    catch (const std::exception& error) { need(false, label + " unexpected REINDEX exception: " + error.what()); }
    need(result == DBStatus::OK, "actual REINDEX status: " + label);
    if (replacementRequired) for (const auto& item : generations) {
        const auto after = generation(item.first);
        need(newGeneration(item.second, after), "actual replacement file generation: " + label + " " + item.first.filename().string());
    }
    need(noArtifacts(), "no residual owned swap/temp/backup artifacts: " + label);
    std::cout << "REINDEX_AM_CASE " << cases << " label=" << label << " status=" << int(result) << '\n';
    return result;
}
void transactionCase(Model& model, int ordinal, char kind, bool commit, int id,
                     const Cell* assignedText = nullptr) {
    const auto original = model;
    const auto originalPhysical = heap(g_engine, model);
    Row changed = kind == 'I' ? seedRow(id) : model.at(id);
    if (kind == 'I' || kind == 'U') changed.text = "alpha transaction" + std::to_string(ordinal);
    if (assignedText) changed.text = *assignedText;
    std::promise<void> changedPromise, mayFinishPromise;
    auto changedReady = changedPromise.get_future(); auto mayFinish = mayFinishPromise.get_future();
    std::exception_ptr writerFailure;
    bool changedOk = false, finishedOk = false;
    std::thread writer([&] {
        Session session; session.username = "testuser"; session.permission = 1; session.currentDB = db;
        auto* previous = currentSession(); setCurrentSession(&session);
        try {
            if (g_engine.beginTransaction(db) != DBStatus::OK) throw std::runtime_error("actual writer BEGIN failed");
            if (kind == 'I') insert(changed);
            else if (kind == 'U') {
                if (g_engine.updateRows(db, table, StorageEngine::SqlRow{{"txt", changed.text}}, {"=id " + std::to_string(id)}) != DBStatus::OK)
                    throw std::runtime_error("actual writer UPDATE failed");
            } else if (g_engine.removeRows(db, table, {"=id " + std::to_string(id)}) != DBStatus::OK)
                throw std::runtime_error("actual writer DELETE failed");
            changedOk = true;
        } catch (...) { writerFailure = std::current_exception(); }
        changedPromise.set_value();
        mayFinish.wait(); // Real acknowledged mutation, no sleeps or timeout-based success.
        try {
            if (changedOk) finishedOk = (commit ? g_engine.commitTransaction() : g_engine.rollbackTransaction()) == DBStatus::OK;
            else if (g_engine.inTransaction()) g_engine.rollbackTransaction();
        } catch (...) { writerFailure = std::current_exception(); }
        setCurrentSession(previous);
    });
    changedReady.wait();
    struct Join { std::promise<void>& release; std::thread& worker; ~Join() { if (worker.joinable()) { release.set_value(); worker.join(); } } } join{mayFinishPromise, writer};
    need(changedOk, "real peer mutation reached stable precommit point");
    if (changedOk) rebuild("active-" + std::string(1, kind) + (commit ? "-commit-" : "-rollback-") + std::to_string(ordinal));
    mayFinishPromise.set_value(); writer.join();
    need(!writerFailure && finishedOk, "actual writer commit/rollback terminal status");
    if (commit && finishedOk) {
        if (kind == 'D') model.erase(id); else model[id] = changed;
    }
    const auto physical = heap(g_engine, model);
    verifyIndexes(g_engine, model, physical, {1, 3, 128}, 11);
    if (!commit) {
        need(model.size() == original.size(), "rollback model retains every original row");
        need(physical == originalPhysical, "actual ROLLBACK restores all original rows/RIDs/NULL/raw bytes");
    }
}
} // namespace

int main() try {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const bool oldAnalyze = g_config.autoAnalyzeEnabled, oldVacuum = g_config.autoVacuumEnabled;
    g_config.autoAnalyzeEnabled = false; g_config.autoVacuumEnabled = false;
    g_engine.setBackgroundIntervals(60000, 60000);
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = db;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* previous; bool analyze, vacuum; ~Restore() { if (g_engine.inTransaction()) g_engine.rollbackTransaction(); setCurrentSession(previous); g_config.autoAnalyzeEnabled = analyze; g_config.autoVacuumEnabled = vacuum; } } restore{previous, oldAnalyze, oldVacuum};
    require(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "actual private database");
    DdlExecutor ddl;
    require(!ddl.executeSql("CREATE TABLE am(id integer PRIMARY KEY,txt text COLLATE \"C\",n bigint,n3 bigint,n128 bigint,p point,pad text)", session), "actual ordinary heap DDL");
    Model model;
    require(g_engine.beginTransaction(db) == DBStatus::OK, "actual seed transaction");
    for (int id = 1; id <= 16; ++id) { auto row = seedRow(id); insert(row); model.emplace(id, std::move(row)); }
    require(g_engine.commitTransaction() == DBStatus::OK, "actual durable seed");
    const std::vector<std::string> statements{
        "CREATE INDEX am_n ON am(n)", "CREATE INDEX am_pair ON am(n,id)",
        "CREATE INDEX am_expr ON am((upper(txt)))", "CREATE INDEX am_partial ON am(id) WHERE id>=4",
        "CREATE INDEX am_hash ON am USING hash(txt)", "CREATE INDEX am_bloom ON am USING bloom(txt)",
        "CREATE INDEX am_fti ON am USING fulltext(txt)", "CREATE INDEX am_gin ON am USING gin(txt)",
        "CREATE INDEX am_gist ON am USING gist(txt)", "CREATE INDEX am_spgist ON am USING spgist(p)",
        "CREATE INDEX am_brin1 ON am USING brin(n) WITH(pages_per_range=1)",
        "CREATE INDEX am_brin3 ON am USING brin(n3) WITH(pages_per_range=3)",
        "CREATE INDEX am_brin128 ON am USING brin(n128) WITH(pages_per_range=128)"};
    for (const auto& sql : statements) require(!ddl.executeSql(sql, session), "actual named/catalog AM DDL: " + sql);
    for (const auto& metadata : g_engine.getIndexMetadata(db, table)) if (metadata.isExpression) expressionKey = metadata.name;
    require(!expressionKey.empty(), "actual retained expression key identity");
    auto initial = heap(g_engine, model);
    // A genuine native BloomIndex with actual heap keys/RIDs establishes a
    // nondefault persisted k=11. No table or type identity is fabricated.
    if (auto* cached = g_engine.getBloomIndex(db, table, "txt")) require(cached->close(), "close old default Bloom generation");
    { BloomIndex custom(root() / "am_txt.bidx", 128, 11); require(custom.openForBuild(), "actual custom Bloom builder");
      for (const auto& entry : expectedText(initial, model)) for (auto rid : entry.second) require(custom.insert(entry.first, rid), "actual non-NULL custom Bloom key/RID");
      require(custom.close(), "durable genuine custom Bloom definition"); }
    require(g_engine.checkpoint(db), "actual heap/catalog/index checkpoint before evidence");
    const auto identity = identities();
    const auto saved = images();
    for (const auto& image : saved) {
        // Ordinary unencrypted BufferPool may create an empty .tde file.
        // Preserve its true presence/absence without claiming encryption.
        if (image.first.extension() != ".tde")
            require(image.second && !image.second->empty(), "actual healthy main index file snapshot");
    }
    { StorageEngine peer;
      (void)peer.getPKIndex(db, table); (void)peer.getSecondaryIndex(db, table, "n");
      (void)peer.getHashIndex(db, table, "txt"); (void)peer.getBloomIndex(db, table, "txt");
      (void)peer.spGiSTSearch(db, table, "p", "=", "1,1");
      rebuild("healthy-configurations-and-open-peer");
      need(heap(g_engine, model) == initial, "healthy rebuild leaves every raw row/RID/NULL byte unchanged");
      sameIdentity(identity); verifyIndexes(g_engine, model, initial, {1, 3, 128}, 11);
      verifyIndexes(peer, model, heap(peer, model), {1, 3, 128}, 11); }
    // Each fixed failure/rebuild shape is independent. Restoring the fixture's
    // original files after recording it is cleanup, never a retry of a case.
    for (const auto& action : {std::string("corrupt"), std::string("delete")}) {
        restoreImages(saved);
        closeLiveTrees();
        for (const auto& definition : definitions) {
            const auto path = file(definition);
            if (action == "corrupt") write(path, "independent corrupt index fixture\n");
            else { require(fs::remove(path), "delete owned defined index file"); if (definition.method == "btree" || definition.method == "composite") fs::remove(fs::path(path.string() + ".tde")); }
        }
        rebuild(action + "-all-defined-AM-files");
        need(heap(g_engine, model) == initial, "failed/repaired files never mutate heap/RIDs/NULLs");
        sameIdentity(identity);
        // Existing metadata contains no reloptions copy. Once its sole valid
        // BRIN/Bloom header was destroyed, original64/default7 fallback is
        // checked explicitly; original1/3/128/k11 is not invented from text.
        verifyIndexes(g_engine, model, initial, {64, 64, 64}, 7);
        restoreImages(saved);
    }
    // A nonregular defined target must fail before publishing a partial set.
    closeLiveTrees();
    const auto blocked = root() / "am_txt.gin";
    require(fs::remove(blocked) && fs::create_directory(blocked), "owned nonregular target fixture");
    const auto beforeFailure = images();
    ++cases;
    const auto failureStatus = g_engine.reindex(db, table);
    std::cout << "REINDEX_AM_CASE " << cases << " label=nonregular-AM-target status=" << int(failureStatus) << '\n';
    need(failureStatus != DBStatus::OK, "nonregular AM target cannot be silently skipped");
    need(noArtifacts(), "failed AM publication leaves no swap artifacts");
    sameIdentity(identity);
    for (const auto& image : beforeFailure) if (image.first != blocked)
        need(fs::is_regular_file(image.first) == bool(image.second) &&
             (!image.second || bytes(image.first) == *image.second), "failed AM build preserves every prior file presence and byte");
    need(heap(g_engine, model) == initial, "failed AM build preserves complete heap");
    restoreImages(saved);
    rebuild("restore-defined-healthy-state-before-transaction-controls");
    verifyIndexes(g_engine, model, heap(g_engine, model), {1, 3, 128}, 11);

    transactionCase(model, 1, 'I', true, 101);
    transactionCase(model, 2, 'I', false, 102);
    transactionCase(model, 3, 'U', true, 4);
    transactionCase(model, 4, 'U', false, 6);
    transactionCase(model, 5, 'D', true, 5);
    transactionCase(model, 6, 'D', false, 8);
    transactionCase(model, 7, 'U', true, 7);
    transactionCase(model, 8, 'D', false, 2);
    // Additional strict ordinary physical-key undo controls; the original
    // eight peer scenarios and all their assertions remain unchanged.
    const Cell sqlNull, emptyText = std::string();
    transactionCase(model, 9, 'D', false, 1);
    transactionCase(model, 10, 'U', false, 1, &sqlNull);
    transactionCase(model, 11, 'U', false, 3, &emptyText);
    const auto beforeSavepoint = heap(g_engine, model);
    require(g_engine.beginTransaction(db) == DBStatus::OK, "actual empty-key SAVEPOINT transaction");
    require(g_engine.savepoint("empty_restore") == DBStatus::OK, "actual public SAVEPOINT");
    require(g_engine.removeRows(db, table, {"=id 1"}) == DBStatus::OK, "actual empty-key DELETE after SAVEPOINT");
    require(g_engine.updateRows(db, table, StorageEngine::SqlRow{{"txt", std::nullopt}}, {"=id 9"}) == DBStatus::OK,
            "actual other empty key UPDATE to SQL NULL after SAVEPOINT");
    rebuild("savepoint-empty-delete-before-restore");
    require(g_engine.rollbackToSavepoint("empty_restore") == DBStatus::OK, "actual public rollback to SAVEPOINT");
    require(g_engine.releaseSavepoint("empty_restore") == DBStatus::OK, "actual public RELEASE SAVEPOINT");
    require(g_engine.commitTransaction() == DBStatus::OK, "actual savepoint-restored transaction COMMIT");
    const auto afterSavepoint = heap(g_engine, model);
    need(afterSavepoint == beforeSavepoint, "SAVEPOINT restores every actual empty-key row/RID/NULL/raw byte exactly");
    verifyIndexes(g_engine, model, afterSavepoint, {1, 3, 128}, 11);
    sameIdentity(identity);
    require(g_engine.checkpoint(db), "actual post-transaction checkpoint");
    { StorageEngine cold; const auto actual = heap(cold, model); verifyIndexes(cold, model, actual, {1, 3, 128}, 11); }

    // Loss of a native-only filename definition is a distinct, observable
    // ordinary limitation, not a fabricated successful reconstruction. SQL
    // named missing files above retain their genuine declaration authority.
    require(!ddl.executeSql("CREATE TABLE native_only(id integer PRIMARY KEY,txt text)", session), "actual native-only gap heap");
    require(g_engine.insertRow(db, "native_only", {{"id", std::string("1")}, {"txt", std::string("alpha")}}) == DBStatus::OK, "actual native-only gap row");
    require(g_engine.createGinIndex(db, "native_only", "txt") == DBStatus::OK, "actual native-only definition file");
    require(fs::remove(root() / "native_only_txt.gin"), "remove sole native-only definition");
    const auto nativeStatus = g_engine.reindex(db, "native_only");
    std::cout << "REINDEX_AM_KNOWN_GAP native_only_deleted_definition status=" << int(nativeStatus)
              << " physical_exists=" << g_engine.hasGinIndex(db, "native_only", "txt")
              << " declaration_authority=absent classification=NOT_A_PASS\n";
    std::cout << "REINDEX_AM_SCOPE gist_empty_encoded=SEPARATE_STRICT_DIAGNOSTIC native_lossy_empty=CHECKED"
                 " lost_reloptions=EXPLICIT_64_7_FALLBACK no_am_family_closure\n";
    std::cout << "REINDEX_ACCESS_METHODS checks=" << checks << " cases=" << cases << " failures=" << failures << '\n';
    return failures ? 1 : 0;
} catch (const std::exception& error) {
    std::cerr << "REINDEX_ACCESS_METHODS_FATAL checks=" << checks << " cases=" << cases
              << " failures=" << failures << ' ' << error.what() << '\n';
    return 1;
}
