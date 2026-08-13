#include "RuntimeStats.h"
#include "StatsPersistence.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string_view>

namespace dbms {
namespace {

// StorageEngine is a process-global object in the CLI.  It persists runtime
// statistics from its destructor, so ordinary namespace-scope maps/mutexes
// would be vulnerable to cross-translation-unit static destruction order.
// Keep this state allocated for the process lifetime; this is intentional and
// makes shutdown persistence safe for both the CLI and embedded backends.
std::mutex& runtimeStatsMutex() {
    static auto* value = new std::mutex();
    return *value;
}
std::map<std::string, RuntimeDatabaseStats>& databaseStatsMap() {
    static auto* value = new std::map<std::string, RuntimeDatabaseStats>();
    return *value;
}
std::map<std::pair<std::string, std::string>, RuntimeTableStats>& tableStatsMap() {
    static auto* value = new std::map<std::pair<std::string, std::string>, RuntimeTableStats>();
    return *value;
}
std::map<std::string, RuntimeDatabaseStats>& persistedDatabaseStatsMap() {
    static auto* value = new std::map<std::string, RuntimeDatabaseStats>();
    return *value;
}
std::map<std::pair<std::string, std::string>, RuntimeTableStats>& persistedTableStatsMap() {
    static auto* value = new std::map<std::pair<std::string, std::string>, RuntimeTableStats>();
    return *value;
}
std::set<std::pair<std::string, std::string>>& removedRuntimeTables() {
    static auto* value = new std::set<std::pair<std::string, std::string>>();
    return *value;
}

constexpr std::string_view kStatsMagic = "DBMS_RUNTIME_STATS_V1";
constexpr uint32_t kStatsVersion = 1;

template <typename T>
bool writeValue(std::ostream& out, T value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
    return out.good();
}

template <typename T>
bool readValue(std::istream& in, T& value) {
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return in.good();
}

bool writeString(std::ostream& out, const std::string& value) {
    if (value.size() > std::numeric_limits<uint32_t>::max()) return false;
    if (!writeValue(out, static_cast<uint32_t>(value.size()))) return false;
    out.write(value.data(), static_cast<std::streamsize>(value.size()));
    return out.good();
}

bool readString(std::istream& in, std::string& value) {
    uint32_t length = 0;
    if (!readValue(in, length) || length > 16 * 1024 * 1024) return false;
    value.assign(length, '\0');
    in.read(value.data(), static_cast<std::streamsize>(length));
    return in.good();
}

bool writeDatabaseStats(std::ostream& out, const RuntimeDatabaseStats& s) {
    return writeString(out, s.dbname) && writeValue(out, s.queries) &&
        writeValue(out, s.failedQueries) && writeValue(out, s.xactCommit) &&
        writeValue(out, s.xactRollback) && writeValue(out, s.tupReturned) &&
        writeValue(out, s.tupFetched) && writeValue(out, s.totalQueryTimeMs);
}

bool readDatabaseStats(std::istream& in, RuntimeDatabaseStats& s) {
    return readString(in, s.dbname) && readValue(in, s.queries) &&
        readValue(in, s.failedQueries) && readValue(in, s.xactCommit) &&
        readValue(in, s.xactRollback) && readValue(in, s.tupReturned) &&
        readValue(in, s.tupFetched) && readValue(in, s.totalQueryTimeMs) &&
        !s.dbname.empty();
}

bool writeTableStats(std::ostream& out, const RuntimeTableStats& s) {
    const uint8_t valid = s.liveTupEstimateValid ? 1 : 0;
    return writeString(out, s.dbname) && writeString(out, s.relname) &&
        writeValue(out, s.seqScan) && writeValue(out, s.seqTupRead) &&
        writeValue(out, s.idxScan) && writeValue(out, s.idxTupFetch) &&
        writeValue(out, s.nTupIns) && writeValue(out, s.nTupUpd) &&
        writeValue(out, s.nTupDel) && writeValue(out, s.nLiveTup) &&
        writeValue(out, valid);
}

bool readTableStats(std::istream& in, RuntimeTableStats& s) {
    uint8_t valid = 0;
    return readString(in, s.dbname) && readString(in, s.relname) &&
        readValue(in, s.seqScan) && readValue(in, s.seqTupRead) &&
        readValue(in, s.idxScan) && readValue(in, s.idxTupFetch) &&
        readValue(in, s.nTupIns) && readValue(in, s.nTupUpd) &&
        readValue(in, s.nTupDel) && readValue(in, s.nLiveTup) &&
        readValue(in, valid) && valid <= 1 && !s.dbname.empty() &&
        !s.relname.empty() && (s.liveTupEstimateValid = valid != 0, true);
}

bool readCount(std::istream& in, uint64_t& count) {
    return readValue(in, count) && count <= 1'000'000;
}

struct DurableStatsSnapshot {
    RuntimeDatabaseStats database;
    std::vector<RuntimeTableStats> tables;
};

bool readSnapshot(const std::filesystem::path& path, const std::string& dbname,
                  DurableStatsSnapshot& snapshot) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    std::string magic;
    uint32_t version = 0;
    uint64_t tableCount = 0;
    if (!readString(in, magic) || magic != kStatsMagic ||
        !readValue(in, version) || version != kStatsVersion ||
        !readDatabaseStats(in, snapshot.database) ||
        snapshot.database.dbname != dbname || !readCount(in, tableCount)) {
        return false;
    }
    snapshot.tables.clear();
    snapshot.tables.reserve(static_cast<size_t>(tableCount));
    for (uint64_t i = 0; i < tableCount; ++i) {
        RuntimeTableStats table;
        if (!readTableStats(in, table) || table.dbname != dbname) return false;
        snapshot.tables.push_back(std::move(table));
    }
    const int trailing = in.peek();
    return trailing == std::char_traits<char>::eof() && in.eof();
}

bool writeSnapshot(const std::filesystem::path& path,
                   const DurableStatsSnapshot& snapshot) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out || !writeString(out, std::string(kStatsMagic)) ||
        !writeValue(out, kStatsVersion) ||
        !writeDatabaseStats(out, snapshot.database) ||
        !writeValue(out, static_cast<uint64_t>(snapshot.tables.size()))) {
        return false;
    }
    for (const auto& table : snapshot.tables) {
        if (!writeTableStats(out, table)) return false;
    }
    out.flush();
    return out.good();
}

template <typename T>
T counterDelta(T current, T baseline) {
    return current >= baseline ? current - baseline : current;
}

RuntimeDatabaseStats mergeDatabaseStats(const RuntimeDatabaseStats& disk,
                                        const RuntimeDatabaseStats& current,
                                        const RuntimeDatabaseStats& baseline) {
    RuntimeDatabaseStats merged = disk;
    merged.dbname = current.dbname;
    merged.queries += counterDelta(current.queries, baseline.queries);
    merged.failedQueries += counterDelta(current.failedQueries, baseline.failedQueries);
    merged.xactCommit += counterDelta(current.xactCommit, baseline.xactCommit);
    merged.xactRollback += counterDelta(current.xactRollback, baseline.xactRollback);
    merged.tupReturned += counterDelta(current.tupReturned, baseline.tupReturned);
    merged.tupFetched += counterDelta(current.tupFetched, baseline.tupFetched);
    const double queryDelta = current.totalQueryTimeMs - baseline.totalQueryTimeMs;
    merged.totalQueryTimeMs += queryDelta >= 0.0 ? queryDelta : current.totalQueryTimeMs;
    return merged;
}

RuntimeTableStats mergeTableStats(const RuntimeTableStats& disk,
                                  const RuntimeTableStats& current,
                                  const RuntimeTableStats& baseline) {
    RuntimeTableStats merged = disk;
    merged.dbname = current.dbname;
    merged.relname = current.relname;
    merged.seqScan += counterDelta(current.seqScan, baseline.seqScan);
    merged.seqTupRead += counterDelta(current.seqTupRead, baseline.seqTupRead);
    merged.idxScan += counterDelta(current.idxScan, baseline.idxScan);
    merged.idxTupFetch += counterDelta(current.idxTupFetch, baseline.idxTupFetch);
    merged.nTupIns += counterDelta(current.nTupIns, baseline.nTupIns);
    merged.nTupUpd += counterDelta(current.nTupUpd, baseline.nTupUpd);
    merged.nTupDel += counterDelta(current.nTupDel, baseline.nTupDel);
    const bool changed = current.seqScan != baseline.seqScan ||
        current.seqTupRead != baseline.seqTupRead ||
        current.idxScan != baseline.idxScan ||
        current.idxTupFetch != baseline.idxTupFetch ||
        current.nTupIns != baseline.nTupIns ||
        current.nTupUpd != baseline.nTupUpd ||
        current.nTupDel != baseline.nTupDel ||
        current.nLiveTup != baseline.nLiveTup ||
        current.liveTupEstimateValid != baseline.liveTupEstimateValid;
    if (changed) {
        merged.nLiveTup = current.nLiveTup;
        merged.liveTupEstimateValid = current.liveTupEstimateValid;
    }
    return merged;
}

std::string firstKeyword(const std::string& sql) {
    size_t begin = 0;
    while (begin < sql.size() && std::isspace(static_cast<unsigned char>(sql[begin]))) ++begin;
    size_t end = begin;
    while (end < sql.size() && !std::isspace(static_cast<unsigned char>(sql[end])) &&
           sql[end] != ';') {
        ++end;
    }
    std::string keyword = sql.substr(begin, end - begin);
    for (char& c : keyword) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return keyword;
}

RuntimeDatabaseStats& databaseStatEntry(const std::string& dbname) {
    auto& stats = databaseStatsMap()[dbname];
    stats.dbname = dbname;
    return stats;
}

RuntimeTableStats& tableStatEntry(const std::string& dbname, const std::string& tablename) {
    auto& stats = tableStatsMap()[{dbname, tablename}];
    stats.dbname = dbname;
    stats.relname = tablename;
    return stats;
}

} // namespace

void recordQueryExecution(const std::string& sql, double elapsedMs,
                          const std::string& dbname, bool succeeded,
                          uint64_t resultRows) {
    if (dbname.empty()) return;
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    auto& stats = databaseStatEntry(dbname);
    ++stats.queries;
    if (!succeeded) ++stats.failedQueries;
    stats.totalQueryTimeMs += std::max(0.0, elapsedMs);
    stats.tupReturned += resultRows;
    stats.tupFetched += resultRows;

    const std::string keyword = firstKeyword(sql);
    if (keyword == "commit" || keyword == "end") {
        ++stats.xactCommit;
    } else if (keyword == "rollback") {
        ++stats.xactRollback;
    }
}

void recordTableScan(const std::string& dbname, const std::string& tablename,
                     uint64_t rowsRead, bool indexScan, bool completeScan) {
    if (dbname.empty() || tablename.empty()) return;
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    auto& stats = tableStatEntry(dbname, tablename);
    if (indexScan) {
        ++stats.idxScan;
        stats.idxTupFetch += rowsRead;
    } else {
        ++stats.seqScan;
        stats.seqTupRead += rowsRead;
    }
    if (completeScan) {
        stats.nLiveTup = static_cast<int64_t>(rowsRead);
        stats.liveTupEstimateValid = true;
    } else if (stats.nLiveTup < static_cast<int64_t>(rowsRead)) {
        // A filtered scan gives only a lower bound. Never reduce a value
        // learned from a complete scan based on that lower bound.
        stats.nLiveTup = static_cast<int64_t>(rowsRead);
    }
}

void recordTableMutation(const std::string& dbname, const std::string& tablename,
                         TableMutation mutation, uint64_t rowCount) {
    if (dbname.empty() || tablename.empty() || rowCount == 0) return;
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    auto& stats = tableStatEntry(dbname, tablename);
    switch (mutation) {
        case TableMutation::Insert:
            stats.nTupIns += rowCount;
            // Keep the historical display value as a lower bound even before
            // an exact scan; the planner consults liveTupEstimateValid and
            // will not use this value until its provenance is exact.
            stats.nLiveTup += static_cast<int64_t>(rowCount);
            break;
        case TableMutation::Update:
            stats.nTupUpd += rowCount;
            break;
        case TableMutation::Delete:
            stats.nTupDel += rowCount;
            stats.nLiveTup = std::max<int64_t>(
                0, stats.nLiveTup - static_cast<int64_t>(rowCount));
            break;
    }
}

std::vector<RuntimeDatabaseStats> getRuntimeDatabaseStats(const std::string& dbFilter) {
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    std::vector<RuntimeDatabaseStats> result;
    for (const auto& [dbname, stats] : databaseStatsMap()) {
        if (dbFilter.empty() || dbname == dbFilter) result.push_back(stats);
    }
    return result;
}

std::vector<RuntimeTableStats> getRuntimeTableStats(const std::string& dbFilter) {
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    std::vector<RuntimeTableStats> result;
    for (const auto& [key, stats] : tableStatsMap()) {
        if (dbFilter.empty() || key.first == dbFilter) result.push_back(stats);
    }
    return result;
}

bool loadRuntimeStats(const std::string& dbname,
                      const std::filesystem::path& path) {
    if (dbname.empty()) return false;
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) return true;
    StatsFileLock lock(path);
    if (!lock.ok()) return false;
    DurableStatsSnapshot snapshot;
    if (!readSnapshot(path, dbname, snapshot)) return false;
    std::lock_guard<std::mutex> guard(runtimeStatsMutex());
    persistedDatabaseStatsMap()[dbname] = snapshot.database;
    databaseStatsMap()[dbname] = snapshot.database;
    for (const auto& table : snapshot.tables) {
        persistedTableStatsMap()[{dbname, table.relname}] = table;
        removedRuntimeTables().erase({dbname, table.relname});
        tableStatsMap()[{dbname, table.relname}] = table;
    }
    return true;
}

bool persistRuntimeStats(const std::string& dbname,
                         const std::filesystem::path& path) {
    if (dbname.empty()) return false;
    std::filesystem::path parent = path.parent_path();
    std::error_code ec;
    if (!parent.empty()) std::filesystem::create_directories(parent, ec);
    if (ec) return false;
    StatsFileLock lock(path);
    if (!lock.ok()) return false;

    std::lock_guard<std::mutex> guard(runtimeStatsMutex());
    RuntimeDatabaseStats current;
    auto dbIt = databaseStatsMap().find(dbname);
    if (dbIt != databaseStatsMap().end()) current = dbIt->second;
    else current.dbname = dbname;
    RuntimeDatabaseStats baseline;
    auto baselineIt = persistedDatabaseStatsMap().find(dbname);
    if (baselineIt != persistedDatabaseStatsMap().end()) baseline = baselineIt->second;
    RuntimeDatabaseStats disk = baseline;
    DurableStatsSnapshot onDisk;
    if (std::filesystem::exists(path, ec)) {
        if (ec || !readSnapshot(path, dbname, onDisk)) return false;
        disk = onDisk.database;
    }
    DurableStatsSnapshot snapshot;
    snapshot.database = mergeDatabaseStats(disk, current, baseline);
    std::set<std::pair<std::string, std::string>> currentKeys;
    for (const auto& [key, table] : tableStatsMap()) {
        if (key.first != dbname) continue;
        currentKeys.insert(key);
        RuntimeTableStats tableBaseline;
        auto baseIt = persistedTableStatsMap().find(key);
        if (baseIt != persistedTableStatsMap().end()) tableBaseline = baseIt->second;
        RuntimeTableStats tableDisk = tableBaseline;
        if (std::filesystem::exists(path, ec)) {
            auto diskIt = std::find_if(onDisk.tables.begin(), onDisk.tables.end(),
                [&](const RuntimeTableStats& item) { return item.relname == table.relname; });
            if (diskIt != onDisk.tables.end()) tableDisk = *diskIt;
        }
        snapshot.tables.push_back(mergeTableStats(tableDisk, table, tableBaseline));
    }
    if (std::filesystem::exists(path, ec)) {
        for (const auto& table : onDisk.tables) {
            const auto key = std::make_pair(dbname, table.relname);
            if (currentKeys.count(key) == 0 && !removedRuntimeTables().count(key)) {
                snapshot.tables.push_back(table);
            }
        }
    }
    std::sort(snapshot.tables.begin(), snapshot.tables.end(),
        [](const RuntimeTableStats& a, const RuntimeTableStats& b) {
            return a.relname < b.relname;
        });

    if (!publishStatsSnapshot(path, [&](const std::filesystem::path& temporary) {
            return writeSnapshot(temporary, snapshot);
        })) return false;
    persistedDatabaseStatsMap()[dbname] = snapshot.database;
    for (auto it = persistedTableStatsMap().begin(); it != persistedTableStatsMap().end();) {
        if (it->first.first == dbname) it = persistedTableStatsMap().erase(it);
        else ++it;
    }
    for (const auto& table : snapshot.tables) {
        persistedTableStatsMap()[{dbname, table.relname}] = table;
    }
    databaseStatsMap()[dbname] = snapshot.database;
    for (const auto& table : snapshot.tables) {
        tableStatsMap()[{dbname, table.relname}] = table;
    }
    for (auto it = removedRuntimeTables().begin(); it != removedRuntimeTables().end();) {
        if (it->first == dbname) it = removedRuntimeTables().erase(it);
        else ++it;
    }
    return true;
}

void resetRuntimeDatabaseStats(const std::string& dbname) {
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    databaseStatsMap().erase(dbname);
    persistedDatabaseStatsMap().erase(dbname);
    for (auto it = tableStatsMap().begin(); it != tableStatsMap().end();) {
        if (it->first.first == dbname) it = tableStatsMap().erase(it);
        else ++it;
    }
    for (auto it = persistedTableStatsMap().begin(); it != persistedTableStatsMap().end();) {
        if (it->first.first == dbname) it = persistedTableStatsMap().erase(it);
        else ++it;
    }
    for (auto it = removedRuntimeTables().begin(); it != removedRuntimeTables().end();) {
        if (it->first == dbname) it = removedRuntimeTables().erase(it);
        else ++it;
    }
}

bool getRuntimeLiveRowEstimate(const std::string& dbname,
                               const std::string& tablename,
                               uint64_t& rows) {
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    const auto it = tableStatsMap().find({dbname, tablename});
    if (it == tableStatsMap().end() || !it->second.liveTupEstimateValid ||
        it->second.nLiveTup < 0) {
        return false;
    }
    rows = static_cast<uint64_t>(it->second.nLiveTup);
    return true;
}

void resetRuntimeTableStats(const std::string& dbname,
                            const std::string& tablename) {
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    tableStatsMap().erase({dbname, tablename});
    persistedTableStatsMap().erase({dbname, tablename});
    removedRuntimeTables().insert({dbname, tablename});
}

void resetRuntimeStats() {
    std::lock_guard<std::mutex> lock(runtimeStatsMutex());
    databaseStatsMap().clear();
    tableStatsMap().clear();
    persistedDatabaseStatsMap().clear();
    persistedTableStatsMap().clear();
    removedRuntimeTables().clear();
}

} // namespace dbms
