#include "SqlStats.h"

#include "StatsPersistence.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <string_view>

namespace dbms {
namespace {

// These maps intentionally live until process exit. StorageEngine is a
// process-global object and persists from its destructor; function-local
// allocated state avoids cross-translation-unit destruction ordering hazards.
std::mutex& sqlStatsMutex() {
    static auto* value = new std::mutex();
    return *value;
}
std::map<std::string, SqlStatEntry>& sqlStatsMap() {
    static auto* value = new std::map<std::string, SqlStatEntry>();
    return *value;
}
std::map<std::string, SqlStatEntry>& persistedSqlStatsMap() {
    static auto* value = new std::map<std::string, SqlStatEntry>();
    return *value;
}

constexpr std::string_view kSqlStatsMagic = "DBMS_SQL_STATS_V1";
constexpr uint32_t kSqlStatsVersion = 1;
constexpr size_t kDefaultSqlStatsMaxEntries = 5000;
constexpr uint64_t kMaxSqlStatsEntries = 1'000'000;
constexpr uint32_t kMaxSqlStatsString = 16 * 1024 * 1024;

size_t& sqlStatsMaxEntries() {
    static size_t value = kDefaultSqlStatsMaxEntries;
    return value;
}

bool isIdentifierChar(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '$';
}

bool startsLiteralNumber(const std::string& sql, size_t i) {
    if (i >= sql.size()) return false;
    const char c = sql[i];
    if (std::isdigit(static_cast<unsigned char>(c))) {
        return i == 0 || !isIdentifierChar(sql[i - 1]);
    }
    return c == '-' && i + 1 < sql.size() &&
           std::isdigit(static_cast<unsigned char>(sql[i + 1])) &&
           (i == 0 || !isIdentifierChar(sql[i - 1]));
}

void appendSpaceIfNeeded(std::string& out, bool& pendingSpace) {
    if (pendingSpace && !out.empty() && out.back() != ' ') out.push_back(' ');
    pendingSpace = false;
}

void skipQuotedLiteral(const std::string& sql, size_t& i) {
    ++i;
    while (i < sql.size()) {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
            i += 2;
            continue;
        }
        if (sql[i] != '\'') {
            ++i;
            continue;
        }
        if (i + 1 < sql.size() && sql[i + 1] == '\'') {
            i += 2;
            continue;
        }
        ++i;
        break;
    }
}

void skipQuotedIdentifier(const std::string& sql, size_t& i, std::string& out) {
    out.push_back('"');
    ++i;
    while (i < sql.size()) {
        out.push_back(sql[i]);
        if (sql[i] == '"') {
            if (i + 1 < sql.size() && sql[i + 1] == '"') {
                out.push_back('"');
                i += 2;
                continue;
            }
            ++i;
            break;
        }
        ++i;
    }
}

void skipNumber(const std::string& sql, size_t& i) {
    if (sql[i] == '-') ++i;
    while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) ++i;
    if (i < sql.size() && sql[i] == '.') {
        ++i;
        while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) ++i;
    }
    if (i < sql.size() && (sql[i] == 'e' || sql[i] == 'E')) {
        size_t exponent = i++;
        if (i < sql.size() && (sql[i] == '+' || sql[i] == '-')) ++i;
        size_t digits = i;
        while (i < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i]))) ++i;
        if (digits == i) i = exponent;
    }
}

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
    return writeValue(out, static_cast<uint32_t>(value.size())) &&
        static_cast<bool>(out.write(value.data(), static_cast<std::streamsize>(value.size())));
}

bool readString(std::istream& in, std::string& value) {
    uint32_t length = 0;
    if (!readValue(in, length) || length > kMaxSqlStatsString) return false;
    value.assign(length, '\0');
    in.read(value.data(), static_cast<std::streamsize>(length));
    return in.good();
}

std::string statKey(const std::string& dbname, const std::string& sql) {
    return dbname + "\x1f" + normalizeSqlForStats(sql);
}

bool validEntry(const SqlStatEntry& entry, const std::string& dbname) {
    return entry.dbname == dbname && !entry.sql.empty() && entry.calls > 0 &&
        std::isfinite(entry.totalTimeMs) && entry.totalTimeMs >= 0.0 &&
        std::isfinite(entry.minTimeMs) && entry.minTimeMs >= 0.0 &&
        std::isfinite(entry.maxTimeMs) && entry.maxTimeMs >= 0.0 &&
        std::isfinite(entry.meanTimeMs) && entry.meanTimeMs >= 0.0;
}

bool writeEntry(std::ostream& out, const SqlStatEntry& entry) {
    return writeString(out, entry.dbname) && writeString(out, entry.sql) &&
        writeValue(out, entry.calls) && writeValue(out, entry.totalTimeMs) &&
        writeValue(out, entry.minTimeMs) && writeValue(out, entry.maxTimeMs) &&
        writeValue(out, entry.meanTimeMs);
}

bool readEntry(std::istream& in, SqlStatEntry& entry, const std::string& dbname) {
    return readString(in, entry.dbname) && readString(in, entry.sql) &&
        readValue(in, entry.calls) && readValue(in, entry.totalTimeMs) &&
        readValue(in, entry.minTimeMs) && readValue(in, entry.maxTimeMs) &&
        readValue(in, entry.meanTimeMs) && validEntry(entry, dbname);
}

bool readSnapshot(const std::filesystem::path& path, const std::string& dbname,
                  std::map<std::string, SqlStatEntry>& entries) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    std::string magic;
    uint32_t version = 0;
    uint64_t count = 0;
    if (!readString(in, magic) || magic != kSqlStatsMagic ||
        !readValue(in, version) || version != kSqlStatsVersion ||
        !readString(in, magic) || magic != dbname || !readValue(in, count) ||
        count > kMaxSqlStatsEntries) {
        return false;
    }
    entries.clear();
    for (uint64_t i = 0; i < count; ++i) {
        SqlStatEntry entry;
        if (!readEntry(in, entry, dbname)) return false;
        entries.emplace(statKey(dbname, entry.sql), std::move(entry));
    }
    return in.peek() == std::char_traits<char>::eof() && in.eof();
}

bool writeSnapshot(const std::filesystem::path& path, const std::string& dbname,
                   const std::map<std::string, SqlStatEntry>& entries) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out || !writeString(out, std::string(kSqlStatsMagic)) ||
        !writeValue(out, kSqlStatsVersion) || !writeString(out, dbname) ||
        !writeValue(out, static_cast<uint64_t>(entries.size()))) {
        return false;
    }
    for (const auto& [key, entry] : entries) {
        if (!writeEntry(out, entry)) return false;
    }
    out.flush();
    return out.good();
}

uint64_t counterDelta(uint64_t current, uint64_t baseline) {
    return current >= baseline ? current - baseline : current;
}

SqlStatEntry mergeEntry(const SqlStatEntry& disk, const SqlStatEntry& current,
                        const SqlStatEntry& baseline) {
    const uint64_t deltaCalls = counterDelta(current.calls, baseline.calls);
    const double deltaTotal = current.totalTimeMs >= baseline.totalTimeMs
        ? current.totalTimeMs - baseline.totalTimeMs : current.totalTimeMs;
    SqlStatEntry merged = disk;
    merged.dbname = current.dbname;
    merged.sql = current.sql;
    if (deltaCalls == 0) return merged;
    merged.calls += deltaCalls;
    merged.totalTimeMs += deltaTotal;
    if (disk.calls == 0) {
        merged.minTimeMs = current.minTimeMs;
        merged.maxTimeMs = current.maxTimeMs;
    } else {
        merged.minTimeMs = std::min(disk.minTimeMs, current.minTimeMs);
        merged.maxTimeMs = std::max(disk.maxTimeMs, current.maxTimeMs);
    }
    merged.meanTimeMs = merged.totalTimeMs / static_cast<double>(merged.calls);
    return merged;
}

bool isLessUsed(const std::pair<const std::string, SqlStatEntry>& left,
                const std::pair<const std::string, SqlStatEntry>& right) {
    if (left.second.calls != right.second.calls) {
        return left.second.calls < right.second.calls;
    }
    if (left.second.totalTimeMs != right.second.totalTimeMs) {
        return left.second.totalTimeMs < right.second.totalTimeMs;
    }
    // Evict the lexicographically greater key to make ties reproducible.
    return left.first > right.first;
}

void trimSqlStatsLocked(std::map<std::string, SqlStatEntry>& entries) {
    while (entries.size() > sqlStatsMaxEntries()) {
        auto victim = entries.begin();
        for (auto it = std::next(entries.begin()); it != entries.end(); ++it) {
            if (isLessUsed(*it, *victim)) victim = it;
        }
        entries.erase(victim);
    }
}

} // namespace

std::string normalizeSqlForStats(const std::string& sql) {
    std::string out;
    bool pendingSpace = false;
    for (size_t i = 0; i < sql.size();) {
        const char c = sql[i];
        if (std::isspace(static_cast<unsigned char>(c))) {
            pendingSpace = true;
            ++i;
            continue;
        }
        if (c == '\'') {
            appendSpaceIfNeeded(out, pendingSpace);
            out.push_back('?');
            skipQuotedLiteral(sql, i);
            continue;
        }
        if (c == '"') {
            appendSpaceIfNeeded(out, pendingSpace);
            skipQuotedIdentifier(sql, i, out);
            continue;
        }
        if (startsLiteralNumber(sql, i)) {
            appendSpaceIfNeeded(out, pendingSpace);
            out.push_back('?');
            skipNumber(sql, i);
            continue;
        }
        appendSpaceIfNeeded(out, pendingSpace);
        out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        ++i;
    }
    while (!out.empty() && out.back() == ' ') out.pop_back();
    return out;
}

void recordSqlStat(const std::string& sql, double elapsedMs,
                   const std::string& dbname) {
    const std::string normalized = normalizeSqlForStats(sql);
    if (normalized.empty() || dbname.empty() || !std::isfinite(elapsedMs)) return;
    elapsedMs = std::max(0.0, elapsedMs);
    std::lock_guard<std::mutex> lock(sqlStatsMutex());
    const std::string key = dbname + "\x1f" + normalized;
    auto& entry = sqlStatsMap()[key];
    if (entry.calls == 0) {
        entry.sql = sql;
        entry.dbname = dbname;
        entry.minTimeMs = elapsedMs;
        entry.maxTimeMs = elapsedMs;
    } else {
        entry.minTimeMs = std::min(entry.minTimeMs, elapsedMs);
        entry.maxTimeMs = std::max(entry.maxTimeMs, elapsedMs);
    }
    ++entry.calls;
    entry.totalTimeMs += elapsedMs;
    entry.meanTimeMs = entry.totalTimeMs / static_cast<double>(entry.calls);
    trimSqlStatsLocked(sqlStatsMap());
}

std::vector<SqlStatEntry> getSqlStats(const std::string& dbFilter) {
    std::lock_guard<std::mutex> lock(sqlStatsMutex());
    std::vector<SqlStatEntry> result;
    for (const auto& item : sqlStatsMap()) {
        if (dbFilter.empty() || item.second.dbname == dbFilter) result.push_back(item.second);
    }
    std::sort(result.begin(), result.end(), [](const SqlStatEntry& a, const SqlStatEntry& b) {
        if (a.totalTimeMs != b.totalTimeMs) return a.totalTimeMs > b.totalTimeMs;
        return a.sql < b.sql;
    });
    return result;
}

bool setSqlStatsMaxEntries(size_t maxEntries) {
    if (maxEntries == 0 || maxEntries > kMaxSqlStatsEntries) return false;
    std::lock_guard<std::mutex> lock(sqlStatsMutex());
    sqlStatsMaxEntries() = maxEntries;
    trimSqlStatsLocked(sqlStatsMap());
    trimSqlStatsLocked(persistedSqlStatsMap());
    return true;
}

size_t getSqlStatsMaxEntries() {
    std::lock_guard<std::mutex> lock(sqlStatsMutex());
    return sqlStatsMaxEntries();
}

bool loadSqlStats(const std::string& dbname, const std::filesystem::path& path) {
    if (dbname.empty()) return false;
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) return !ec;
    StatsFileLock lock(path);
    if (!lock.ok()) return false;
    std::map<std::string, SqlStatEntry> entries;
    if (!readSnapshot(path, dbname, entries)) return false;
    std::lock_guard<std::mutex> guard(sqlStatsMutex());
    for (auto it = sqlStatsMap().begin(); it != sqlStatsMap().end();) {
        if (it->second.dbname == dbname) it = sqlStatsMap().erase(it);
        else ++it;
    }
    for (auto it = persistedSqlStatsMap().begin(); it != persistedSqlStatsMap().end();) {
        if (it->second.dbname == dbname) it = persistedSqlStatsMap().erase(it);
        else ++it;
    }
    for (const auto& [key, entry] : entries) {
        persistedSqlStatsMap()[key] = entry;
        sqlStatsMap()[key] = entry;
    }
    trimSqlStatsLocked(persistedSqlStatsMap());
    trimSqlStatsLocked(sqlStatsMap());
    return true;
}

bool persistSqlStats(const std::string& dbname, const std::filesystem::path& path) {
    if (dbname.empty()) return false;
    const auto parent = path.parent_path();
    std::error_code ec;
    if (!parent.empty()) std::filesystem::create_directories(parent, ec);
    if (ec) return false;
    StatsFileLock lock(path);
    if (!lock.ok()) return false;

    std::lock_guard<std::mutex> guard(sqlStatsMutex());
    std::map<std::string, SqlStatEntry> onDisk;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec || (exists && !readSnapshot(path, dbname, onDisk))) return false;
    std::map<std::string, SqlStatEntry> merged = onDisk;
    for (const auto& [key, current] : sqlStatsMap()) {
        if (current.dbname != dbname) continue;
        SqlStatEntry baseline;
        if (const auto it = persistedSqlStatsMap().find(key);
            it != persistedSqlStatsMap().end()) {
            baseline = it->second;
        }
        SqlStatEntry disk;
        if (const auto it = onDisk.find(key); it != onDisk.end()) disk = it->second;
        merged[key] = mergeEntry(disk, current, baseline);
    }
    trimSqlStatsLocked(merged);
    if (!publishStatsSnapshot(path, [&](const std::filesystem::path& temporary) {
            return writeSnapshot(temporary, dbname, merged);
        })) return false;

    for (auto it = persistedSqlStatsMap().begin(); it != persistedSqlStatsMap().end();) {
        if (it->second.dbname == dbname) it = persistedSqlStatsMap().erase(it);
        else ++it;
    }
    for (const auto& [key, entry] : merged) persistedSqlStatsMap()[key] = entry;
    for (auto it = sqlStatsMap().begin(); it != sqlStatsMap().end();) {
        if (it->second.dbname == dbname) it = sqlStatsMap().erase(it);
        else ++it;
    }
    for (const auto& [key, entry] : merged) sqlStatsMap()[key] = entry;
    return true;
}

void resetSqlDatabaseStats(const std::string& dbname) {
    std::lock_guard<std::mutex> lock(sqlStatsMutex());
    for (auto it = sqlStatsMap().begin(); it != sqlStatsMap().end();) {
        if (it->second.dbname == dbname) it = sqlStatsMap().erase(it);
        else ++it;
    }
    for (auto it = persistedSqlStatsMap().begin(); it != persistedSqlStatsMap().end();) {
        if (it->second.dbname == dbname) it = persistedSqlStatsMap().erase(it);
        else ++it;
    }
}

void resetSqlStats() {
    std::lock_guard<std::mutex> lock(sqlStatsMutex());
    sqlStatsMap().clear();
    persistedSqlStatsMap().clear();
}

} // namespace dbms
