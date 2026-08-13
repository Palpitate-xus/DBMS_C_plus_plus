#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace dbms {

// SQL statistics shared by the interactive and protocol entry points. The
// current process keeps a live copy; StorageEngine persists each database
// snapshot at checkpoint/shutdown boundaries.
struct SqlStatEntry {
    std::string sql;
    uint64_t calls = 0;
    double totalTimeMs = 0.0;
    double minTimeMs = 0.0;
    double maxTimeMs = 0.0;
    double meanTimeMs = 0.0;
    std::string dbname;
};

// Normalize whitespace, SQL keywords and literal constants so equivalent
// statements are aggregated without changing quoted identifiers.
std::string normalizeSqlForStats(const std::string& sql);

void recordSqlStat(const std::string& sql, double elapsedMs,
                   const std::string& dbname);

std::vector<SqlStatEntry> getSqlStats(const std::string& dbFilter = "");

void resetSqlStats();

// Bound the process-wide normalized statement table. Entries with the fewest
// calls are evicted first; ties are resolved deterministically by total time
// and key so concurrent backends converge on the same durable set.
bool setSqlStatsMaxEntries(size_t maxEntries);
size_t getSqlStatsMaxEntries();

// Durable per-database statistics. Malformed snapshots are rejected so the
// caller can fail closed during database startup.
bool loadSqlStats(const std::string& dbname,
                  const std::filesystem::path& path);
bool persistSqlStats(const std::string& dbname,
                     const std::filesystem::path& path);
void resetSqlDatabaseStats(const std::string& dbname);

} // namespace dbms
