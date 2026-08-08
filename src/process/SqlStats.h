#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dbms {

// Process-wide SQL statistics shared by the interactive and protocol entry
// points.  The storage is intentionally in-memory; persistence and bounded
// eviction are separate operational features and are not implied here.
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

} // namespace dbms
