#include "RuntimeStats.h"

#include <algorithm>
#include <cctype>
#include <map>
#include <mutex>

namespace dbms {
namespace {

std::mutex g_runtimeStatsMutex;
std::map<std::string, RuntimeDatabaseStats> g_databaseStats;
std::map<std::pair<std::string, std::string>, RuntimeTableStats> g_tableStats;

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

RuntimeDatabaseStats& databaseStats(const std::string& dbname) {
    auto& stats = g_databaseStats[dbname];
    stats.dbname = dbname;
    return stats;
}

RuntimeTableStats& tableStats(const std::string& dbname, const std::string& tablename) {
    auto& stats = g_tableStats[{dbname, tablename}];
    stats.dbname = dbname;
    stats.relname = tablename;
    return stats;
}

} // namespace

void recordQueryExecution(const std::string& sql, double elapsedMs,
                          const std::string& dbname, bool succeeded,
                          uint64_t resultRows) {
    if (dbname.empty()) return;
    std::lock_guard<std::mutex> lock(g_runtimeStatsMutex);
    auto& stats = databaseStats(dbname);
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
    std::lock_guard<std::mutex> lock(g_runtimeStatsMutex);
    auto& stats = tableStats(dbname, tablename);
    if (indexScan) {
        ++stats.idxScan;
        stats.idxTupFetch += rowsRead;
    } else {
        ++stats.seqScan;
        stats.seqTupRead += rowsRead;
    }
    if (completeScan) {
        stats.nLiveTup = static_cast<int64_t>(rowsRead);
    } else if (stats.nLiveTup < static_cast<int64_t>(rowsRead)) {
        // A filtered scan gives only a lower bound. Never reduce a value
        // learned from a complete scan based on that lower bound.
        stats.nLiveTup = static_cast<int64_t>(rowsRead);
    }
}

void recordTableMutation(const std::string& dbname, const std::string& tablename,
                         TableMutation mutation, uint64_t rowCount) {
    if (dbname.empty() || tablename.empty() || rowCount == 0) return;
    std::lock_guard<std::mutex> lock(g_runtimeStatsMutex);
    auto& stats = tableStats(dbname, tablename);
    switch (mutation) {
        case TableMutation::Insert:
            stats.nTupIns += rowCount;
            stats.nLiveTup += static_cast<int64_t>(rowCount);
            break;
        case TableMutation::Update:
            stats.nTupUpd += rowCount;
            break;
        case TableMutation::Delete:
            stats.nTupDel += rowCount;
            stats.nLiveTup = std::max<int64_t>(0, stats.nLiveTup - static_cast<int64_t>(rowCount));
            break;
    }
}

std::vector<RuntimeDatabaseStats> getRuntimeDatabaseStats(const std::string& dbFilter) {
    std::lock_guard<std::mutex> lock(g_runtimeStatsMutex);
    std::vector<RuntimeDatabaseStats> result;
    for (const auto& [dbname, stats] : g_databaseStats) {
        if (dbFilter.empty() || dbname == dbFilter) result.push_back(stats);
    }
    return result;
}

std::vector<RuntimeTableStats> getRuntimeTableStats(const std::string& dbFilter) {
    std::lock_guard<std::mutex> lock(g_runtimeStatsMutex);
    std::vector<RuntimeTableStats> result;
    for (const auto& [key, stats] : g_tableStats) {
        if (dbFilter.empty() || key.first == dbFilter) result.push_back(stats);
    }
    return result;
}

void resetRuntimeStats() {
    std::lock_guard<std::mutex> lock(g_runtimeStatsMutex);
    g_databaseStats.clear();
    g_tableStats.clear();
}

} // namespace dbms
