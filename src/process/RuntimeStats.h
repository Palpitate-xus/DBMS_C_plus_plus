#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace dbms {

// Runtime counters are updated at execution/storage boundaries rather than by
// the SQL display layer so SHOW and pg_stat_* views observe the same data from
// every entry point. The current process keeps a live copy; StorageEngine
// persists each database snapshot at checkpoint/shutdown boundaries.
struct RuntimeDatabaseStats {
    std::string dbname;
    uint64_t queries = 0;
    uint64_t failedQueries = 0;
    uint64_t xactCommit = 0;
    uint64_t xactRollback = 0;
    uint64_t tupReturned = 0;
    uint64_t tupFetched = 0;
    double totalQueryTimeMs = 0.0;
};

struct RuntimeTableStats {
    std::string dbname;
    std::string relname;
    uint64_t seqScan = 0;
    uint64_t seqTupRead = 0;
    uint64_t idxScan = 0;
    uint64_t idxTupFetch = 0;
    uint64_t nTupIns = 0;
    uint64_t nTupUpd = 0;
    uint64_t nTupDel = 0;
    int64_t nLiveTup = 0;
    // True only when nLiveTup was established by a complete visible scan or
    // adjusted from such a scan by subsequent mutations. A filtered/index
    // scan is only a lower bound and must not feed planner row estimates.
    bool liveTupEstimateValid = false;
};

// Record one complete SQL statement. elapsedMs is measured by the caller;
// resultRows is the number of rows returned by a structured result path.
void recordQueryExecution(const std::string& sql, double elapsedMs,
                          const std::string& dbname, bool succeeded,
                          uint64_t resultRows = 0);

// Record storage-level activity. completeScan is true only when the caller
// scanned the complete visible table, allowing nLiveTup to be refreshed.
void recordTableScan(const std::string& dbname, const std::string& tablename,
                     uint64_t rowsRead, bool indexScan = false,
                     bool completeScan = false);

enum class TableMutation { Insert, Update, Delete };

void recordTableMutation(const std::string& dbname, const std::string& tablename,
                         TableMutation mutation, uint64_t rowCount);

std::vector<RuntimeDatabaseStats> getRuntimeDatabaseStats(
    const std::string& dbFilter = "");
std::vector<RuntimeTableStats> getRuntimeTableStats(
    const std::string& dbFilter = "");

// Return a process-local row estimate only when its provenance is exact
// enough for planning. Callers must fall back to durable catalog/storage
// counts when this returns false.
bool getRuntimeLiveRowEstimate(const std::string& dbname,
                               const std::string& tablename,
                               uint64_t& rows);

// Drop relation-local process statistics when a relation is recreated or
// truncated. This prevents a same-name relation from inheriting stale row
// estimates from a previous physical identity.
void resetRuntimeTableStats(const std::string& dbname,
                            const std::string& tablename);

// Durable per-database statistics.  The file is a versioned current-format
// artifact, protected by a sidecar flock and published atomically.  Loading
// malformed data returns false; callers must not continue as if it were a
// valid snapshot.
bool loadRuntimeStats(const std::string& dbname,
                      const std::filesystem::path& path);
bool persistRuntimeStats(const std::string& dbname,
                         const std::filesystem::path& path);
void resetRuntimeDatabaseStats(const std::string& dbname);

void resetRuntimeStats();

} // namespace dbms
