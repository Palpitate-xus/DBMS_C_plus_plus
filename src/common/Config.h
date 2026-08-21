#pragma once

#include <cstddef>
#include <string>

namespace dbms {

// Runtime configuration loaded from a .conf file.
// Supported parameters mirror common database settings.
struct Config {
    int maxConnections = 64;
    double slowQueryThresholdMs = 100.0;
    int checkpointInterval = 30;
    int statementTimeoutMs = 0;
    size_t bufferPoolFrames = 16;
    bool enableQueryPlanCache = true;
    size_t queryPlanCacheSize = 100;
    int passwordPolicyLevel = 0; // 0=none, 1=warn, 2=medium required, 3=strong required
    int auditLevel = 0; // 0=none, 1=DDL only, 2=DML+DDL, 3=all
    bool autoVacuumEnabled = true; // automatically VACUUM when dead tuples exceed threshold
    int autoVacuumThreshold = 50;  // dead tuple count threshold to trigger auto-vacuum
    bool autoAnalyzeEnabled = true; // automatically ANALYZE when modifications exceed threshold
    int autoAnalyzeThreshold = 50;  // modification count threshold to trigger auto-analyze
    int lockTimeoutMs = 0;         // 0 = no timeout
    int deadlockTimeoutMs = 1000;  // ms to wait before declaring deadlock (0 = immediate check)
    size_t workMemKb = 4096;       // work memory per query in KB
    bool enableSeqScan = true;     // enable sequential scan
    bool enableHashJoin = true;    // enable hash join
    bool enableMergeJoin = true;   // enable merge join
    int maxParallelWorkersPerGather = 0; // 0 disables parallel heap scans
    bool autoExplainEnabled = false; // auto_explain: log query plan for slow queries
    double autoExplainThresholdMs = 100.0; // threshold for auto_explain
    size_t sqlStatsMaxEntries = 5000; // bounded pg_stat_statements-style entries
    // Planner cost parameters (PostgreSQL names).
    double seqPageCost = 1.0;
    double randomPageCost = 4.0;
    double cpuTupleCost = 0.01;
    double cpuIndexTupleCost = 0.005;
    double cpuOperatorCost = 0.0025;
    bool enableNestloop = true;
    // Connection pooling (PgBouncer-style backend context pool).
    std::string poolMode = "session";  // session | transaction | statement
    int poolSize = 16;                 // pooled backend contexts per user
    int maxClientConnections = 0;      // 0 = unlimited (engine maxConnections still applies)
    // Transparent data encryption: empty = disabled; otherwise a keyring
    // file path whose 64-hex-char key encrypts heap pages at rest.
    std::string tdeKeyring;

    // Load from file; returns true only when the file is absent or every
    // recognized setting is valid. A present but malformed file is rejected
    // without modifying the current configuration object.
    bool load(const std::string& filename);
    // Save current values to file after validating all values.
    bool save(const std::string& filename) const;
    // Apply one runtime parameter to a candidate configuration. The object is
    // unchanged when the parameter is unknown or invalid.
    bool setParameter(const std::string& name, const std::string& value);
    // Dump current values for SHOW VARIABLES.
    void printAll() const;

    // Validate the complete runtime configuration independently of file I/O.
    bool validate() const;
};

} // namespace dbms
