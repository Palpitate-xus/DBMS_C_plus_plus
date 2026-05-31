#pragma once

#include <string>
#include <map>
#include <fstream>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cctype>

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
    std::string passwordHashAlgorithm = "sha256"; // "sha256" or "md5"
    int auditLevel = 0; // 0=none, 1=DDL only, 2=DML+DDL, 3=all
    bool autoVacuumEnabled = true; // automatically VACUUM when dead tuples exceed threshold
    int autoVacuumThreshold = 50;  // dead tuple count threshold to trigger auto-vacuum
    int lockTimeoutMs = 0;         // 0 = no timeout
    int deadlockTimeoutMs = 1000;  // ms to wait before declaring deadlock (0 = immediate check)

    // Load from file; returns true on success.
    bool load(const std::string& filename);
    // Save current values to file
    bool save(const std::string& filename) const;
    // Dump current values for SHOW VARIABLES
    void printAll() const;
};

} // namespace dbms
