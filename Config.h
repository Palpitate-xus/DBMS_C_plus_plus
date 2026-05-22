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

    // Load from file; returns true on success.
    bool load(const std::string& filename);
    // Dump current values for SHOW VARIABLES
    void printAll() const;
};

} // namespace dbms
