#include "Config.h"

#include <cmath>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace dbms {

static std::string trim(const std::string& s) {
    size_t a = 0;
    while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

namespace {

bool parseBool(const std::string& value, bool& result) {
    if (value == "1" || value == "true" || value == "on") {
        result = true;
        return true;
    }
    if (value == "0" || value == "false" || value == "off") {
        result = false;
        return true;
    }
    return false;
}

bool parseInt(const std::string& value, int& result) {
    try {
        size_t consumed = 0;
        const int parsed = std::stoi(value, &consumed);
        if (consumed != value.size()) return false;
        result = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool parseSize(const std::string& value, size_t& result) {
    try {
        size_t consumed = 0;
        const unsigned long long parsed = std::stoull(value, &consumed);
        if (consumed != value.size()) return false;
        if (parsed > std::numeric_limits<size_t>::max()) return false;
        result = static_cast<size_t>(parsed);
        return true;
    } catch (...) {
        return false;
    }
}

bool parseDouble(const std::string& value, double& result) {
    try {
        size_t consumed = 0;
        const double parsed = std::stod(value, &consumed);
        if (consumed != value.size() || !std::isfinite(parsed)) return false;
        result = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

bool Config::validate() const {
    return maxConnections > 0 && maxConnections <= 1000000 &&
           std::isfinite(slowQueryThresholdMs) && slowQueryThresholdMs >= 0.0 &&
           checkpointInterval > 0 && checkpointInterval <= 1000000 &&
           statementTimeoutMs >= 0 &&
           bufferPoolFrames > 0 && bufferPoolFrames <= 100000000 &&
           queryPlanCacheSize <= 1000000 &&
           passwordPolicyLevel >= 0 && passwordPolicyLevel <= 3 &&
           auditLevel >= 0 && auditLevel <= 3 &&
           autoVacuumThreshold > 0 && autoVacuumThreshold <= 100000000 &&
           autoAnalyzeThreshold > 0 && autoAnalyzeThreshold <= 100000000 &&
           lockTimeoutMs >= 0 && deadlockTimeoutMs >= 0 &&
           workMemKb > 0 && workMemKb <= 100000000 &&
           maxParallelWorkersPerGather >= 0 &&
           maxParallelWorkersPerGather <= 128 &&
           std::isfinite(seqPageCost) && seqPageCost >= 0.0 &&
           std::isfinite(randomPageCost) && randomPageCost >= 0.0 &&
           std::isfinite(cpuTupleCost) && cpuTupleCost >= 0.0 &&
           std::isfinite(cpuIndexTupleCost) && cpuIndexTupleCost >= 0.0 &&
           std::isfinite(cpuOperatorCost) && cpuOperatorCost >= 0.0 &&
           std::isfinite(autoExplainThresholdMs) && autoExplainThresholdMs >= 0.0 &&
           sqlStatsMaxEntries > 0 && sqlStatsMaxEntries <= 1000000;
}

namespace {

bool assignParameter(Config& config, const std::string& key,
                     const std::string& value) {
    bool parsed = false;
    if (key == "max_connections") parsed = parseInt(value, config.maxConnections);
    else if (key == "slow_query_threshold_ms" || key == "slow_query_threshold") {
        parsed = parseDouble(value, config.slowQueryThresholdMs);
    } else if (key == "checkpoint_interval") parsed = parseInt(value, config.checkpointInterval);
    else if (key == "statement_timeout_ms" || key == "statement_timeout") {
        parsed = parseInt(value, config.statementTimeoutMs);
    } else if (key == "buffer_pool_frames") parsed = parseSize(value, config.bufferPoolFrames);
    else if (key == "enable_query_plan_cache") parsed = parseBool(value, config.enableQueryPlanCache);
    else if (key == "query_plan_cache_size") parsed = parseSize(value, config.queryPlanCacheSize);
    else if (key == "password_policy_level") parsed = parseInt(value, config.passwordPolicyLevel);
    else if (key == "audit_level") parsed = parseInt(value, config.auditLevel);
    else if (key == "auto_vacuum") parsed = parseBool(value, config.autoVacuumEnabled);
    else if (key == "auto_vacuum_threshold") parsed = parseInt(value, config.autoVacuumThreshold);
    else if (key == "auto_analyze") parsed = parseBool(value, config.autoAnalyzeEnabled);
    else if (key == "auto_analyze_threshold") parsed = parseInt(value, config.autoAnalyzeThreshold);
    else if (key == "lock_timeout_ms" || key == "lock_timeout") parsed = parseInt(value, config.lockTimeoutMs);
    else if (key == "deadlock_timeout_ms" || key == "deadlock_timeout") parsed = parseInt(value, config.deadlockTimeoutMs);
    else if (key == "work_mem_kb" || key == "work_mem") parsed = parseSize(value, config.workMemKb);
    else if (key == "enable_seq_scan") parsed = parseBool(value, config.enableSeqScan);
    else if (key == "enable_hash_join" || key == "enable_hashjoin") parsed = parseBool(value, config.enableHashJoin);
    else if (key == "enable_merge_join") parsed = parseBool(value, config.enableMergeJoin);
    else if (key == "max_parallel_workers_per_gather") {
        parsed = parseInt(value, config.maxParallelWorkersPerGather);
    } else if (key == "auto_explain") parsed = parseBool(value, config.autoExplainEnabled);
    else if (key == "auto_explain_threshold_ms" || key == "auto_explain_threshold") {
        parsed = parseDouble(value, config.autoExplainThresholdMs);
    } else if (key == "pg_stat_statements.max") {
        parsed = parseSize(value, config.sqlStatsMaxEntries);
    } else if (key == "seq_page_cost") {
        parsed = parseDouble(value, config.seqPageCost);
    } else if (key == "random_page_cost") {
        parsed = parseDouble(value, config.randomPageCost);
    } else if (key == "cpu_tuple_cost") {
        parsed = parseDouble(value, config.cpuTupleCost);
    } else if (key == "cpu_index_tuple_cost") {
        parsed = parseDouble(value, config.cpuIndexTupleCost);
    } else if (key == "cpu_operator_cost") {
        parsed = parseDouble(value, config.cpuOperatorCost);
    } else if (key == "enable_nestloop") {
        parsed = parseBool(value, config.enableNestloop);
    } else {
        return false;
    }
    return parsed && config.validate();
}

} // namespace

bool Config::setParameter(const std::string& name, const std::string& value) {
    Config candidate = *this;
    if (!assignParameter(candidate, name, value)) return false;
    *this = std::move(candidate);
    return true;
}

bool Config::load(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs) return false;
    Config candidate = *this;
    std::set<std::string> seenKeys;
    std::string line;
    while (std::getline(ifs, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') continue;
        size_t eq = line.find('=');
        if (eq == std::string::npos || line.find('=', eq + 1) != std::string::npos) return false;
        std::string key = trim(line.substr(0, eq));
        std::string val = trim(line.substr(eq + 1));
        if (key.empty() || val.empty()) return false;
        if (!seenKeys.insert(key).second) return false;
        if (!assignParameter(candidate, key, val)) return false;
    }
    if (!candidate.validate()) return false;
    *this = std::move(candidate);
    return true;
}

void Config::printAll() const {
    std::cout << "max_connections " << maxConnections << "\n"
              << "slow_query_threshold_ms " << slowQueryThresholdMs << "\n"
              << "checkpoint_interval " << checkpointInterval << "\n"
              << "statement_timeout_ms " << statementTimeoutMs << "\n"
              << "buffer_pool_frames " << bufferPoolFrames << "\n"
              << "enable_query_plan_cache " << (enableQueryPlanCache ? "on" : "off") << "\n"
              << "query_plan_cache_size " << queryPlanCacheSize << "\n"
              << "password_policy_level " << passwordPolicyLevel << "\n"
              << "audit_level " << auditLevel << "\n"
              << "auto_vacuum " << (autoVacuumEnabled ? "on" : "off") << "\n"
              << "auto_vacuum_threshold " << autoVacuumThreshold << "\n"
              << "auto_analyze " << (autoAnalyzeEnabled ? "on" : "off") << "\n"
              << "auto_analyze_threshold " << autoAnalyzeThreshold << "\n"
              << "lock_timeout_ms " << lockTimeoutMs << "\n"
              << "deadlock_timeout_ms " << deadlockTimeoutMs << "\n"
              << "work_mem_kb " << workMemKb << "\n"
              << "enable_seq_scan " << (enableSeqScan ? "on" : "off") << "\n"
              << "enable_hash_join " << (enableHashJoin ? "on" : "off") << "\n"
              << "enable_merge_join " << (enableMergeJoin ? "on" : "off") << "\n"
              << "max_parallel_workers_per_gather " << maxParallelWorkersPerGather << "\n"
              << "seq_page_cost " << seqPageCost << "\n"
              << "random_page_cost " << randomPageCost << "\n"
              << "cpu_tuple_cost " << cpuTupleCost << "\n"
              << "cpu_index_tuple_cost " << cpuIndexTupleCost << "\n"
              << "cpu_operator_cost " << cpuOperatorCost << "\n"
              << "enable_nestloop " << (enableNestloop ? "on" : "off") << "\n"
              << "auto_explain " << (autoExplainEnabled ? "on" : "off") << "\n"
              << "auto_explain_threshold_ms " << autoExplainThresholdMs << "\n"
              << "pg_stat_statements.max " << sqlStatsMaxEntries << "\n";
}

bool Config::save(const std::string& filename) const {
    if (!validate()) return false;
    static std::mutex saveMutex;
    std::lock_guard<std::mutex> lock(saveMutex);
    const std::string temporary = filename + ".tmp." + std::to_string(::getpid());
    std::ofstream ofs(temporary, std::ios::out | std::ios::trunc);
    if (!ofs) return false;
    ofs << "# DBMS runtime configuration\n"
        << "max_connections=" << maxConnections << "\n"
        << "slow_query_threshold_ms=" << slowQueryThresholdMs << "\n"
        << "checkpoint_interval=" << checkpointInterval << "\n"
        << "statement_timeout_ms=" << statementTimeoutMs << "\n"
        << "buffer_pool_frames=" << bufferPoolFrames << "\n"
        << "enable_query_plan_cache=" << (enableQueryPlanCache ? "on" : "off") << "\n"
        << "query_plan_cache_size=" << queryPlanCacheSize << "\n"
        << "password_policy_level=" << passwordPolicyLevel << "\n"
        << "audit_level=" << auditLevel << "\n"
        << "auto_vacuum=" << (autoVacuumEnabled ? "on" : "off") << "\n"
        << "auto_vacuum_threshold=" << autoVacuumThreshold << "\n"
        << "auto_analyze=" << (autoAnalyzeEnabled ? "on" : "off") << "\n"
        << "auto_analyze_threshold=" << autoAnalyzeThreshold << "\n"
        << "lock_timeout_ms=" << lockTimeoutMs << "\n"
        << "deadlock_timeout_ms=" << deadlockTimeoutMs << "\n"
        << "work_mem_kb=" << workMemKb << "\n"
        << "enable_seq_scan=" << (enableSeqScan ? "on" : "off") << "\n"
        << "enable_hash_join=" << (enableHashJoin ? "on" : "off") << "\n"
        << "enable_merge_join=" << (enableMergeJoin ? "on" : "off") << "\n"
        << "max_parallel_workers_per_gather=" << maxParallelWorkersPerGather << "\n"
        << "seq_page_cost=" << seqPageCost << "\n"
        << "random_page_cost=" << randomPageCost << "\n"
        << "cpu_tuple_cost=" << cpuTupleCost << "\n"
        << "cpu_index_tuple_cost=" << cpuIndexTupleCost << "\n"
        << "cpu_operator_cost=" << cpuOperatorCost << "\n"
        << "enable_nestloop=" << (enableNestloop ? "on" : "off") << "\n"
        << "auto_explain=" << (autoExplainEnabled ? "on" : "off") << "\n"
        << "auto_explain_threshold_ms=" << autoExplainThresholdMs << "\n"
        << "pg_stat_statements.max=" << sqlStatsMaxEntries << "\n";
    ofs.flush();
    if (!ofs) {
        ofs.close();
        std::error_code ec;
        std::filesystem::remove(temporary, ec);
        return false;
    }
    ofs.close();

    const int fileFd = ::open(temporary.c_str(), O_RDONLY | O_CLOEXEC);
    if (fileFd < 0 || ::fsync(fileFd) != 0) {
        if (fileFd >= 0) ::close(fileFd);
        std::error_code ec;
        std::filesystem::remove(temporary, ec);
        return false;
    }
    if (::close(fileFd) != 0 || ::rename(temporary.c_str(), filename.c_str()) != 0) {
        std::error_code ec;
        std::filesystem::remove(temporary, ec);
        return false;
    }

    const std::filesystem::path target(filename);
    const std::filesystem::path parent = target.has_parent_path()
                                             ? target.parent_path()
                                             : std::filesystem::path(".");
    const int dirFd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dirFd < 0 || ::fsync(dirFd) != 0) {
        if (dirFd >= 0) ::close(dirFd);
        return false;
    }
    return ::close(dirFd) == 0;
}

} // namespace dbms
