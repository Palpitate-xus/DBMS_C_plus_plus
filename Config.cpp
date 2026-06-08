#include "Config.h"

namespace dbms {

static std::string trim(const std::string& s) {
    size_t a = 0;
    while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    size_t b = s.size();
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

bool Config::load(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs) return false;
    std::string line;
    while (std::getline(ifs, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') continue;
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = trim(line.substr(0, eq));
        std::string val = trim(line.substr(eq + 1));
        if (key == "max_connections") {
            try { maxConnections = std::stoi(val); } catch (...) {}
        } else if (key == "slow_query_threshold_ms") {
            try { slowQueryThresholdMs = std::stod(val); } catch (...) {}
        } else if (key == "checkpoint_interval") {
            try { checkpointInterval = std::stoi(val); } catch (...) {}
        } else if (key == "statement_timeout_ms") {
            try { statementTimeoutMs = std::stoi(val); } catch (...) {}
        } else if (key == "buffer_pool_frames") {
            try { bufferPoolFrames = static_cast<size_t>(std::stoull(val)); } catch (...) {}
        } else if (key == "enable_query_plan_cache") {
            enableQueryPlanCache = (val == "1" || val == "true" || val == "on");
        } else if (key == "query_plan_cache_size") {
            try { queryPlanCacheSize = static_cast<size_t>(std::stoull(val)); } catch (...) {}
        } else if (key == "password_policy_level") {
            try { passwordPolicyLevel = std::stoi(val); } catch (...) {}
        } else if (key == "password_hash_algorithm") {
            passwordHashAlgorithm = val;
        } else if (key == "audit_level") {
            try { auditLevel = std::stoi(val); } catch (...) {}
        } else if (key == "auto_vacuum") {
            autoVacuumEnabled = (val == "1" || val == "true" || val == "on");
        } else if (key == "auto_vacuum_threshold") {
            try { autoVacuumThreshold = std::stoi(val); } catch (...) {}
        } else if (key == "auto_analyze") {
            autoAnalyzeEnabled = (val == "1" || val == "true" || val == "on");
        } else if (key == "auto_analyze_threshold") {
            try { autoAnalyzeThreshold = std::stoi(val); } catch (...) {}
        } else if (key == "lock_timeout_ms") {
            try { lockTimeoutMs = std::stoi(val); } catch (...) {}
        } else if (key == "deadlock_timeout_ms") {
            try { deadlockTimeoutMs = std::stoi(val); } catch (...) {}
        } else if (key == "work_mem_kb") {
            try { workMemKb = static_cast<size_t>(std::stoull(val)); } catch (...) {}
        } else if (key == "enable_seq_scan") {
            enableSeqScan = (val == "1" || val == "true" || val == "on");
        } else if (key == "enable_hash_join") {
            enableHashJoin = (val == "1" || val == "true" || val == "on");
        } else if (key == "enable_merge_join") {
            enableMergeJoin = (val == "1" || val == "true" || val == "on");
        } else if (key == "auto_explain") {
            autoExplainEnabled = (val == "1" || val == "true" || val == "on");
        } else if (key == "auto_explain_threshold_ms") {
            try { autoExplainThresholdMs = std::stod(val); } catch (...) {}
        }
    }
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
              << "password_hash_algorithm " << passwordHashAlgorithm << "\n"
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
              << "auto_explain " << (autoExplainEnabled ? "on" : "off") << "\n"
              << "auto_explain_threshold_ms " << autoExplainThresholdMs << "\n";
}

bool Config::save(const std::string& filename) const {
    std::ofstream ofs(filename);
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
        << "password_hash_algorithm=" << passwordHashAlgorithm << "\n"
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
        << "auto_explain=" << (autoExplainEnabled ? "on" : "off") << "\n"
        << "auto_explain_threshold_ms=" << autoExplainThresholdMs << "\n";
    return true;
}

} // namespace dbms
