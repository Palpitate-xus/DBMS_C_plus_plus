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
              << "password_hash_algorithm " << passwordHashAlgorithm << "\n";
}

} // namespace dbms
