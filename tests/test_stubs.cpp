// Test stubs for symbols referenced by NetworkServer.cpp but normally defined in main.cpp

#include <string>

struct Session;

int g_checkpointInterval = 0;
double g_slowQueryThresholdMs = 0.0;

void logSlowQuery(const std::string& sql, double ms,
                  const std::string& username, const std::string& dbname) {
    (void)sql; (void)ms; (void)username; (void)dbname;
}

void recordSqlStat(const std::string& sql, double ms, const std::string& dbname) {
    (void)sql; (void)ms; (void)dbname;
}

bool execute(const std::string& sql, Session& session) {
    (void)sql; (void)session;
    return false;
}
