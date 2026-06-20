// Test stubs for symbols referenced by NetworkServer.cpp and DdlExecutor.cpp but normally defined in main.cpp

#include <string>
#include <vector>
#include "Session.h"
#include "commands/TableManage.h"
#include "common/Config.h"

struct Session;

int g_checkpointInterval = 0;
double g_slowQueryThresholdMs = 0.0;
dbms::StorageEngine g_engine __attribute__((weak));
dbms::Config g_config __attribute__((weak));

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

// Stubs for DdlExecutor.cpp helpers that live in main.cpp
bool checkAdmin(const Session& s) { (void)s; return true; }
bool checkDB(const Session& s) { (void)s; return s.currentDB.empty() ? false : true; }
std::string resolveTableName(Session& s, const std::string& name) { (void)s; return name; }
