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
std::string resolveTableName(Session& s, const std::string& name) {
    const size_t dot = name.find('.');
    if (dot != std::string::npos && dot > 0 && dot + 1 < name.size()) {
        const std::string schema = name.substr(0, dot);
        const std::string table = name.substr(dot + 1);
        if ((schema == "pg_temp" || schema.rfind("pg_temp_", 0) == 0) &&
            (s.tempTables.count(table) ||
             s.transientTempTables.count(table))) {
            return tempTablePrefix(s, table);
        }
        if (g_engine.schemaExists(s.currentDB, schema)) {
            const std::string physical = schema == "public"
                ? table : schema + "__" + table;
            const std::string legacyPublic = "public__" + table;
            if (schema == "public" &&
                !g_engine.tableExists(s.currentDB, physical) &&
                !g_engine.viewExists(s.currentDB, physical) &&
                (g_engine.tableExists(s.currentDB, legacyPublic) ||
                 g_engine.viewExists(s.currentDB, legacyPublic))) {
                return legacyPublic;
            }
            return physical;
        }
        return name;
    }
    if (s.tempTables.count(name) || s.transientTempTables.count(name)) {
        return tempTablePrefix(s, name);
    }
    std::vector<std::string> entries;
    std::string canonical;
    if (!dbms::parseSessionSearchPath(s.searchPath, entries, canonical)) {
        entries = {"public"};
    }
    std::string firstCandidate;
    for (const auto& rawSchema : entries) {
        const std::string schema = dbms::expandSessionSearchPathEntry(
            rawSchema, s.username);
        if (schema == "pg_catalog" || schema == "pg_temp" ||
            schema.rfind("pg_temp_", 0) == 0 ||
            !g_engine.schemaExists(s.currentDB, schema)) {
            continue;
        }
        std::string physical = schema == "public"
            ? name : schema + "__" + name;
        const std::string legacyPublic = "public__" + name;
        if (schema == "public" &&
            !g_engine.tableExists(s.currentDB, physical) &&
            !g_engine.viewExists(s.currentDB, physical) &&
            (g_engine.tableExists(s.currentDB, legacyPublic) ||
             g_engine.viewExists(s.currentDB, legacyPublic))) {
            physical = legacyPublic;
        }
        if (firstCandidate.empty()) firstCandidate = physical;
        if (g_engine.tableExists(s.currentDB, physical) ||
            g_engine.viewExists(s.currentDB, physical)) {
            return physical;
        }
    }
    return firstCandidate.empty() ? name : firstCandidate;
}
