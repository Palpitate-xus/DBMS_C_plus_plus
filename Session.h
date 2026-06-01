#pragma once

#include <map>
#include <set>
#include <string>

// Per-connection session context.
// Replaces the previous global session variables (g_nowUser, g_nowPermission, etc.)
struct Session {
    std::string username;
    int permission = 0;
    std::string currentDB = "info";
    std::map<std::string, std::string> preparedStmts;
    int isolationLevel = 2; // 0=READ UNCOMMITTED, 1=READ COMMITTED, 2=REPEATABLE READ, 3=SERIALIZABLE
    std::set<std::string> tempTables; // temporary table names in this session
    int statementTimeoutMs = 0; // 0 = disabled
    int timezoneOffsetMinutes = 0; // Session timezone offset from UTC (e.g. +480 for Asia/Shanghai)
    std::string currentRole;      // SET ROLE target (empty = use original user)
    std::string originalRole;     // Session user's role (set at login)
    std::map<std::string, std::string> userVariables; // user-defined variables @var

    // Cursors: named result sets for DECLARE CURSOR / FETCH / CLOSE
    struct Cursor {
        std::vector<std::string> rows;    // result rows (including header as first element)
        std::vector<std::string> colNames;
        int pos = -1;                     // -1 = before first row (FETCH NEXT gives row 0)
    };
    std::map<std::string, Cursor> cursors;
};
