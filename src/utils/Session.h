#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>

// Per-connection session context.
// Replaces the previous global session variables (g_nowUser, g_nowPermission, etc.)
struct Session {
    std::string username;
    int permission = 0;
    std::string authenticatedUser; // login identity for RESET SESSION AUTHORIZATION
    int authenticatedPermission = 0;
    std::string currentDB = "info";
    std::map<std::string, std::string> preparedStmts;
    int isolationLevel = 2; // 0=READ UNCOMMITTED, 1=READ COMMITTED, 2=REPEATABLE READ, 3=SERIALIZABLE
    std::set<std::string> tempTables; // temporary table names in this session
    int statementTimeoutMs = 0; // 0 = disabled
    int defaultStatementTimeoutMs = 0; // RESET statement_timeout target
    int timezoneOffsetMinutes = 0; // Session timezone offset from UTC (e.g. +480 for Asia/Shanghai)
    std::string currentRole;      // SET ROLE target (empty = use original user)
    std::string originalRole;     // Session user's role (set at login)
    std::map<std::string, std::string> userVariables; // user-defined variables @var
    std::map<std::string, int64_t> sequenceLastValues; // session-local currval state
    bool constraintsDeferred = false; // SET CONSTRAINTS ALL/constraint_list DEFERRED
    std::set<std::string> listenedChannels; // channels this session is LISTENing to
    uint64_t pid = 0; // process id for pg_cancel_backend / pg_terminate_backend

    // Cursors: named result sets for DECLARE CURSOR / FETCH / CLOSE
    struct Cursor {
        std::vector<std::string> rows;    // result rows (including header as first element)
        std::vector<std::string> colNames;
        int pos = -1;                     // -1 = before first row (FETCH NEXT gives row 0)
    };
    std::map<std::string, Cursor> cursors;

    // Cancellation flags for pg_cancel_backend / pg_terminate_backend
    bool cancelRequested = false;   // set true to cancel current query
    bool terminateRequested = false; // set true to terminate session
};
