#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

// Per-connection session context.
// Replaces the previous global session variables (g_nowUser, g_nowPermission, etc.)
struct Session {
    std::string username;
    int permission = 0;
    std::string authenticatedUser; // login identity for RESET SESSION AUTHORIZATION
    int authenticatedPermission = 0;
    std::string currentDB = "info";
    // Startup/runtime parameters are connection-local.  Keep the original
    // startup values for diagnostics, plus canonical effective values used
    // by protocol ParameterStatus and SHOW.
    std::map<std::string, std::string> startupParameters;
    std::string startupOptions;
    std::string applicationName;
    std::string clientEncoding = "UTF8";
    std::string replicationMode = "false";
    std::string searchPath = "public";
    std::string timeZone = "UTC";
    std::string defaultApplicationName;
    std::string defaultClientEncoding = "UTF8";
    std::string defaultSearchPath = "public";
    std::string defaultTimeZone = "UTC";
    std::map<std::string, std::string> preparedStmts;
    // PostgreSQL PREPARE name(types) AS ...: declared parameter types
    std::map<std::string, std::vector<std::string>> preparedStmtTypes;
    // SELECT/UPDATE/DELETE ... FROM ONLY t: suppress inheritance expansion
    // for the next statement (reset after each execution).
    bool onlyNext = false;
    int isolationLevel = 2; // 0=READ UNCOMMITTED, 1=READ COMMITTED, 2=REPEATABLE READ, 3=SERIALIZABLE
    std::set<std::string> tempTables; // temporary table names in this session
    std::set<std::string> transientTempTables; // query-local CTE/derived table names
    std::map<std::string, std::string> tempTableOnCommit; // logical name -> preserve/delete/drop
    std::set<std::string> tempTablesCreatedInTransaction;
    int statementTimeoutMs = 0; // 0 = disabled
    int defaultStatementTimeoutMs = 0; // RESET statement_timeout target
    int lockTimeoutMs = 0; // 0 = no timeout
    int deadlockTimeoutMs = 1000; // milliseconds before deadlock detection
    int timezoneOffsetMinutes = 0; // Session timezone offset from UTC (e.g. +480 for Asia/Shanghai)
    std::string currentRole;      // SET ROLE target (empty = use original user)
    std::string originalRole;     // Session user's role (set at login)
    std::map<std::string, std::string> userVariables; // user-defined variables @var
    std::map<std::string, int64_t> sequenceLastValues; // session-local currval state
    bool constraintsDeferred = false; // SET CONSTRAINTS ALL/constraint_list DEFERRED
    std::set<std::string> listenedChannels; // channels this session is LISTENing to
    // Connection-local protocol portals are owned by NetworkServer. Mirror
    // their count here while executing a statement so project commands that
    // replace the database session context can fail closed.
    uint64_t openProtocolPortals = 0;
    uint64_t pid = 0; // process id for pg_cancel_backend / pg_terminate_backend
    uint64_t advisoryOwnerId = 0; // stable owner for session/xact advisory locks

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

    // Compatibility mode (gap DIV-01..DIV-14 framework): "postgresql18" is
    // the default and rejects project-only syntax with SQLSTATE 0A000;
    // "extended" enables only project extensions backed by a real runtime.
    // It never turns unsupported PostgreSQL objects into metadata-only
    // compatibility records.
    std::string compatibilityMode = "postgresql18";
};

inline std::string tempTablePrefix(const Session& session, const std::string& name) {
    return "__tmp_" + std::to_string(session.pid) + "_" + name;
}
