#pragma once

#include <atomic>
#include <cctype>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace dbms {

// Parse the list-valued search_path GUC without flattening quoted identifiers
// or accepting empty/malformed elements.  Entries are stored without quotes;
// unquoted identifiers follow PostgreSQL's lower-case folding rule.  "$user",
// pg_temp and pg_catalog are resolved by relation lookup rather than here.
inline bool parseSessionSearchPath(const std::string& value,
                                   std::vector<std::string>& entries,
                                   std::string& canonical) {
    entries.clear();
    canonical.clear();
    if (value.empty()) return true;
    std::string current;
    bool quoted = false;
    bool currentWasQuoted = false;
    const auto finish = [&]() -> bool {
        size_t first = 0;
        size_t last = current.size();
        if (!currentWasQuoted) {
            while (first < last && std::isspace(
                       static_cast<unsigned char>(current[first]))) ++first;
            while (last > first && std::isspace(
                       static_cast<unsigned char>(current[last - 1]))) --last;
        }
        std::string entry = current.substr(first, last - first);
        if (entry.empty() || entry.find('\0') != std::string::npos)
            return false;
        if (!currentWasQuoted) {
            for (char& ch : entry) {
                ch = static_cast<char>(std::tolower(
                    static_cast<unsigned char>(ch)));
            }
        }
        entries.push_back(std::move(entry));
        current.clear();
        currentWasQuoted = false;
        return true;
    };

    for (size_t offset = 0; offset < value.size(); ++offset) {
        const char ch = value[offset];
        if (quoted) {
            if (ch == '"') {
                if (offset + 1 < value.size() && value[offset + 1] == '"') {
                    current.push_back('"');
                    ++offset;
                } else {
                    quoted = false;
                }
            } else {
                current.push_back(ch);
            }
            continue;
        }
        if (ch == '"') {
            if (current.find_first_not_of(" \t\r\n") != std::string::npos)
                return false;
            current.clear();
            quoted = true;
            currentWasQuoted = true;
        } else if (ch == ',') {
            if (!finish()) return false;
        } else {
            if (currentWasQuoted) {
                if (std::isspace(static_cast<unsigned char>(ch))) continue;
                return false;
            }
            current.push_back(ch);
        }
    }
    if (quoted || !finish()) return false;

    for (size_t index = 0; index < entries.size(); ++index) {
        if (index != 0) canonical += ", ";
        const std::string& entry = entries[index];
        bool needsQuotes = entry == "$user";
        for (const unsigned char ch : entry) {
            if (!(std::islower(ch) || std::isdigit(ch) || ch == '_')) {
                needsQuotes = true;
                break;
            }
        }
        if (!needsQuotes) {
            canonical += entry;
            continue;
        }
        canonical.push_back('"');
        for (const char ch : entry) {
            if (ch == '"') canonical.push_back('"');
            canonical.push_back(ch);
        }
        canonical.push_back('"');
    }
    return true;
}

inline std::string expandSessionSearchPathEntry(
    const std::string& entry, const std::string& username) {
    return entry == "$user" ? username : entry;
}

}  // namespace dbms

// Shared between the protocol worker and the short-lived CancelRequest
// connection. Keeping this state outside Session itself makes copied pooled
// Session contexts observe the same interrupt without making Session
// non-copyable.
struct SessionInterruptState {
    std::atomic<bool> queryActive{false};
    std::atomic<bool> cancelRequested{false};
    std::atomic<bool> terminateRequested{false};
};

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
    // Protocol OIDs for the same statement registry.  SQL PREPARE and wire
    // Parse share this namespace; zero means an as-yet unspecified type.
    std::map<std::string, std::vector<uint32_t>> preparedStmtParameterOids;
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
    // PostgreSQL identifies sequence session state by relation OID, not by
    // the spelling used in nextval().  Keep the string map above only as a
    // compatibility mirror for older internal callers and tests.
    std::map<uint32_t, int64_t> sequenceLastValuesByOid;
    uint32_t lastUsedSequenceOid = 0;
    std::string lastUsedSequenceDatabase;
    bool constraintsDeferred = false; // SET CONSTRAINTS ALL/constraint_list DEFERRED
    std::set<std::string> listenedChannels; // channels this session is LISTENing to
    // Connection-local protocol portals are owned by NetworkServer. Mirror
    // their count here while executing a statement so project commands that
    // replace the database session context can fail closed.
    uint64_t openProtocolPortals = 0;
    // Physical top-level abort can release the engine transaction before the
    // client ends its SQL block. NetworkServer mirrors that failed block here
    // while executing recovery commands such as ROLLBACK AND CHAIN.
    bool failedTransactionBlock = false;
    // CHAIN restores transaction characteristics outside the new block's
    // SET TRANSACTION scope. Top-level abort restores this inherited mode;
    // an ordinary BEGIN instead restores the engine's session default.
    bool transactionChainOrigin = false;
    bool transactionChainReadOnly = false;
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
    std::shared_ptr<SessionInterruptState> interruptState =
        std::make_shared<SessionInterruptState>();

    // Compatibility mode (gap DIV-01..DIV-14 framework): "postgresql18" is
    // the default and rejects project-only syntax with SQLSTATE 0A000;
    // "extended" enables only project extensions backed by a real runtime.
    // It never turns unsupported PostgreSQL objects into metadata-only
    // compatibility records.
    std::string compatibilityMode = "postgresql18";
    // Appended to preserve the offsets of the long-lived session ABI used by
    // independently linked executor tests.
    std::string lcMonetary = "C";
    std::string defaultLcMonetary = "C";
    // A session's pg_temp alias remains resolvable after its last temp table
    // is dropped; it is not equivalent to tempTables being nonempty.
    bool tempNamespaceCreated = false;
};

inline std::string tempTablePrefix(const Session& session, const std::string& name) {
    return "__tmp_" + std::to_string(session.pid) + "_" + name;
}
