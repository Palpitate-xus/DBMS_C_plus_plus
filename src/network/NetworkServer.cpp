#include "NetworkServer.h"
#include "common/version.h"
#include "common/DbError.h"
#include "common/SqlTrivia.h"
#include "commands/DmlExecutor.h"
#include "TableManage.h"
#include "permissions.h"
#include "PostgresProtocol.h"
#include "Session.h"
#include "network/ConnectionPool.h"
#include "TLSWrapper.h"
#include "utils/pg_hba.h"
#include "common/scram_sha256.h"
#include "common/FeatureGate.h"
#include "common/NotificationManager.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "common/DateType.h"
#include "common/BooleanCodec.h"
#include "common/IntegerTextCodec.h"
#include "common/NetworkValue.h"
#include "common/GeometryValue.h"
#include "PostgresNumeric.h"
#include "types/bytea.h"
#include "types/money.h"
#include "types/xml.h"
#include "process/SqlStats.h"
#include "process/RuntimeStats.h"
#include "process/OutputCapture.h"
#include "process/AdvisoryLockManager.h"
#include "utils/prepared_stmts.h"
#include "Config.h"
#include "parser/parser.h"
#include "expression/expr_helper.h"
#include "expression/ExprEvaluator.h"
#include <netinet/tcp.h>

#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <poll.h>
#include <random>
#include <set>
#include <signal.h>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sys/socket.h>
#include <sys/random.h>
#include <unistd.h>

// External globals from main.cpp
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

// Forward declare execute() and logSlowQuery() from main.cpp
extern bool execute(const std::string& rawSql, Session& s);
extern std::string resolveTableName(Session& s, const std::string& name);
extern double g_slowQueryThresholdMs;
extern void logSlowQuery(const std::string& sql, double ms,
                         const std::string& username,
                         const std::string& dbname);

namespace dbms {

static ServerStats g_stats;

// Process list: active connections
static std::mutex g_processMutex;
static std::map<uint64_t, ProcessInfo> g_processList;
static std::set<std::string> g_droppingDatabases;
struct BackendKeyEntry {
    uint32_t secretKey = 0;
    std::weak_ptr<SessionInterruptState> interruptState;
};
static std::map<uint64_t, BackendKeyEntry> g_backendKeys;
static uint64_t g_nextProcessId = 1;
static std::mutex g_roleConnectionMutex;
static std::unordered_map<std::string, int> g_roleConnections;
static std::atomic<bool> g_serverStopRequested{false};
static volatile sig_atomic_t g_signalStopRequested = 0;
static std::atomic<int> g_listenFd{-1};
static std::mutex g_clientFdMutex;
static std::set<int> g_clientFds;

void registerClientFd(int fd) {
    std::lock_guard<std::mutex> lock(g_clientFdMutex);
    g_clientFds.insert(fd);
}

void unregisterClientFd(int fd) {
    std::lock_guard<std::mutex> lock(g_clientFdMutex);
    g_clientFds.erase(fd);
}

void shutdownActiveClients() {
    std::lock_guard<std::mutex> lock(g_clientFdMutex);
    for (const int fd : g_clientFds) {
        ::shutdown(fd, SHUT_RDWR);
    }
}

void serverSignalHandler(int) {
    g_signalStopRequested = 1;
}

struct ServerSignalGuard {
    struct sigaction oldTerm{};
    struct sigaction oldInt{};
    bool installed = false;

    bool install() {
        struct sigaction action{};
        action.sa_handler = serverSignalHandler;
        sigemptyset(&action.sa_mask);
        action.sa_flags = 0;
        if (::sigaction(SIGTERM, &action, &oldTerm) != 0 ||
            ::sigaction(SIGINT, &action, &oldInt) != 0) {
            return false;
        }
        installed = true;
        return true;
    }

    ~ServerSignalGuard() {
        if (!installed) return;
        ::sigaction(SIGTERM, &oldTerm, nullptr);
        ::sigaction(SIGINT, &oldInt, nullptr);
    }
};

ServerStats& getServerStats() {
    return g_stats;
}

uint32_t randomBackendSecret() {
    uint32_t secret = 0;
    while (secret == 0) {
        const ssize_t received = ::getrandom(&secret, sizeof(secret), 0);
        if (received == static_cast<ssize_t>(sizeof(secret))) continue;
        std::random_device device;
        secret = (static_cast<uint32_t>(device()) << 16) ^
                 static_cast<uint32_t>(device());
    }
    return secret;
}

BackendRegistration registerProcess(
    const std::string& user, const std::string& host, const std::string& db,
    const std::shared_ptr<SessionInterruptState>& interruptState) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    if (g_droppingDatabases.count(db) != 0 || !g_engine.databaseExists(db)) {
        return {};
    }
    uint64_t pid = g_nextProcessId++;
    ProcessInfo info;
    info.id = pid;
    info.user = user;
    info.host = host;
    info.db = db;
    info.command = "Sleep";
    info.timeSec = 0.0;
    info.state = "idle";
    info.info = "";
    info.connectTime = std::chrono::steady_clock::now();
    g_processList[pid] = std::move(info);
    const uint32_t secretKey = randomBackendSecret();
    g_backendKeys[pid] = BackendKeyEntry{secretKey, interruptState};
    return BackendRegistration{pid, secretKey};
}

bool reserveDatabaseDrop(const std::string& db) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    if (g_droppingDatabases.count(db) != 0) return false;
    for (const auto& [pid, process] : g_processList) {
        (void)pid;
        if (process.db == db) return false;
    }
    g_droppingDatabases.insert(db);
    return true;
}

void releaseDatabaseDrop(const std::string& db) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    g_droppingDatabases.erase(db);
}

bool trySwitchProcessDb(uint64_t pid, const std::string& db) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    if (g_droppingDatabases.count(db) != 0 ||
        (db != "information_schema" && !g_engine.databaseExists(db))) {
        return false;
    }
    const auto it = g_processList.find(pid);
    if (it != g_processList.end()) it->second.db = db;
    return true;
}

bool isServerTransportAllowed(bool tlsEnabled, bool allowPlaintext) {
    return tlsEnabled || allowPlaintext;
}

bool tryReserveConnectionSlot() {
    int active = g_stats.activeConnections.load(std::memory_order_relaxed);
    const int maximum = g_stats.maxConnections.load(std::memory_order_relaxed);
    while (active < maximum) {
        if (g_stats.activeConnections.compare_exchange_weak(
                active, active + 1, std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void releaseConnectionSlot() {
    int active = g_stats.activeConnections.load(std::memory_order_relaxed);
    while (active > 0) {
        if (g_stats.activeConnections.compare_exchange_weak(
                active, active - 1, std::memory_order_acq_rel,
                std::memory_order_relaxed)) {
            return;
        }
    }
}

bool tryReserveRoleConnection(const dbms::PgAuthIdRow& account) {
    if (account.rolsuper || account.rolconnlimit < 0) return true;
    std::lock_guard<std::mutex> lock(g_roleConnectionMutex);
    int& active = g_roleConnections[account.rolname];
    if (active >= account.rolconnlimit) return false;
    ++active;
    return true;
}

void releaseRoleConnection(const std::string& roleName) {
    std::lock_guard<std::mutex> lock(g_roleConnectionMutex);
    auto it = g_roleConnections.find(roleName);
    if (it == g_roleConnections.end()) return;
    if (--it->second <= 0) g_roleConnections.erase(it);
}

void updateProcessInfo(uint64_t pid, const std::string& command,
                       const std::string& state, const std::string& info) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    auto it = g_processList.find(pid);
    if (it != g_processList.end()) {
        it->second.command = command;
        it->second.state = state;
        it->second.info = info;
        if (state == "active" || state == "executing")
            it->second.lastQuery = info;
    }
}

void updateProcessDb(uint64_t pid, const std::string& db) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    auto it = g_processList.find(pid);
    if (it != g_processList.end()) {
        it->second.db = db;
    }
}

void unregisterProcess(uint64_t pid) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    g_processList.erase(pid);
    g_backendKeys.erase(pid);
}

bool cancelBackend(uint64_t pid) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    auto it = g_processList.find(pid);
    if (it == g_processList.end()) return false;
    it->second.cancelRequested = true;
    const auto key = g_backendKeys.find(pid);
    if (key != g_backendKeys.end()) {
        if (const auto state = key->second.interruptState.lock();
            state && state->queryActive.load(std::memory_order_acquire)) {
            state->cancelRequested.store(true, std::memory_order_release);
        }
    }
    return true;
}

bool terminateBackend(uint64_t pid) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    auto it = g_processList.find(pid);
    if (it == g_processList.end()) return false;
    it->second.terminateRequested = true;
    const auto key = g_backendKeys.find(pid);
    if (key != g_backendKeys.end()) {
        if (const auto state = key->second.interruptState.lock()) {
            state->terminateRequested.store(true, std::memory_order_release);
        }
    }
    return true;
}

bool cancelBackend(uint32_t pid, uint32_t secretKey) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    const auto key = g_backendKeys.find(pid);
    if (key == g_backendKeys.end() || key->second.secretKey != secretKey) {
        return false;
    }
    const auto state = key->second.interruptState.lock();
    if (!state || !state->queryActive.load(std::memory_order_acquire)) {
        return false;
    }
    state->timeoutRequested.store(false, std::memory_order_release);
    state->cancelRequested.store(true, std::memory_order_release);
    const auto process = g_processList.find(pid);
    if (process != g_processList.end()) process->second.cancelRequested = true;
    return true;
}

std::vector<ProcessInfo> getProcessList() {
    std::lock_guard<std::mutex> lock(g_processMutex);
    std::vector<ProcessInfo> result;
    auto now = std::chrono::steady_clock::now();
    for (auto& kv : g_processList) {
        ProcessInfo info = kv.second;
        auto elapsed = std::chrono::duration<double>(now - info.connectTime).count();
        info.timeSec = elapsed;
        result.push_back(std::move(info));
    }
    return result;
}

namespace {

struct QueryNotice {
    std::string severity;
    std::string sqlState;
    std::string message;
};

struct QueryResult {
    bool error = false;
    std::string errorMessage;
    std::string sqlState = "XX000";
    bool resultSet = false;
    std::vector<std::string> columns;
    std::vector<std::string> columnTypes;
    std::vector<PgColumnDescription> columnDescriptions;
    std::vector<std::vector<std::string>> rows;
    std::vector<std::vector<bool>> nulls;
    std::vector<QueryNotice> notices;
    std::string commandTag;
};

struct ProtocolPortal {
    ProtocolPortal() = default;
    ProtocolPortal(std::string statementName, std::string query,
                   std::string sourceQuery,
                   std::vector<uint32_t> sourceParameterOids,
                   std::vector<uint16_t> formats)
        : statement(std::move(statementName)),
          sql(std::move(query)),
          preparedSql(std::move(sourceQuery)),
          parameterOids(std::move(sourceParameterOids)),
          resultFormats(std::move(formats)) {}

    std::string statement;
    std::string sql;
    std::string preparedSql;
    std::vector<uint32_t> parameterOids;
    std::vector<uint16_t> resultFormats;
    QueryResult result;
    size_t rowOffset = 0;
    bool executed = false;
    bool completed = false;
};

std::string lowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

bool splitStartupOptions(const std::string& raw,
                         std::vector<std::string>& arguments,
                         std::string& error) {
    std::string current;
    bool escaped = false;
    for (unsigned char ch : raw) {
        if (escaped) {
            current.push_back(static_cast<char>(ch));
            escaped = false;
        } else if (ch == '\\') {
            escaped = true;
        } else if (std::isspace(ch)) {
            if (!current.empty()) {
                arguments.push_back(std::move(current));
                current.clear();
            }
        } else {
            current.push_back(static_cast<char>(ch));
        }
    }
    if (escaped) {
        error = "invalid startup options: trailing backslash";
        return false;
    }
    if (!current.empty()) arguments.push_back(std::move(current));
    return true;
}

bool parseStartupOptionAssignments(
        const std::string& raw,
        std::vector<std::pair<std::string, std::string>>& assignments,
        std::string& error) {
    std::vector<std::string> arguments;
    if (!splitStartupOptions(raw, arguments, error)) return false;
    for (size_t i = 0; i < arguments.size(); ++i) {
        std::string assignment;
        if (arguments[i] == "-c") {
            if (++i == arguments.size()) {
                error = "invalid startup options: -c requires name=value";
                return false;
            }
            assignment = arguments[i];
        } else if (arguments[i].rfind("-c", 0) == 0 &&
                   arguments[i].size() > 2) {
            assignment = arguments[i].substr(2);
        } else if (arguments[i].rfind("--", 0) == 0 &&
                   arguments[i].size() > 2) {
            assignment = arguments[i].substr(2);
        } else {
            error = "invalid startup options: unsupported argument " +
                    arguments[i];
            return false;
        }
        const size_t equals = assignment.find('=');
        if (equals == std::string::npos || equals == 0) {
            error = "invalid startup options: expected name=value";
            return false;
        }
        assignments.emplace_back(assignment.substr(0, equals),
                                 assignment.substr(equals + 1));
    }
    return true;
}

bool applyStartupRuntimeParameter(Session& session,
                                  const std::string& rawName,
                                  const std::string& value,
                                  std::string& sqlState,
                                  std::string& error) {
    std::string name = lowerAscii(rawName);
    std::replace(name.begin(), name.end(), '-', '_');
    if (name == "application_name") {
        session.applicationName = value;
        return true;
    }
    if (name == "client_encoding") {
        std::string encoding = lowerAscii(value);
        encoding.erase(std::remove_if(encoding.begin(), encoding.end(),
                                      [](char ch) {
                                          return ch == '-' || ch == '_';
                                      }),
                       encoding.end());
        if (encoding != "utf8" && encoding != "unicode") {
            sqlState = "0A000";
            error = "client encoding \"" + value +
                    "\" is not supported; only UTF8 is available";
            return false;
        }
        session.clientEncoding = "UTF8";
        return true;
    }
    if (name == "search_path") {
        std::vector<std::string> entries;
        std::string canonical;
        if (!dbms::parseSessionSearchPath(value, entries, canonical)) {
            sqlState = "22023";
            error = "invalid value for parameter \"search_path\"";
            return false;
        }
        session.searchPath = std::move(canonical);
        return true;
    }
    if (name == "lc_monetary") {
        if (!Money::localeAvailable(value)) {
            sqlState = "22023";
            error = "invalid value for parameter \"lc_monetary\": \"" +
                    value + "\"";
            return false;
        }
        session.lcMonetary = value;
        return true;
    }
    if (name == "timezone") {
        const std::string timezone = lowerAscii(value);
        if (timezone != "utc" && timezone != "gmt" && timezone != "z" &&
            timezone != "+00" && timezone != "+00:00" &&
            timezone != "-00" && timezone != "-00:00") {
            sqlState = "0A000";
            error = "startup TimeZone is not supported except for UTC";
            return false;
        }
        session.timezoneOffsetMinutes = 0;
        return true;
    }
    if (name == "datestyle") {
        std::string style = lowerAscii(value);
        style.erase(std::remove_if(style.begin(), style.end(),
                                   [](unsigned char ch) {
                                       return std::isspace(ch);
                                   }),
                    style.end());
        if (style != "iso,mdy") {
            sqlState = "0A000";
            error = "startup DateStyle is not supported except for ISO, MDY";
            return false;
        }
        return true;
    }
    if (name == "intervalstyle") {
        if (lowerAscii(value) != "postgres") {
            sqlState = "0A000";
            error = "startup IntervalStyle is not supported except for postgres";
            return false;
        }
        return true;
    }
    if (name == "statement_timeout" || name == "statement_timeout_ms" ||
        name == "lock_timeout" || name == "lock_timeout_ms" ||
        name == "deadlock_timeout" || name == "deadlock_timeout_ms") {
        Config candidate = g_config;
        if (!candidate.setParameter(name, value)) {
            sqlState = "22023";
            error = "invalid value for parameter \"" + rawName + "\"";
            return false;
        }
        if (name == "statement_timeout" || name == "statement_timeout_ms") {
            session.statementTimeoutMs = candidate.statementTimeoutMs;
            session.defaultStatementTimeoutMs = candidate.statementTimeoutMs;
        } else if (name == "lock_timeout" || name == "lock_timeout_ms") {
            session.lockTimeoutMs = candidate.lockTimeoutMs;
        } else {
            session.deadlockTimeoutMs = candidate.deadlockTimeoutMs;
        }
        return true;
    }
    sqlState = "42704";
    error = "unrecognized configuration parameter \"" + rawName + "\"";
    return false;
}

bool applyStartupParameters(const PgStartupMessage& startup,
                            Session& session,
                            std::string& sqlState,
                            std::string& error) {
    session.startupParameters = startup.parameters;
    const auto application = startup.parameters.find("application_name");
    if (application != startup.parameters.end()) {
        session.applicationName = application->second;
    }
    const auto encoding = startup.parameters.find("client_encoding");
    if (encoding != startup.parameters.end() &&
        !applyStartupRuntimeParameter(session, encoding->first,
                                      encoding->second, sqlState, error)) {
        return false;
    }

    const auto replication = startup.parameters.find("replication");
    if (replication != startup.parameters.end()) {
        const std::string mode = lowerAscii(replication->second);
        if (mode == "false" || mode == "off" || mode == "no" || mode == "0") {
            session.replicationMode = "false";
        } else if (mode == "true" || mode == "on" || mode == "yes" ||
                   mode == "1" || mode == "database") {
            session.replicationMode = mode == "database" ? "database" : "true";
            sqlState = "0A000";
            error = "replication protocol connections are not implemented";
            return false;
        } else {
            sqlState = "22023";
            error = "invalid value for parameter \"replication\": \"" +
                    replication->second + "\"";
            return false;
        }
    }

    static const std::set<std::string> special = {
        "user", "database", "options", "replication", "application_name",
        "client_encoding",
    };
    for (const auto& parameter : startup.parameters) {
        if (special.count(parameter.first)) continue;
        if (!applyStartupRuntimeParameter(session, parameter.first,
                                          parameter.second, sqlState, error)) {
            return false;
        }
    }

    const auto options = startup.parameters.find("options");
    if (options != startup.parameters.end()) {
        session.startupOptions = options->second;
        std::vector<std::pair<std::string, std::string>> assignments;
        if (!parseStartupOptionAssignments(options->second, assignments,
                                           error)) {
            sqlState = "42601";
            return false;
        }
        for (const auto& assignment : assignments) {
            if (!applyStartupRuntimeParameter(session, assignment.first,
                                              assignment.second, sqlState,
                                              error)) {
                return false;
            }
        }
    }
    session.defaultApplicationName = session.applicationName;
    session.defaultClientEncoding = session.clientEncoding;
    session.defaultSearchPath = session.searchPath;
    session.defaultTimeZone = session.timeZone;
    session.defaultLcMonetary = session.lcMonetary;
    return true;
}

std::map<std::string, std::string> mutableProtocolParameterStatuses(
        const Session& session,
        const std::string* previouslyReportedSuperuser = nullptr) {
    const std::string effectiveRole = session.currentRole.empty()
                                          ? session.username
                                          : session.currentRole;
    // A metadata-only deferred transaction has not acquired database catalog
    // ownership. Reporting an unchanged role is an observation, not a reason
    // to acquire that ownership. The caller may reuse its already reported
    // value only while the effective role is unchanged.
    std::string superuser;
    if (previouslyReportedSuperuser) {
        superuser = *previouslyReportedSuperuser;
    } else {
        const auto account = authCatalog().getAuthIdByName(effectiveRole);
        superuser = account && account->rolsuper ? "on" : "off";
    }
    return {
        {"application_name", session.applicationName},
        {"client_encoding", session.clientEncoding},
        {"is_superuser", superuser},
        {"search_path", session.searchPath},
        {"lc_monetary", session.lcMonetary},
        {"session_authorization", session.username},
        {"TimeZone", session.timeZone},
    };
}

bool isIntegerParameterType(uint32_t typeOid) {
    return typeOid == 20 || typeOid == 21 || typeOid == 23 || typeOid == 26;
}

bool isNumericParameterType(uint32_t typeOid) {
    return typeOid == 700 || typeOid == 701 || typeOid == 1700;
}

bool isStrictInteger(const std::string& value) {
    if (value.empty()) return false;
    size_t offset = (value.front() == '-' || value.front() == '+') ? 1 : 0;
    if (offset == value.size()) return false;
    for (; offset < value.size(); ++offset) {
        if (!std::isdigit(static_cast<unsigned char>(value[offset]))) return false;
    }
    return true;
}

bool isStrictNumeric(const std::string& value) {
    if (value.empty()) return false;
    char* end = nullptr;
    errno = 0;
    std::strtold(value.c_str(), &end);
    return errno != ERANGE && end == value.c_str() + value.size();
}

std::string quoteProtocolText(const std::string& value, std::string& error) {
    if (value.find('\0') != std::string::npos) {
        error = "parameter contains a NUL byte";
        return {};
    }
    std::string literal;
    literal.reserve(value.size() + 2);
    literal.push_back('\'');
    for (char c : value) {
        if (c == '\'') literal.push_back('\'');
        literal.push_back(c);
    }
    literal.push_back('\'');
    return literal;
}

bool decodeBinaryUnsigned(const std::vector<uint8_t>& raw, size_t width, uint64_t& value) {
    if (raw.size() != width || width == 0 || width > sizeof(uint64_t)) return false;
    value = 0;
    for (uint8_t byte : raw) value = (value << 8) | byte;
    return true;
}

std::string binaryProtocolParameterLiteral(uint32_t typeOid,
                                           const std::vector<uint8_t>& raw,
                                           const std::string& moneyLocale,
                                           std::string& error) {
    uint64_t bits = 0;
    if (geometryTypeNameForOid(typeOid)) {
        std::string value;
        if (!decodeGeometryBinary(typeOid, raw, value)) {
            error = "invalid binary input syntax for geometric type";
            return {};
        }
        return quoteProtocolText(value, error);
    }
    switch (typeOid) {
        case 17:
            return quoteProtocolText(
                ByteaValue::fromBytes(std::string(raw.begin(), raw.end()))
                    .toString(),
                error);
        case 16:
            if (raw.size() != 1 || (raw[0] != 0 && raw[0] != 1)) {
                error = "invalid binary input syntax for type boolean";
                return {};
            }
            return raw[0] ? "TRUE" : "FALSE";
        case 21:
            if (!decodeBinaryUnsigned(raw, 2, bits)) break;
            return std::to_string(static_cast<int16_t>(bits));
        case 23:
            if (!decodeBinaryUnsigned(raw, 4, bits)) break;
            return std::to_string(static_cast<int32_t>(bits));
        case 26:
            if (!decodeBinaryUnsigned(raw, 4, bits)) break;
            return std::to_string(bits);
        case 20:
            if (!decodeBinaryUnsigned(raw, 8, bits)) break;
            return std::to_string(static_cast<int64_t>(bits));
        case 700: {
            if (!decodeBinaryUnsigned(raw, 4, bits)) break;
            uint32_t rawBits = static_cast<uint32_t>(bits);
            float number = 0;
            std::memcpy(&number, &rawBits, sizeof(number));
            return std::to_string(number);
        }
        case 701: {
            if (!decodeBinaryUnsigned(raw, 8, bits)) break;
            double number = 0;
            std::memcpy(&number, &bits, sizeof(number));
            return std::to_string(number);
        }
        case 790: {
            if (!decodeBinaryUnsigned(raw, 8, bits)) break;
            const int64_t minorUnits =
                bits <= static_cast<uint64_t>(
                            std::numeric_limits<int64_t>::max())
                    ? static_cast<int64_t>(bits)
                    : -1 - static_cast<int64_t>(~bits);
            return quoteProtocolText(
                Money(minorUnits).format(moneyLocale), error);
        }
        case 1082: {
            if (!decodeBinaryUnsigned(raw, 4, bits)) break;
            const int32_t days = static_cast<int32_t>(bits);
            const Date date = DISCONV(Date(2000, 1, 1).convert() + days);
            if (date.year == 0) break;
            return quoteProtocolText(str(date), error);
        }
        case 1083: {
            if (!decodeBinaryUnsigned(raw, 8, bits)) break;
            const int64_t micros = static_cast<int64_t>(bits);
            if (micros < 0 || micros % 1000000 != 0 || micros >= 86400LL * 1000000) break;
            return quoteProtocolText(formatTimeSeconds(static_cast<int32_t>(micros / 1000000)), error);
        }
        case 1114: case 1184: {
            if (!decodeBinaryUnsigned(raw, 8, bits)) break;
            const int64_t micros = static_cast<int64_t>(bits);
            if (micros == INT64_MAX)
                return quoteProtocolText("infinity", error);
            if (micros == INT64_MIN)
                return quoteProtocolText("-infinity", error);
            if (micros % 1000000 != 0) break;
            const int64_t epoch = parseTimestampToSeconds("2000-01-01 00:00:00");
            const __int128 seconds = static_cast<__int128>(epoch) + micros / 1000000;
            if (seconds < std::numeric_limits<int64_t>::min() ||
                seconds > std::numeric_limits<int64_t>::max()) break;
            const std::string formatted = formatTimestampSeconds(static_cast<int64_t>(seconds));
            if (formatted.empty()) break;
            return quoteProtocolText(formatted, error);
        }
        case 1700: {
            std::string numeric;
            if (!decodePostgresNumeric(raw, numeric)) break;
            return quoteProtocolText(numeric, error);
        }
        case 1560: case 1562: {
            if (raw.size() < 4 || !decodeBinaryUnsigned(
                    std::vector<uint8_t>(raw.begin(), raw.begin() + 4),
                    4, bits)) {
                break;
            }
            const int32_t bitLength = static_cast<int32_t>(bits);
            if (bitLength < 0 || bitLength > 8388608 ||
                raw.size() != 4 + (static_cast<size_t>(bitLength) + 7) / 8) {
                break;
            }
            std::string value;
            value.reserve(static_cast<size_t>(bitLength));
            for (int32_t bit = 0; bit < bitLength; ++bit) {
                const uint8_t byte = raw[4 + static_cast<size_t>(bit) / 8];
                value.push_back(
                    (byte & (1U << (7U - (static_cast<unsigned>(bit) & 7U))))
                        ? '1' : '0');
            }
            if (bitLength % 8 != 0 && !raw.empty()) {
                const unsigned unused = 8U -
                    (static_cast<unsigned>(bitLength) & 7U);
                const uint8_t mask = static_cast<uint8_t>((1U << unused) - 1U);
                if ((raw.back() & mask) != 0) break;
            }
            return "B'" + value + "'";
        }
        case 650: case 869: {
            if (raw.size() != 8 && raw.size() != 20) break;
            NetworkAddressValue address;
            address.family = raw[0];
            address.bits = raw[1];
            const bool cidr = typeOid == 650;
            const size_t addressLength = address.byteLength();
            if (addressLength == 0 || raw[2] != (cidr ? 1 : 0) ||
                raw[3] != addressLength || raw.size() != 4 + addressLength ||
                address.bits > address.maxBits()) {
                break;
            }
            std::copy(raw.begin() + 4, raw.end(), address.address.begin());
            if (cidr && address.hasHostBits()) break;
            return quoteProtocolText(address.toString(cidr), error);
        }
        case 829: case 774: {
            const size_t length = typeOid == 829 ? 6 : 8;
            if (raw.size() != length) break;
            return quoteProtocolText(formatMacAddress(raw.data(), length), error);
        }
        case 2950: {
            if (raw.size() != 16) break;
            static constexpr char hex[] = "0123456789abcdef";
            std::string uuid;
            uuid.reserve(36);
            for (size_t i = 0; i < raw.size(); ++i) {
                if (i == 4 || i == 6 || i == 8 || i == 10) uuid.push_back('-');
                uuid.push_back(hex[raw[i] >> 4]);
                uuid.push_back(hex[raw[i] & 0x0f]);
            }
            return quoteProtocolText(uuid, error);
        }
        case 142: {
            const std::string value(raw.begin(), raw.end());
            if (!validateXml(value, XmlParseMode::Content).ok) {
                error = "invalid binary input syntax for type xml";
                return {};
            }
            return quoteProtocolText(value, error);
        }
        case 25: case 1042: case 1043:
            return quoteProtocolText(std::string(raw.begin(), raw.end()), error);
        default:
            error = "binary parameter type is not supported";
            return {};
    }
    error = "invalid binary input length for parameter type";
    return {};
}

std::string protocolParameterLiteral(uint32_t typeOid,
                                     const std::vector<uint8_t>& raw,
                                     bool binary,
                                     const std::string& moneyLocale,
                                     std::string& error) {
    if (binary) {
        return binaryProtocolParameterLiteral(
            typeOid, raw, moneyLocale, error);
    }
    std::string value(raw.begin(), raw.end());
    if (value.find('\0') != std::string::npos) {
        error = "parameter contains a NUL byte";
        return {};
    }
    if (typeOid == 16) {
        const auto parsed = parsePostgresBoolean(value);
        if (parsed) return *parsed ? "TRUE" : "FALSE";
        error = "invalid input syntax for type boolean";
        return {};
    }
    if (isIntegerParameterType(typeOid)) {
        if (!isStrictInteger(value)) {
            error = "invalid input syntax for integer parameter";
            return {};
        }
        return value;
    }
    if (isNumericParameterType(typeOid)) {
        if (!isStrictNumeric(value)) {
            error = "invalid input syntax for numeric parameter";
            return {};
        }
        return value;
    }
    if (typeOid == 1560 || typeOid == 1562) {
        if (std::any_of(value.begin(), value.end(),
                        [](char bit) { return bit != '0' && bit != '1'; })) {
            error = "invalid input syntax for type bit";
            return {};
        }
        return "B'" + value + "'";
    }
    if (typeOid == 650 || typeOid == 869) {
        NetworkAddressValue address;
        const bool cidr = typeOid == 650;
        if (!parseNetworkAddress(value, address, cidr)) {
            error = std::string("invalid input syntax for type ") +
                    (cidr ? "cidr" : "inet");
            return {};
        }
        return quoteProtocolText(address.toString(cidr), error);
    }
    if (typeOid == 829 || typeOid == 774) {
        const size_t length = typeOid == 829 ? 6 : 8;
        std::array<uint8_t, 8> address{};
        if (!parseMacAddress(value, length, address)) {
            error = std::string("invalid input syntax for type ") +
                    (typeOid == 829 ? "macaddr" : "macaddr8");
            return {};
        }
        return quoteProtocolText(formatMacAddress(address.data(), length),
                                 error);
    }
    if (typeOid == 142) {
        if (!validateXml(value, XmlParseMode::Content).ok) {
            error = "invalid input syntax for type xml";
            return {};
        }
    }
    if (const char* geometryType = geometryTypeNameForOid(typeOid)) {
        std::string canonical;
        if (!normalizeGeometryText(value, geometryType, canonical, true)) {
            error = std::string("invalid input syntax for type ") + geometryType;
            return {};
        }
        return quoteProtocolText(canonical, error);
    }
    return quoteProtocolText(value, error);
}

// These types can be represented as SQL casts in the bare VALUES execution
// path.  Keeping the cast on a bound parameter preserves its declared type
// when VALUES chooses a common type across rows.
const char* valuesParameterCastType(uint32_t typeOid) {
    switch (typeOid) {
        case 16: return "boolean";
        case 20: return "bigint";
        case 21: return "smallint";
        case 23: return "integer";
        case 25: return "text";
        case 700: return "real";
        case 701: return "double precision";
        case 1082: return "date";
        case 1114: return "timestamp";
        case 1184: return "timestamptz";
        case 1700: return "numeric";
        default: return nullptr;
    }
}

static const char* protocolParameterErrorSqlstate(
    const std::string& message) {
    if (message.rfind("invalid binary input", 0) == 0 ||
        message == "invalid binary input length for parameter type") {
        return "22P03";  // invalid_binary_representation
    }
    if (message.rfind("invalid input syntax", 0) == 0 ||
        message.find("contains a NUL byte") != std::string::npos) {
        return "22P02";  // invalid_text_representation
    }
    return "0A000";      // unsupported type/format capability
}

std::string trimText(const std::string& value) {
    size_t first = 0;
    while (first < value.size() && std::isspace(static_cast<unsigned char>(value[first]))) ++first;
    size_t last = value.size();
    while (last > first && std::isspace(static_cast<unsigned char>(value[last - 1]))) --last;
    return value.substr(first, last - first);
}

size_t skipSqlTrivia(const std::string& sql, size_t pos) {
    return dbms::skipLeadingSqlTrivia(sql, pos);
}

size_t sqlCommandOffset(const std::string& sql) {
    return skipSqlTrivia(sql, 0);
}

bool readSqlKeyword(const std::string& sql, size_t& position,
                    std::string& keyword) {
    const size_t start = skipSqlTrivia(sql, position);
    if (start == std::string::npos || start >= sql.size() ||
        (!std::isalnum(static_cast<unsigned char>(sql[start])) &&
         sql[start] != '_')) {
        return false;
    }
    size_t end = start;
    while (end < sql.size() &&
           (std::isalnum(static_cast<unsigned char>(sql[end])) ||
            sql[end] == '_')) ++end;
    keyword = sql.substr(start, end - start);
    for (char& c : keyword) {
        c = static_cast<char>(
            std::tolower(static_cast<unsigned char>(c)));
    }
    position = end;
    return true;
}

std::string firstSqlKeyword(const std::string& sql) {
    size_t position = 0;
    std::string keyword;
    // PostgreSQL permits a parenthesized query expression as a complete
    // statement (including operands with their own ORDER/LIMIT clauses).
    // Such a statement has no lexical keyword at byte zero; skip its opening
    // query-expression parentheses before deciding that a Simple Query is
    // empty. The SQL parser remains responsible for validating the grammar.
    while (true) {
        position = skipSqlTrivia(sql, position);
        if (position == std::string::npos || position >= sql.size() ||
            sql[position] != '(') break;
        ++position;
    }
    if (!readSqlKeyword(sql, position, keyword)) return {};
    return keyword;
}

std::vector<std::string> splitSimpleQueryStatements(const std::string& sql) {
    std::vector<std::string> statements;
    size_t statementStart = 0;
    bool singleQuoted = false;
    bool escapeSingleQuoted = false;
    bool doubleQuoted = false;
    bool lineComment = false;
    int blockCommentDepth = 0;
    std::string dollarDelimiter;
    const auto identifierContinuation = [](unsigned char c) {
        return std::isalnum(c) || c == '_' || c == '$' || c >= 0x80;
    };

    for (size_t i = 0; i < sql.size(); ++i) {
        const char c = sql[i];
        if (lineComment) {
            if (c == '\n' || c == '\r') lineComment = false;
            continue;
        }
        if (blockCommentDepth > 0) {
            if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
                ++blockCommentDepth;
                ++i;
            } else if (c == '*' && i + 1 < sql.size() && sql[i + 1] == '/') {
                --blockCommentDepth;
                ++i;
            }
            continue;
        }
        if (!dollarDelimiter.empty()) {
            if (sql.compare(i, dollarDelimiter.size(), dollarDelimiter) == 0) {
                i += dollarDelimiter.size() - 1;
                dollarDelimiter.clear();
            }
            continue;
        }
        if (singleQuoted) {
            if (escapeSingleQuoted && c == '\\' && i + 1 < sql.size()) {
                ++i;
            } else if (c == '\'' && i + 1 < sql.size() && sql[i + 1] == '\'') {
                ++i;
            } else if (c == '\'') {
                singleQuoted = false;
                escapeSingleQuoted = false;
            }
            continue;
        }
        if (doubleQuoted) {
            if (c == '"' && i + 1 < sql.size() && sql[i + 1] == '"') {
                ++i;
            } else if (c == '"') {
                doubleQuoted = false;
            }
            continue;
        }
        if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
            lineComment = true;
            ++i;
            continue;
        }
        if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
            blockCommentDepth = 1;
            ++i;
            continue;
        }
        if (c == '\'') {
            singleQuoted = true;
            escapeSingleQuoted = i > 0 &&
                (sql[i - 1] == 'e' || sql[i - 1] == 'E') &&
                (i < 2 || (!std::isalnum(static_cast<unsigned char>(sql[i - 2])) &&
                           sql[i - 2] != '_' && sql[i - 2] != '$'));
            continue;
        }
        if (c == '"') {
            doubleQuoted = true;
            continue;
        }
        if (c == '$' &&
            (i == 0 || !identifierContinuation(static_cast<unsigned char>(sql[i - 1])))) {
            size_t end = i + 1;
            if (end < sql.size() &&
                (std::isalpha(static_cast<unsigned char>(sql[end])) ||
                 sql[end] == '_')) {
                while (end < sql.size() &&
                       (std::isalnum(static_cast<unsigned char>(sql[end])) ||
                        sql[end] == '_')) {
                    ++end;
                }
            }
            if (end < sql.size() && sql[end] == '$') {
                dollarDelimiter = sql.substr(i, end - i + 1);
                i = end;
                continue;
            }
        }
        if (c != ';') continue;
        const std::string statement = trimText(
            sql.substr(statementStart, i - statementStart));
        if (!firstSqlKeyword(statement).empty()) statements.push_back(statement);
        statementStart = i + 1;
    }
    const std::string statement = trimText(sql.substr(statementStart));
    if (!firstSqlKeyword(statement).empty()) statements.push_back(statement);
    return statements;
}

bool isTransactionControlStatement(const std::string& sql) {
    const std::string keyword = firstSqlKeyword(sql);
    if (keyword == "prepare") {
        size_t position = 0;
        std::string first;
        std::string second;
        return readSqlKeyword(sql, position, first) &&
               readSqlKeyword(sql, position, second) && second == "transaction";
    }
    return keyword == "begin" || keyword == "start" || keyword == "commit" ||
           keyword == "end" || keyword == "rollback" || keyword == "abort" ||
           keyword == "savepoint" || keyword == "release";
}

bool isStandaloneDiscardAllStatement(const std::string& sql) {
    if (firstSqlKeyword(sql) != "discard") return false;
    const auto tokens = SQLParser::tokenize(sql);
    return (tokens.size() == 2 ||
            (tokens.size() == 3 && tokens.back() == ";")) &&
           firstSqlKeyword(tokens[0]) == "discard" &&
           firstSqlKeyword(tokens[1]) == "all";
}

std::vector<std::string> splitProtocolFields(const std::string& line) {
    std::vector<std::string> fields;
    // Double-quoted segments are single fields (multi-word column headers
    // such as a quoted "hello world" arrive as one cell from the producer).
    std::string field;
    bool inDq = false;
    for (size_t i = 0; i < line.size(); ++i) {
        const char c = line[i];
        if (c == 34) {
            if (inDq && i + 1 < line.size() && line[i + 1] == 34) {
                field += c;
                ++i;
            } else {
                inDq = !inDq;
            }
            continue;
        }
        if (!inDq && std::isspace(static_cast<unsigned char>(c))) {
            if (!field.empty()) { fields.push_back(field); field.clear(); }
            continue;
        }
        field += c;
    }
    if (!field.empty()) fields.push_back(field);
    return fields;
}

std::vector<std::string> outputLines(const std::string& output) {
    std::vector<std::string> lines;
    std::istringstream input(output);
    std::string line;
    while (std::getline(input, line)) {
        // Trim only the RIGHT side: a leading space is data (first cell
        // NULL renders as " val ..."), while trailing spaces are the
        // cell-separator artifact.
        const bool originallyEmpty = line.empty();
        while (!line.empty() &&
               isspace(static_cast<unsigned char>(line.back()))) line.pop_back();
        if (originallyEmpty) continue;
        // A line that was non-empty before trimming is real data: an
        // all-NULL row renders as a single space.  Keep it so the row
        // survives; only originally-empty lines (statement separators)
        // are dropped.
        if (line.empty()) line = " ";
        lines.push_back(std::move(line));
    }
    return lines;
}

std::string lowerProtocolText(std::string value) {
    for (char& c : value) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return value;
}

bool startsWithSqlPhrase(const std::string& sql, const std::string& phrase) {
    size_t position = 0;
    std::istringstream expected(phrase);
    std::string expectedKeyword;
    while (expected >> expectedKeyword) {
        std::string actualKeyword;
        if (!readSqlKeyword(sql, position, actualKeyword) ||
            actualKeyword != lowerProtocolText(expectedKeyword)) {
            return false;
        }
    }
    if (position >= sql.size()) return true;
    return std::isspace(static_cast<unsigned char>(sql[position])) ||
           sql[position] == ';' ||
           (position + 1 < sql.size() &&
            ((sql[position] == '-' && sql[position + 1] == '-') ||
             (sql[position] == '/' && sql[position + 1] == '*')));
}

struct ProtocolPhysicalSource {
    TableSchema table;
    Oid relationOid = INVALID_OID;
    std::vector<PgAttributeRow> attributes;
    std::vector<std::optional<std::string>> projections;
};

ProtocolPhysicalSource protocolPhysicalSourceFromQuery(const std::string& sql,
                                                      const Session& session) {
    ProtocolPhysicalSource result;
    SQLParser parser;
    const auto parsed = parser.parseForBinding(sql);
    if (!parsed.success || !parsed.stmt) return result;
    const Stmt* statement = parsed.stmt.get();
    const auto* with = dynamic_cast<const WithStmt*>(statement);
    if (with)
        statement = with->statement.get();
    const auto* select = dynamic_cast<const SelectStmt*>(statement);
    const std::vector<SelectItem>* projections = nullptr;
    std::string sourceName, sourceAlias;
    const ReturningOptions* images = nullptr;
    if (select) {
        if (select->setOp != SetOp::None || !select->fromClause ||
            select->fromClause->type != FromItem::Type::Table) return result;
        sourceName = select->fromClause->tableName;
        sourceAlias = select->fromClause->alias;
        projections = &select->selectList;
    } else if (const auto* insert = dynamic_cast<const InsertStmt*>(statement)) {
        sourceName = insert->tableName; projections = &insert->returning;
        images = &insert->returningOptions;
    } else if (const auto* update = dynamic_cast<const UpdateStmt*>(statement)) {
        sourceName = update->tableName; sourceAlias = update->alias;
        projections = &update->returning; images = &update->returningOptions;
    } else if (const auto* deletion = dynamic_cast<const DeleteStmt*>(statement)) {
        sourceName = deletion->tableName; sourceAlias = deletion->alias;
        projections = &deletion->returning; images = &deletion->returningOptions;
    } else if (const auto* merge = dynamic_cast<const MergeStmt*>(statement)) {
        sourceName = merge->targetTable; sourceAlias = merge->targetAlias;
        projections = &merge->returning; images = &merge->returningOptions;
    }
    if (!projections || projections->empty()) return result;
    // String/comment data and expression grammar (e.g. EXTRACT ... FROM)
    // never introduce a relation. Only the parser's actual range source can
    // request physical table-origin metadata for this legacy descriptor.
    CatalogManager::QualifiedName source;
    if (!CatalogManager::parseQualifiedName(sourceName, source, true)) return result;
    if (select && source.schema.empty()) {
        const auto shadowsPhysical = [&](const std::vector<SelectStmt::CTE>& definitions) {
            for (const auto& cte : definitions) {
                CatalogManager::QualifiedName name;
                if (CatalogManager::parseQualifiedName(cte.name, name, true) && name.name == source.name)
                    return true;
            }
            return false;
        };
        if (shadowsPhysical(select->ctes) || (with && shadowsPhysical(with->ctes))) return result;
    }
    // Resolve the actual SQL range from copied metadata, not a physical
    // temp filename, a result label, or a data scan. In particular do not
    // call the execution resolver: it notes temporary-relation access.
    const auto catalog = g_engine.catalogService().metadataSnapshot(session.currentDB);
    std::vector<std::string> schemas;
    if (!source.schema.empty()) {
        schemas.push_back(source.schema == "pg_temp"
            ? sessionTempSchemaName(session) : source.schema);
    } else {
        schemas.push_back(sessionTempSchemaName(session));
        schemas.push_back("pg_catalog");
        std::vector<std::string> entries; std::string canonical;
        if (!parseSessionSearchPath(session.searchPath, entries, canonical)) entries = {"public"};
        for (const auto& entry : entries)
            schemas.push_back(expandSessionSearchPathEntry(entry, session.username));
    }
    std::string actualSchema;
    for (auto schema : schemas) {
        if (schema == "pg_temp") schema = sessionTempSchemaName(session);
        Oid namespaceOid = INVALID_OID;
        for (const auto& name : catalog.namespaces)
            if (name.nspname == schema) { namespaceOid = name.oid; break; }
        if (namespaceOid == INVALID_OID) continue;
        for (const auto& relation : catalog.relations) {
            if (relation.relnamespace != namespaceOid || relation.relname != source.name) continue;
            // A nearer view/virtual relation shadows a farther physical
            // table; metadata lookup must follow the same namespace order.
            if (relation.relkind != 'r') return result;
            const auto physical = schema == sessionTempSchemaName(session)
                ? tempTablePrefix(session, source.name)
                : schema == "public" ? source.name : schema + "__" + source.name;
            if (!g_engine.tableExists(session.currentDB, physical)) return result;
            result.table = g_engine.getTableSchema(session.currentDB, physical);
            result.relationOid = relation.oid; actualSchema = schema;
            for (const auto& attribute : catalog.attributes)
                if (attribute.attrelid == relation.oid && attribute.attnum > 0 && !attribute.attisdropped) {
                    auto valueAttribute = attribute;
                    std::set<Oid> ancestry;
                    for (;;) {
                        const PgTypeRow* type = nullptr;
                        for (const auto& candidate : catalog.types)
                            if (candidate.oid == valueAttribute.atttypid) { type = &candidate; break; }
                        if (!type || type->typtype != 'd') break;
                        if (ancestry.size() >= 64 || !ancestry.insert(type->oid).second ||
                            type->typbasetype == INVALID_OID)
                            throw DbError("XX001", "invalid domain catalog ancestry");
                        if (valueAttribute.atttypmod < 0 && type->typtypmod >= 0)
                            valueAttribute.atttypmod = type->typtypmod;
                        valueAttribute.atttypid = type->typbasetype;
                    }
                    result.attributes.push_back(valueAttribute);
                }
            break;
        }
        if (result.relationOid != INVALID_OID) break;
    }
    // Native/legacy physical relations may lack catalog registration. They
    // can still provide type metadata, but never a fabricated relation OID.
    if (result.table.len == 0 && source.schema.empty() &&
        g_engine.tableExists(session.currentDB, source.name))
        result.table = g_engine.getTableSchema(session.currentDB, source.name);
    if (result.table.len == 0) return result;
    std::string alias;
    CatalogManager::QualifiedName parsedAlias;
    if (CatalogManager::parseQualifiedName(sourceAlias, parsedAlias, true)) alias = parsedAlias.name;
    const auto imageAlias = [](bool aliased, const std::string& token, const char* fallback) {
        if (!aliased) return std::string(fallback);
        CatalogManager::QualifiedName decoded;
        return CatalogManager::parseQualifiedName(token, decoded, true)
            ? decoded.name : std::string{};
    };
    const auto ownsReference = [&](const ColumnRefExpr& reference) {
        if (reference.table.empty()) return reference.schema.empty();
        if (images && reference.schema.empty() &&
            (reference.table == imageAlias(images->oldAliased, images->oldAlias, "old") ||
             reference.table == imageAlias(images->newAliased, images->newAlias, "new"))) return true;
        if (!alias.empty()) return reference.schema.empty() && reference.table == alias;
        return reference.table == source.name &&
            (reference.schema.empty() || reference.schema == source.schema ||
             reference.schema == actualSchema ||
             (reference.schema == "pg_temp" && actualSchema == sessionTempSchemaName(session)));
    };
    // Projection provenance is positional. Output aliases, including aliases
    // colliding with another physical column, do not change a ColumnRef's
    // source. Computed expressions must not inherit that column's typmod/OID.
    for (const auto& item : *projections) {
        const auto* literal = dynamic_cast<const LiteralExpr*>(item.expr.get());
        const auto* column = dynamic_cast<const ColumnRefExpr*>(item.expr.get());
        if ((literal && literal->value == "*") ||
            (column && column->column == "*" && ownsReference(*column))) {
            for (size_t i = 0; i < result.table.len; ++i)
                result.projections.push_back(result.table.cols[i].dataName);
        } else if (column && ownsReference(*column)) {
            result.projections.push_back(column->column);
        } else result.projections.push_back(std::nullopt);
    }
    return result;
}

std::string protocolPhysicalTypeName(const Column& column) {
    // Storage schemas retain the element name and a separate array flag.
    // A protocol descriptor needs the declared array identity, independently
    // of row count, NULL/value bytes, SQL alias, and number of dimensions.
    return column.isArray
        ? ExprHelper::canonicalResultTypeName(column.dataType + "[]")
        : column.dataType;
}

int16_t protocolTypeSize(uint32_t typeOid, const Column& column) {
    if (column.isArray) return -1;
    const int16_t geometryLength = geometryTypeLengthForOid(typeOid);
    if (geometryLength != 0) return geometryLength;
    switch (typeOid) {
        case 16: return 1;   // bool
        case 18: return 1;   // internal char
        case 19: return 64;  // name (NAMEDATALEN)
        case 20: return 8;   // int8
        case 21: return 2;   // int2
        case 23: return 4;   // int4
        case 24: case 26: case 28: case 29: case 2206: return 4;
        case 27: return 6;   // tid
        case 700: return 4;  // float4
        case 701: return 8;  // float8
        case 790: return 8;  // money
        case 1082: return 4; // date
        case 1083: return 8; // time
        case 1114: case 1184: return 8; // timestamp/timestamptz
        case 1186: return 16; // interval
        case 1042: case 1043: return -1; // bpchar/varchar
        case 1700: return -1; // numeric
        case 650: case 869: return -1; // cidr/inet
        case 774: return 8;   // macaddr8
        case 829: return 6;   // macaddr
        case 2278: return 4;  // void
        case 2950: return 16; // uuid
        default: return column.isVariableLength ? -1 : static_cast<int16_t>(column.dsize);
    }
}

int32_t protocolCharacterCastModifier(const Expr* expression) {
    if (!expression) return -1;
    std::string typeName;
    std::vector<std::string> modifiers;
    if (const auto* cast = dynamic_cast<const CastExpr*>(expression)) {
        typeName = lowerProtocolText(cast->typeName);
        modifiers = cast->typeMods;
    } else if (const auto* binary =
                   dynamic_cast<const BinaryOpExpr*>(expression);
               binary && binary->op == "::" && binary->right) {
        typeName = lowerProtocolText(binary->right->toString());
        const size_t open = typeName.find('(');
        if (open != std::string::npos && typeName.back() == ')') {
            modifiers.push_back(typeName.substr(open + 1,
                typeName.size() - open - 2));
            typeName.resize(open);
        }
    } else {
        return -1;
    }
    const bool fixedCharacter = typeName == "char" ||
                                typeName == "character";
    const bool varyingCharacter = typeName == "varchar" ||
                                  typeName == "character varying";
    if (!fixedCharacter && !varyingCharacter) return -1;
    if (modifiers.empty()) {
        return fixedCharacter ? 5 : -1; // Only SQL CHAR defaults to length 1.
    }
    if (modifiers.size() != 1 || modifiers.front().empty()) return -1;
    const std::string& lengthText = modifiers.front();
    if (!std::all_of(lengthText.begin(), lengthText.end(),
                     [](unsigned char c) { return std::isdigit(c); })) return -1;
    try {
        const unsigned long length = std::stoul(lengthText);
        if (length > static_cast<unsigned long>(
                std::numeric_limits<int32_t>::max() - 4)) return -1;
        return static_cast<int32_t>(length + 4);
    } catch (const std::exception&) {
        return -1;
    }
}

std::vector<PgColumnDescription> describeProtocolColumns(const QueryResult& result,
                                                          const std::string& sql,
                                                          const Session& session) {
    std::vector<PgColumnDescription> descriptions;
    descriptions.reserve(result.columns.size());

    const auto source = protocolPhysicalSourceFromQuery(sql, session);
    const auto& table = source.table;
    const auto relationOid = source.relationOid;
    const auto& catalogAttributes = source.attributes;

    for (const auto& name : result.columns) {
        PgColumnDescription description;
        description.name = name;
        description.moneyLocale = session.lcMonetary;
        const size_t columnIndex = descriptions.size();
        const std::optional<std::string> physicalColumn =
            source.projections.size() == result.columns.size()
                ? source.projections[columnIndex] : std::nullopt;
        const bool hasStructuredType =
            columnIndex < result.columnTypes.size() &&
            !result.columnTypes[columnIndex].empty();
        if (hasStructuredType) {
            Column expressionColumn;
            expressionColumn.dataType = result.columnTypes[columnIndex];
            expressionColumn.isVariableLength = true;
            const std::string typeName =
                lowerProtocolText(expressionColumn.dataType);
            description.typeOid = isByteaTypeName(typeName)
                                      ? 17
                                      : mapBuiltinTypeNameToOid(typeName);
            if (description.typeOid == INVALID_OID) description.typeOid = 25;
            description.typeSize = protocolTypeSize(description.typeOid, expressionColumn);
        }
        for (size_t i = 0; i < table.len; ++i) {
            const Column& column = table.cols[i];
            // Schema/output names are already canonical SQL identities. A
            // delimited F and an unquoted f are different physical columns.
            if (!physicalColumn || column.dataName != *physicalColumn) continue;
            const std::string physicalTypeName =
                lowerProtocolText(protocolPhysicalTypeName(column));
            const bool structuredMatchesPhysical = hasStructuredType &&
                (!column.domainName.empty() || column.isArray || physicalTypeName == "bit" ||
                 physicalTypeName == "bit varying" ||
                 physicalTypeName == "character" ||
                 physicalTypeName == "char" ||
                 physicalTypeName == "bpchar" ||
                 physicalTypeName == "varchar" ||
                 physicalTypeName == "inet" || physicalTypeName == "cidr" ||
                 physicalTypeName == "macaddr" ||
                 physicalTypeName == "macaddr8" ||
                 isGeometryTypeName(physicalTypeName)) &&
                (column.isArray || !column.domainName.empty()
                    ? ExprHelper::canonicalResultTypeName(result.columnTypes[columnIndex]) ==
                        ExprHelper::canonicalResultTypeName(physicalTypeName)
                    : lowerProtocolText(result.columnTypes[columnIndex]) == physicalTypeName);
            if (!hasStructuredType || structuredMatchesPhysical) {
                const std::string typeName = physicalTypeName;
                description.typeOid = isByteaTypeName(typeName)
                                          ? 17
                                          : mapBuiltinTypeNameToOid(typeName);
                if (description.typeOid == INVALID_OID) description.typeOid = 25;
                description.typeSize = protocolTypeSize(description.typeOid, column);
                const bool unconstrainedVarbit =
                    typeName == "bit varying" && column.dsize == 8388608;
                description.typeModifier =
                    !column.isArray && column.isVariableLength && column.dsize > 0 &&
                            !unconstrainedVarbit
                        ? static_cast<int32_t>(column.dsize + 4)
                        : -1;
            }
            description.tableOid = relationOid;
            description.attributeNumber = static_cast<uint16_t>(i + 1);
            for (const auto& attribute : catalogAttributes) {
                if (attribute.attname == column.dataName) {
                    description.tableOid = relationOid;
                    description.attributeNumber = static_cast<uint16_t>(attribute.attnum);
                    if (!hasStructuredType || structuredMatchesPhysical) {
                        // Older/current project catalogs may publish the
                        // element OID plus attndims. Do not overwrite an
                        // authoritative schema array declaration with that
                        // scalar OID or its fixed element length.
                        if (!column.isArray && attribute.atttypid != INVALID_OID)
                            description.typeOid = attribute.atttypid;
                        if (!column.isArray && attribute.attlen != 0)
                            description.typeSize = attribute.attlen;
                        description.typeModifier = attribute.atttypmod;
                    }
                    break;
                }
            }
            break;
        }
        descriptions.push_back(std::move(description));
    }
    if (descriptions.size() == 1 &&
        (descriptions[0].typeOid == 1042 ||
         descriptions[0].typeOid == 1043) &&
        descriptions[0].tableOid == INVALID_OID) {
        SQLParser parser;
        ParseResult parsed = parser.parse(sql);
        if (parsed.isValid()) {
            const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
            if (select && select->selectList.size() == 1) {
                const int32_t modifier = protocolCharacterCastModifier(
                    select->selectList.front().expr.get());
                if (modifier >= 0) descriptions[0].typeModifier = modifier;
            }
        }
    }
    return descriptions;
}

// Parse and Describe share the query host's genuine pure descriptor. Returning
// false means another existing consumer owns the shape, not an analysis error.
bool describePreparedSetResult(const std::string& sql, Session& session,
                               std::vector<PgColumnDescription>& columns) {
    SQLParser parser;
    const auto probe = parser.parseForBinding(sql);
    if(!probe.isValid()) {
        int depth=0;
        for(const auto& token:SQLParser::tokenize(sql)) {
            if(token=="("){++depth;continue;}if(token==")"){--depth;continue;}
            const auto word=SQLParser::toLower(token);
            if(!depth && (word=="union" || word=="intersect" || word=="except"))
                throw DbError("42601",probe.error);
        }
    }
    const Stmt* statement = probe.isValid() ? probe.stmt.get() : nullptr;
    if (const auto* with = dynamic_cast<const WithStmt*>(statement))
        statement = with->statement.get();
    const auto* select = dynamic_cast<const SelectStmt*>(statement);
    if (!select) return false;
    std::function<bool(const SelectStmt*)> setQuery;
    std::function<bool(const Expr*)> setValue = [&](const Expr* value) {
        if (!value) return false;
        if (const auto* child = dynamic_cast<const SelectStmt*>(value->preparedSubquery.get()))
            return setQuery(child);
        if (const auto* literal = dynamic_cast<const LiteralExpr*>(value)) {
            // This is the parser's SQL-child grammar envelope, not arbitrary
            // string data or a search for UNION in rendered expression text.
            const auto origin = SQLParser::tokenize(literal->value);
            if (origin.size()>1 && origin.front()=="(" &&
                (SQLParser::toLower(origin[1])=="select" || SQLParser::toLower(origin[1])=="with")) {
                const auto parsedChild=parser.parseForBinding(literal->value);
                return parsedChild.isValid() && setQuery(dynamic_cast<const SelectStmt*>(parsedChild.stmt.get()));
            }
        } else if (const auto* binary=dynamic_cast<const BinaryOpExpr*>(value))return setValue(binary->left.get()) || (binary->op!="::" && setValue(binary->right.get()));
        else if (const auto* quantified=dynamic_cast<const QuantifiedComparisonExpr*>(value))return setValue(quantified->left.get()) || setValue(quantified->right.get());
        else if (const auto* unary=dynamic_cast<const UnaryOpExpr*>(value))return setValue(unary->operand.get());
        else if (const auto* cast=dynamic_cast<const CastExpr*>(value))return setValue(cast->operand.get());
        else if (const auto* call=dynamic_cast<const FunctionCallExpr*>(value)) {
            for (const auto& arg:call->args)if(setValue(arg.get()))return true;
            for (const auto& arg:call->namedArgs)if(setValue(arg.value.get()))return true;
        } else if (const auto* conditional=dynamic_cast<const CaseExpr*>(value)) {
            if(setValue(conditional->switchExpr.get()) || setValue(conditional->elseExpr.get()))return true;
            for(const auto& arm:conditional->whenClauses)if(setValue(arm.first.get()) || setValue(arm.second.get()))return true;
        } else if(const auto* array=dynamic_cast<const ArrayExpr*>(value)) {
            for(const auto& cell:array->elements)if(setValue(cell.get()))return true;
        }
        return false;
    };
    setQuery = [&](const SelectStmt* query) {
        if(!query)return false;
        if(query->setOp!=SetOp::None)return true;
        for(const auto& item:query->selectList)if(setValue(item.expr.get()))return true;
        return setValue(query->whereClause.get());
    };
    if(!setQuery(select))return false;
    // The completed whole-tree binder owns common output types, UNKNOWN
    // input conversion and every branch namespace. Neither a left target
    // nor a source row is an authoritative set-query descriptor.
    const auto prepared = g_engine.prepareBoundQuery(session.currentDB, sql);
    QueryResult shape;
    for (const auto& field : prepared.output) {
        shape.columns.push_back(field.name);
        shape.columnTypes.push_back(field.type);
    }
    // Set outputs have no physical table/attribute origin, even when their
    // label happens to name the left branch's actual physical column.
    columns = describeProtocolColumns(shape, std::string{}, session);
    return !columns.empty();
}

bool describePreparedQueryHostResult(const std::string& sql, Session& session,
                                    std::vector<PgColumnDescription>& columns) {
    SQLParser parser;
    const auto probe=parser.parseForBinding(sql);
    const auto* statement=probe.isValid()?dynamic_cast<const SelectStmt*>(probe.stmt.get()):nullptr;
    if(!statement || statement->fromClause)return false;
    const bool owned=std::any_of(statement->selectList.begin(),statement->selectList.end(),[&](const auto& target) {
        const auto* call=dynamic_cast<const FunctionCallExpr*>(target.expr.get());
        return call && g_engine.ownsPreparedSetReturningCall(session.currentDB,call);
    });
    if(!owned)return false;
    const auto prepared=g_engine.prepareBoundQuery(session.currentDB,sql);
    QueryResult shape;
    for(const auto& field:prepared.output){shape.columns.push_back(field.name);shape.columnTypes.push_back(field.type);}
    columns=describeProtocolColumns(shape,sql,session);
    return !columns.empty();
}

// Describe must not execute the statement: even a SELECT can call a function
// with side effects. Resolve statically knowable SELECT/RETURNING projections
// against the catalog and leave other query shapes to the execution path.
bool describePreparedResult(const std::string& sql, Session& session,
                            std::vector<PgColumnDescription>& columns,
                            const std::vector<uint32_t>& parameterOids = {}) {
    if (parameterOids.empty() && describePreparedSetResult(sql, session, columns))
        return true;
    SQLParser parser;
    ParseResult parsed = parser.parse(sql);
    if (!parsed.isValid()) return false;
    if (const auto* shown = dynamic_cast<const SetStmt*>(parsed.stmt.get())) {
        if (shown->isShow && shown->name == "transaction_isolation") {
            QueryResult shape;
            shape.columns = {"transaction_isolation"};
            shape.columnTypes = {"text"};
            columns = describeProtocolColumns(shape, sql, session);
            return !columns.empty();
        }
    }
    const auto* select = dynamic_cast<const SelectStmt*>(parsed.stmt.get());
    if (select && !select->fromClause && parameterOids.empty()) {
        // Query-host providers publish their genuine whole-bound static
        // descriptor. Describe neither reads their backend rows nor guesses
        // an element type from a scalar callback/fallback TEXT result.
        if(describePreparedQueryHostResult(sql,session,columns))return true;
    }
    if (select && select->command == SqlCommand::Values) {
        if (select->valuesRows.empty() ||
            select->valuesRows.front().empty()) return false;
        const size_t width = select->valuesRows.front().size();
        QueryResult shape;
        std::vector<std::vector<std::string>> inputTypes(width);
        std::vector<uint32_t> directParameterOids(width, 0);
        for (const auto& row : select->valuesRows) {
            if (row.size() != width) return false;
            for (size_t i = 0; i < width; ++i) {
                if (!row[i]) return false;
                const std::string expression = row[i]->toString();
                if (expression.empty()) return false;
                const auto* literal =
                    dynamic_cast<const LiteralExpr*>(row[i].get());
                std::string typeName = literal && !literal->typeName.empty()
                    ? ExprHelper::canonicalResultTypeName(literal->typeName)
                    : ExprHelper::inferValuesResultType(expression);
                if (expression.size() > 1 && expression.front() == '$' &&
                    std::all_of(expression.begin() + 1, expression.end(),
                                [](unsigned char ch) {
                                    return std::isdigit(ch);
                                })) {
                    const unsigned long index = std::stoul(expression.substr(1));
                    if (index == 0 || index > parameterOids.size()) return false;
                    const uint32_t parameterOid = parameterOids[index - 1];
                    if (const char* castType =
                            valuesParameterCastType(parameterOid)) {
                        typeName = castType;
                    } else if (select->valuesRows.size() > 1) {
                        return false;
                    }
                    if (select->valuesRows.size() == 1)
                        directParameterOids[i] = parameterOid;
                }
                inputTypes[i].push_back(std::move(typeName));
            }
        }
        for (size_t i = 0; i < width; ++i) {
            std::string typeName;
            std::string error;
            if (!ExprHelper::resolveValuesResultType(
                    inputTypes[i], typeName, error)) return false;
            shape.columns.push_back("column" + std::to_string(i + 1));
            shape.columnTypes.push_back(std::move(typeName));
        }
        columns = describeProtocolColumns(shape, sql, session);
        if (select->valuesRows.size() == 1) {
            for (size_t i = 0; i < columns.size(); ++i) {
                if (directParameterOids[i] == 0) continue;
                columns[i].typeOid = directParameterOids[i];
                columns[i].typeSize =
                    protocolTypeSize(columns[i].typeOid, Column{});
            }
        }
        return !columns.empty();
    }
    const std::vector<SelectItem>* projections = nullptr;
    std::string sourceName;
    std::string sourceAlias;
    if (select) {
        if (!select->ctes.empty() || select->setOp != SetOp::None ||
            !select->valuesRows.empty()) return false;
        projections = &select->selectList;
        if (select->fromClause) {
            if (select->fromClause->type != FromItem::Type::Table) return false;
            sourceName = select->fromClause->tableName;
            sourceAlias = select->fromClause->alias;
        }
    } else if (const auto* insert =
                   dynamic_cast<const InsertStmt*>(parsed.stmt.get())) {
        if (insert->returning.empty() ||
            insert->returningOptions.oldAliased ||
            insert->returningOptions.newAliased) return false;
        projections = &insert->returning;
        sourceName = insert->tableName;
    } else if (const auto* update =
                   dynamic_cast<const UpdateStmt*>(parsed.stmt.get())) {
        if (update->returning.empty() || update->fromClause ||
            update->returningOptions.oldAliased ||
            update->returningOptions.newAliased) return false;
        projections = &update->returning;
        sourceName = update->tableName;
        sourceAlias = update->alias;
    } else if (const auto* deletion =
                   dynamic_cast<const DeleteStmt*>(parsed.stmt.get())) {
        if (deletion->returning.empty() || deletion->usingClause ||
            deletion->returningOptions.oldAliased ||
            deletion->returningOptions.newAliased) return false;
        projections = &deletion->returning;
        sourceName = deletion->tableName;
        sourceAlias = deletion->alias;
    } else if (const auto* merge =
                   dynamic_cast<const MergeStmt*>(parsed.stmt.get())) {
        if (merge->returning.empty() ||
            merge->returningOptions.oldAliased ||
            merge->returningOptions.newAliased) return false;
        projections = &merge->returning;
        sourceName = merge->targetTable;
        sourceAlias = merge->targetAlias;
    } else {
        return false;
    }
    if (!projections || projections->empty()) return false;
    const auto outputAlias = [](const std::string& token) {
        CatalogManager::QualifiedName name;
        if (CatalogManager::parseQualifiedName(token, name, true) && name.schema.empty()) {
            return name.name;
        }
        return token;
    };
    // FROM aliases retain source SQL quoting, while ColumnRef components
    // have already been decoded/folded by the parser. Canonicalize once.
    sourceAlias = outputAlias(sourceAlias);
    std::string relation;
    TableSchema schema;
    std::map<std::string, std::string> typeHints;
    bool virtualPgClass = false;
    bool virtualPgSettings = false;
    bool virtualPgStatActivity = false;
    if (!sourceName.empty()) {
        relation = resolveTableName(session, sourceName);
        if (g_engine.tableExists(session.currentDB, relation)) {
            schema = g_engine.getTableSchema(session.currentDB, relation);
        } else {
            CatalogManager::QualifiedName name;
            virtualPgClass = select &&
                CatalogManager::parseQualifiedName(sourceName, name, true) &&
                name.name == "pg_class" &&
                (name.schema.empty() || name.schema == "pg_catalog");
            CatalogManager::QualifiedName settingsName;
            const bool namesPgSettings = select &&
                CatalogManager::parseQualifiedName(sourceName, settingsName, true) &&
                settingsName.name == "pg_settings" &&
                (settingsName.schema.empty() ||
                 settingsName.schema == "pg_catalog");
            virtualPgSettings = namesPgSettings &&
                (settingsName.schema == "pg_catalog" ||
                 (!g_engine.tableExists(session.currentDB, relation) &&
                  !g_engine.viewExists(session.currentDB, relation)));
            CatalogManager::QualifiedName activityName;
            const bool namesPgStatActivity = select &&
                CatalogManager::parseQualifiedName(sourceName, activityName, true) &&
                activityName.name == "pg_stat_activity" &&
                (activityName.schema.empty() ||
                 activityName.schema == "pg_catalog");
            virtualPgStatActivity = namesPgStatActivity &&
                (activityName.schema == "pg_catalog" ||
                 (!g_engine.tableExists(session.currentDB, relation) &&
                  !g_engine.viewExists(session.currentDB, relation)));
            if (!virtualPgClass && !virtualPgSettings &&
                !virtualPgStatActivity) return false;

            // Match each virtual catalog's supported typed execution shape
            // without executing SELECT (Describe must never acquire rows or
            // cause side effects). pg_settings and pg_stat_activity
            // intentionally expose only their implemented subsets.
            const std::vector<std::pair<std::string, std::string>> fields =
                virtualPgClass
                    ? std::vector<std::pair<std::string, std::string>>{
                          {"oid", "oid"}, {"relname", "name"},
                          {"relnamespace", "oid"}, {"relkind", "\"char\""},
                          {"relnatts", "smallint"},
                          {"relpersistence", "\"char\""},
                          {"relowner", "oid"}}
                    : virtualPgSettings
                    ? std::vector<std::pair<std::string, std::string>>{
                          {"name", "text"}, {"setting", "text"},
                          {"unit", "text"}}
                    : std::vector<std::pair<std::string, std::string>>{
                          {"pid", "integer"}, {"datname", "name"},
                          {"usename", "name"}, {"state", "text"},
                          {"query", "text"}};
            for (const auto& field : fields) {
                Column column;
                column.dataName = field.first;
                column.dataType = field.second;
                schema.cols[schema.len++] = std::move(column);
            }
            relation = virtualPgClass ? "pg_class"
                : virtualPgSettings ? "pg_settings" : "pg_stat_activity";
        }
        for (size_t i = 0; i < schema.len; ++i) {
            const Column& column = schema.cols[i];
            const auto declaredType = protocolPhysicalTypeName(column);
            typeHints[column.dataName] = declaredType;
            typeHints[relation + "." + column.dataName] = declaredType;
            if (virtualPgClass && sourceAlias.empty())
                typeHints["pg_catalog.pg_class." + column.dataName] = declaredType;
            if (virtualPgSettings && sourceAlias.empty())
                typeHints["pg_catalog.pg_settings." + column.dataName] =
                    declaredType;
            if (virtualPgStatActivity && sourceAlias.empty())
                typeHints["pg_catalog.pg_stat_activity." + column.dataName] =
                    declaredType;
            if (!sourceAlias.empty()) {
                typeHints[sourceAlias + "." + column.dataName] =
                    declaredType;
            }
        }
    }

    QueryResult shape;
    std::vector<std::string> outputNames;
    std::vector<uint32_t> directParameterOids;
    std::vector<int32_t> projectionTypeModifiers;
    const auto appendPhysicalColumns = [&]() {
        for (size_t i = 0; i < schema.len; ++i) {
            shape.columns.push_back(schema.cols[i].dataName);
            shape.columnTypes.push_back(protocolPhysicalTypeName(schema.cols[i]));
            outputNames.push_back(schema.cols[i].dataName);
            directParameterOids.push_back(0);
            projectionTypeModifiers.push_back(-1);
        }
    };
    const auto qualifierMatches = [&](const ColumnRefExpr& reference) {
        if (reference.table.empty()) return reference.schema.empty();
        if (!sourceAlias.empty())
            return reference.schema.empty() && reference.table == sourceAlias;
        CatalogManager::QualifiedName source;
        if (!CatalogManager::parseQualifiedName(sourceName, source, true)) return false;
        if (reference.table != source.name && reference.table != relation) return false;
        if (reference.schema.empty()) return true;
        return reference.schema == source.schema ||
               (source.schema.empty() && reference.schema == "public") ||
               ((virtualPgClass || virtualPgSettings || virtualPgStatActivity) &&
                reference.schema == "pg_catalog");
    };
    for (const auto& item : *projections) {
        if (!item.expr) return false;
        const auto* literal = dynamic_cast<const LiteralExpr*>(item.expr.get());
        const auto* reference = dynamic_cast<const ColumnRefExpr*>(item.expr.get());
        if ((literal && literal->value == "*") ||
            (reference && reference->column == "*")) {
            if (relation.empty()) return false;
            if (virtualPgSettings || virtualPgStatActivity) return false;
            if (reference && !qualifierMatches(*reference)) return false;
            appendPhysicalColumns();
            continue;
        }
        if (reference && !relation.empty()) {
            if (!qualifierMatches(*reference)) return false;
            bool found = false;
            for (size_t i = 0; i < schema.len; ++i) {
                if (schema.cols[i].dataName != reference->column) continue;
                shape.columns.push_back(schema.cols[i].dataName);
                shape.columnTypes.push_back(protocolPhysicalTypeName(schema.cols[i]));
                outputNames.push_back(item.alias.empty() ? reference->column
                                                          : outputAlias(item.alias));
                directParameterOids.push_back(0);
                projectionTypeModifiers.push_back(-1);
                found = true;
                break;
            }
            if (!found) return false;
            continue;
        }
        const std::string expression = item.expr->toString();
        if (expression.empty()) return false;
        // Use a synthetic lookup key: an expression aliased to a physical
        // column must not inherit that column's table OID/attribute number.
        shape.columns.push_back("\x1f" "expression_" +
                                std::to_string(shape.columns.size()));
        const std::string inferredType =
            literal && !literal->typeName.empty()
                ? ExprHelper::canonicalResultTypeName(literal->typeName)
                : ExprHelper::inferParsedResultType(item.expr.get(), typeHints,
                                                  session.currentDB, &g_engine);
        shape.columnTypes.push_back(inferredType);
        std::string outputName = item.alias.empty() ? std::string{} : outputAlias(item.alias);
        if (outputName.empty() && virtualPgClass) {
            const Expr* operand = item.expr.get();
            while (operand) {
                if (const auto* cast = dynamic_cast<const CastExpr*>(operand)) {
                    operand = cast->operand.get();
                } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(operand);
                           binary && binary->op == "::") {
                    operand = binary->left.get();
                } else break;
            }
            if (const auto* column = dynamic_cast<const ColumnRefExpr*>(operand))
                outputName = column->column;
        }
        if (outputName.empty()) {
            const auto* call =
                dynamic_cast<const FunctionCallExpr*>(item.expr.get());
            const auto* cast = dynamic_cast<const CastExpr*>(item.expr.get());
            const auto* binary =
                dynamic_cast<const BinaryOpExpr*>(item.expr.get());
            if (call) {
                outputName = lowerProtocolText(call->funcName);
            } else if (cast || (binary && binary->op == "::") ||
                       (literal && !literal->typeName.empty())) {
                switch (mapBuiltinTypeNameToOid(inferredType)) {
                    case 16: outputName = "bool"; break;
                    case 20: outputName = "int8"; break;
                    case 21: outputName = "int2"; break;
                    case 23: outputName = "int4"; break;
                    case 700: outputName = "float4"; break;
                    case 701: outputName = "float8"; break;
                    case 1042: outputName = "bpchar"; break;
                    default: outputName = lowerProtocolText(inferredType); break;
                }
            } else if (dynamic_cast<const CaseExpr*>(item.expr.get())) {
                outputName = "case";
            } else if (reference && relation.empty() &&
                       (inferredType == "name" || inferredType == "date" ||
                        inferredType == "timestamp" ||
                        inferredType == "timestamptz") &&
                       (lowerProtocolText(reference->column) == "current_user" ||
                        lowerProtocolText(reference->column) == "session_user" ||
                        lowerProtocolText(reference->column) == "user" ||
                        lowerProtocolText(reference->column) == "current_date" ||
                        lowerProtocolText(reference->column) == "current_timestamp" ||
                        lowerProtocolText(reference->column) == "localtimestamp")) {
                outputName = lowerProtocolText(reference->column);
            } else {
                outputName = "?column?";
            }
        }
        outputNames.push_back(std::move(outputName));
        uint32_t parameterOid = 0;
        if (expression.size() > 1 && expression.front() == '$' &&
            std::all_of(expression.begin() + 1, expression.end(),
                        [](unsigned char ch) { return std::isdigit(ch); })) {
            const unsigned long index = std::stoul(expression.substr(1));
            if (index > 0 && index <= parameterOids.size())
                parameterOid = parameterOids[index - 1];
        }
        directParameterOids.push_back(parameterOid);
        // The physical-descriptor adapter below uses SELECT * and cannot
        // retain a projection cast. Read typmods from the original live AST.
        projectionTypeModifiers.push_back(protocolCharacterCastModifier(item.expr.get()));
    }
    if (shape.columns.empty()) return false;
    const std::string descriptionSql =
        (virtualPgSettings || virtualPgStatActivity)
            ? "SELECT 1"
            : sql;
    columns = describeProtocolColumns(shape,
                                      descriptionSql, session);
    for (size_t i = 0; i < columns.size(); ++i) {
        columns[i].name = outputNames[i];
        if ((columns[i].typeOid == 1042 || columns[i].typeOid == 1043) &&
            projectionTypeModifiers[i] >= 0)
            columns[i].typeModifier = projectionTypeModifiers[i];
        if (directParameterOids[i] != 0) {
            columns[i].typeOid = directParameterOids[i];
            columns[i].typeSize = protocolTypeSize(columns[i].typeOid, Column{});
        }
    }
    return true;
}

std::string commandTagFor(const std::string& sql, const std::vector<std::string>& lines,
                          size_t rowCount) {
    std::string keyword = firstSqlKeyword(sql);
    if (startsWithSqlPhrase(sql, "prepare transaction")) {
        return "PREPARE TRANSACTION";
    }
    if (startsWithSqlPhrase(sql, "commit prepared")) {
        return "COMMIT PREPARED";
    }
    if (startsWithSqlPhrase(sql, "rollback prepared")) {
        return "ROLLBACK PREPARED";
    }
    if (startsWithSqlPhrase(sql, "discard all")) {
        return "DISCARD ALL";
    }
    if (startsWithSqlPhrase(sql, "discard sequences")) {
        return "DISCARD SEQUENCES";
    }
    if (startsWithSqlPhrase(sql, "discard temp") ||
        startsWithSqlPhrase(sql, "discard temporary")) {
        return "DISCARD TEMP";
    }
    if (startsWithSqlPhrase(sql, "drop materialized view")) {
        return "DROP MATERIALIZED VIEW";
    }
    if (startsWithSqlPhrase(sql, "create unique index")) {
        return "CREATE INDEX";
    }
    if (startsWithSqlPhrase(sql, "create temp table") ||
        startsWithSqlPhrase(sql, "create temporary table")) {
        return "CREATE TABLE";
    }
    if (startsWithSqlPhrase(sql, "create unlogged table")) {
        return "CREATE TABLE";
    }
    if (startsWithSqlPhrase(sql, "create or replace view")) {
        return "CREATE VIEW";
    }
    if (startsWithSqlPhrase(sql, "create or replace function")) {
        return "CREATE FUNCTION";
    }
    if (startsWithSqlPhrase(sql, "create or replace procedure")) {
        return "CREATE PROCEDURE";
    }
    const auto mutationCount = [](const std::string& line,
                                  const std::string& prefix)
        -> std::optional<std::string> {
        if (line.rfind(prefix, 0) != 0) return std::nullopt;
        const size_t begin = prefix.size();
        size_t end = begin;
        while (end < line.size() &&
               std::isdigit(static_cast<unsigned char>(line[end]))) {
            ++end;
        }
        if (end == begin || end >= line.size() || line[end] != ' ')
            return std::nullopt;
        return line.substr(begin, end - begin);
    };
    if (keyword == "insert") {
        for (const auto& line : lines) {
            if (line.rfind("INSERT ", 0) == 0) return line;

            // The legacy executor reports successful VALUES/SELECT inserts as
            // "N row(s) inserted". Normalize that output to PostgreSQL's
            // command tag so simple-query clients observe the real count.
            for (const std::string suffix : {" row(s) inserted", " rows inserted",
                                              " row(s) replaced"}) {
                if (line.size() <= suffix.size() ||
                    line.compare(line.size() - suffix.size(), suffix.size(), suffix) != 0) {
                    continue;
                }
                const std::string count = line.substr(0, line.size() - suffix.size());
                if (!count.empty() && std::all_of(count.begin(), count.end(),
                                                  [](unsigned char c) { return std::isdigit(c); })) {
                    return "INSERT 0 " + count;
                }
            }
        }
        return "INSERT 0 0";
    }
    if (keyword == "update") {
        for (const auto& line : lines) {
            if (line.rfind("UPDATE ", 0) == 0) return line;
            if (const auto count = mutationCount(line, "Update done ("))
                return "UPDATE " + *count;
        }
        return "UPDATE 0";
    }
    if (keyword == "delete") {
        for (const auto& line : lines) {
            if (const auto count = mutationCount(line, "Delete done ("))
                return "DELETE " + *count;
        }
        return "DELETE 0";
    }
    if (keyword == "select" || keyword == "show" || keyword == "values" || keyword == "with") {
        return (keyword == "show") ? "SHOW" : "SELECT " + std::to_string(rowCount);
    }
    if (keyword == "begin") return "BEGIN";
    if (keyword == "start") return "START TRANSACTION";
    if (keyword == "commit" || keyword == "end") return "COMMIT";
    if (keyword == "rollback" || keyword == "abort") return "ROLLBACK";
    if (keyword == "set") return "SET";
    if (keyword == "truncate") return "TRUNCATE TABLE";
    if (keyword == "lock") return "LOCK TABLE";
    if (keyword == "create" || keyword == "alter" || keyword == "drop" ||
        keyword == "grant" || keyword == "revoke") {
        std::vector<std::string> words = splitProtocolFields(trimText(sql));
        if (words.size() >= 2) {
            std::string tag = words[0] + " " + words[1];
            for (char& c : tag) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
            return tag;
        }
    }
    std::string tag = keyword;
    for (char& c : tag) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return tag.empty() ? "OK" : tag;
}

namespace {
// Analyze actual WHERE expressions, including on empty inputs. A substring
// from the first WHERE to end-of-statement crosses CTE/derived query scopes
// and mistakes a following LATERAL (...) for an undefined function. Shared
// SQL tokens retain quoted/dollar literals and discard comments; parenthesis
// depth bounds each predicate before its expression AST is inspected.
std::string whereUnknownFunctionError(const std::string& sql,
                                       Session& session) {
    static const char* known[] = {
        "length", "char_length", "character_length", "octet_length",
        "bit_length", "upper", "lower", "initcap", "btrim", "ltrim",
        "rtrim", "reverse", "left", "right", "lpad", "rpad",
        "repeat", "ascii", "chr", "md5", "sha256", "hash",
        "abs", "ceil", "ceiling", "floor", "round", "trunc",
        "sqrt", "exp", "ln", "log", "power", "pow", "sign",
        "width_bucket", "substring", "substr", "translate", "replace",
        "split_part", "concat", "concat_ws", "coalesce", "nullif",
        "greatest", "least", "to_char", "to_date", "to_number",
        "to_timestamp", "date_trunc", "date_part", "extract",
        "age", "justify_days", "justify_hours", "now", "current_date",
        "current_time", "current_timestamp", "random", "mod", "cbrt",
        "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "cot",
        "degrees", "radians", "pi", "sinh", "cosh", "tanh",
        "asinh", "acosh", "atanh", "div", "count", "sum", "avg",
        "min", "max", "string_agg", "array_agg", "bool_and", "bool_or",
        "every", "stddev", "variance", "position", "trim", "overlay",
        "starts_with", "encode", "decode", "format", "uuid", "gen_random_uuid",
        "uuidv4", "uuidv7", "uuid_extract_version", "uuid_extract_timestamp",
        "unnest", "array_lower", "array_upper", "array_length", "cardinality",
        "row_number", "rank", "dense_rank", "ntile", "lag", "lead",
        "first_value", "last_value", "nth_value",
        "pg_notify", "pg_notification_queue_usage", "pg_listening_channels",
        "xml_is_well_formed", "xml_is_well_formed_content",
        "xml_is_well_formed_document", "xml_is_document", "xmlconcat",
        "xmlcomment"
    };
    ExprEvaluator functionResolver;
    functionResolver.setCurrentDB(session.currentDB);
    const auto tokens = SQLParser::tokenize(sql);
    std::vector<int> depths;
    int depth = 0;
    for (const auto& token : tokens) {
        depths.push_back(depth);
        if (token == "(") ++depth;
        else if (token == ")") --depth;
    }
    for (size_t where = 0; where < tokens.size(); ++where) {
        if (SQLParser::toLower(tokens[where]) != "where") continue;
        const int ownerDepth = depths[where];
        size_t end = where + 1;
        for (; end < tokens.size(); ++end) {
            if (depths[end] < ownerDepth ||
                (tokens[end] == ")" && depths[end] == ownerDepth)) break;
            if (depths[end] != ownerDepth) continue;
            const auto word = SQLParser::toLower(tokens[end]);
            if (word == ";" || word == "group" || word == "order" ||
                word == "having" || word == "limit" || word == "offset" ||
                word == "fetch" || word == "for" || word == "window" ||
                word == "union" || word == "intersect" || word == "except" ||
                word == "returning") break;
        }
        std::string predicate = "select 1 where ";
        for (size_t i = where + 1; i < end; ++i) predicate += tokens[i] + " ";
        SQLParser parser;
        const auto parsed = parser.parse(predicate);
        const auto* select = parsed.success
            ? dynamic_cast<const SelectStmt*>(parsed.stmt.get()) : nullptr;
        if (!select || !select->whereClause) continue; // execution owns syntax errors

        const FunctionCallExpr* missing = nullptr;
        std::function<void(const Expr*)> visit = [&](const Expr* expr) {
            if (!expr || missing) return;
            if (const auto* call = dynamic_cast<const FunctionCallExpr*>(expr)) {
                const auto name = SQLParser::toLower(call->funcName);
                bool supported = name == "exists" || name == "in" ||
                    name == "not in" || name == "between" || name == "not between" ||
                    name == "any" || name == "all";
                for (const auto* builtin : known) supported |= name == builtin;
                supported |= functionResolver.hasScalarFunction(call);
                if (!supported) { missing = call; return; }
                for (const auto& arg : call->args) visit(arg.get());
                for (const auto& arg : call->namedArgs) visit(arg.value.get());
                visit(call->filter.get());
            } else if (const auto* unary = dynamic_cast<const UnaryOpExpr*>(expr)) {
                visit(unary->operand.get());
            } else if (const auto* binary = dynamic_cast<const BinaryOpExpr*>(expr)) {
                visit(binary->left.get()); visit(binary->right.get());
            } else if (const auto* quantified = dynamic_cast<const QuantifiedComparisonExpr*>(expr)) {
                visit(quantified->left.get()); visit(quantified->right.get());
            } else if (const auto* cast = dynamic_cast<const CastExpr*>(expr)) {
                visit(cast->operand.get());
            } else if (const auto* conditional = dynamic_cast<const CaseExpr*>(expr)) {
                visit(conditional->switchExpr.get());
                for (const auto& arm : conditional->whenClauses) {
                    visit(arm.first.get()); visit(arm.second.get());
                }
                visit(conditional->elseExpr.get());
            } else if (const auto* array = dynamic_cast<const ArrayExpr*>(expr)) {
                for (const auto& element : array->elements) visit(element.get());
            } else if (const auto* row = dynamic_cast<const RowExpr*>(expr)) {
                for (const auto& element : row->elements) visit(element.get());
            }
        };
        visit(select->whereClause.get());
        if (!missing) continue;

        // Preserve the existing first-argument diagnostic for a simple FROM
        // relation, without treating quoted identifiers or literals as names.
        std::string table;
        for (size_t i = where; i-- > 0;) {
            if (depths[i] != ownerDepth) continue;
            const auto word = SQLParser::toLower(tokens[i]);
            if (word == "select" || word == ";") break;
            if ((word == "from" || word == "update") && i + 1 < where) {
                table = tokens[i + 1];
                for (size_t part = i + 2; part + 1 < where && tokens[part] == "."; part += 2)
                    table += "." + tokens[part + 1];
                break;
            }
        }
        std::string type = "unknown";
        const auto* arg = missing->args.empty() ? nullptr
            : dynamic_cast<const ColumnRefExpr*>(missing->args.front().get());
        if (arg && !table.empty() && table != "(") {
            std::string physical;
            try {
                physical = ::resolveTableName(session, table);
            } catch (const DbError& error) {
                if (error.sqlState() != "55000") throw;
                // Analysis needs a materialized view's column types, not
                // permission to scan it. Undefined functions must be rejected
                // before the executor's unpopulated-view access gate.
                CatalogManager::QualifiedName name;
                if (!CatalogManager::parseQualifiedName(table, name, true)) throw;
                std::vector<std::string> schemas;
                if (!name.schema.empty()) schemas.push_back(name.schema);
                else {
                    std::string canonical;
                    if (!parseSessionSearchPath(session.searchPath, schemas, canonical))
                        schemas = {"public"};
                }
                for (const auto& entry : schemas) {
                    const auto schemaName = expandSessionSearchPathEntry(entry, session.username);
                    if (const auto view = g_engine.resolveMaterializedView(
                            session.currentDB, schemaName, name.name)) {
                        physical = view->backingTable;
                        break;
                    }
                }
                if (physical.empty()) throw;
            }
            const TableSchema schema = g_engine.getTableSchema(session.currentDB, physical);
            for (size_t i = 0; i < schema.len; ++i) {
                if (schema.cols[i].dataName != arg->column) continue;
                const auto dt = SQLParser::toLower(schema.cols[i].dataType);
                if (dt.find("int") != std::string::npos) type = "integer";
                else if (dt.find("varchar") != std::string::npos ||
                         dt.find("character varying") != std::string::npos)
                    type = "character varying";
                else if (dt.find("char") != std::string::npos) type = "character";
                else if (dt.find("text") != std::string::npos) type = "text";
                else if (dt.find("numeric") != std::string::npos ||
                         dt.find("decimal") != std::string::npos) type = "numeric";
                break;
            }
        }
        return "function " + SQLParser::toLower(missing->funcName) +
            "(" + type + ") does not exist";
    }
    return "";
}
}  // namespace

enum class CopyWireDirection { None, FromStdin, ToStdout };

struct CopyWirePlan {
    bool matched = false;
    bool wire = false;
    CopyWireDirection direction = CopyWireDirection::None;
    std::string relation;
    std::string physicalTable;
    std::vector<std::string> requestedColumns;
    std::vector<size_t> columnIndexes;
    char delimiter = '\t';
    std::string nullMarker = "\\N";
    std::string sqlState;
    std::string error;
};

namespace {

constexpr size_t kMaxCopyRecordBytes = 16 * 1024 * 1024;

struct CopyToken {
    enum class Kind {
        End,
        Word,
        String,
        LeftParen,
        RightParen,
        Comma,
        Dot,
        Equal,
        Semicolon,
        Invalid,
    } kind = Kind::End;
    std::string text;
    bool quoted = false;
};

class CopySqlScanner {
public:
    explicit CopySqlScanner(const std::string& sql) : sql_(sql) {}

    CopyToken next() {
        if (held_) {
            CopyToken token = std::move(*held_);
            held_.reset();
            return token;
        }
        if (!skipSpaceAndComments()) {
            return {CopyToken::Kind::Invalid,
                    "unterminated comment in COPY statement", false};
        }
        if (offset_ == sql_.size()) return {};
        const char ch = sql_[offset_++];
        switch (ch) {
            case '(': return {CopyToken::Kind::LeftParen, "(", false};
            case ')': return {CopyToken::Kind::RightParen, ")", false};
            case ',': return {CopyToken::Kind::Comma, ",", false};
            case '.': return {CopyToken::Kind::Dot, ".", false};
            case '=': return {CopyToken::Kind::Equal, "=", false};
            case ';': return {CopyToken::Kind::Semicolon, ";", false};
            case '"': return quotedToken('"', CopyToken::Kind::Word);
            case '\'': return quotedToken('\'', CopyToken::Kind::String);
            default: break;
        }
        if (std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' ||
            ch == '$') {
            const size_t begin = offset_ - 1;
            while (offset_ < sql_.size()) {
                const unsigned char current =
                    static_cast<unsigned char>(sql_[offset_]);
                if (!std::isalnum(current) && current != '_' &&
                    current != '$') {
                    break;
                }
                ++offset_;
            }
            std::string text = sql_.substr(begin, offset_ - begin);
            std::transform(text.begin(), text.end(), text.begin(),
                           [](unsigned char value) {
                               return static_cast<char>(std::tolower(value));
                           });
            return {CopyToken::Kind::Word, std::move(text), false};
        }
        return {CopyToken::Kind::Invalid,
                std::string("unexpected character '") + ch +
                    "' in COPY statement",
                false};
    }

    CopyToken peek() {
        CopyToken token = next();
        held_ = token;
        return token;
    }

private:
    bool skipSpaceAndComments() {
        while (offset_ < sql_.size()) {
            if (std::isspace(static_cast<unsigned char>(sql_[offset_]))) {
                ++offset_;
                continue;
            }
            if (offset_ + 1 < sql_.size() &&
                sql_[offset_] == '-' && sql_[offset_ + 1] == '-') {
                offset_ += 2;
                while (offset_ < sql_.size() && sql_[offset_] != '\n' &&
                       sql_[offset_] != '\r') {
                    ++offset_;
                }
                continue;
            }
            if (offset_ + 1 < sql_.size() &&
                sql_[offset_] == '/' && sql_[offset_ + 1] == '*') {
                offset_ += 2;
                size_t depth = 1;
                while (offset_ < sql_.size() && depth != 0) {
                    if (offset_ + 1 < sql_.size() &&
                        sql_[offset_] == '/' && sql_[offset_ + 1] == '*') {
                        ++depth;
                        offset_ += 2;
                    } else if (offset_ + 1 < sql_.size() &&
                               sql_[offset_] == '*' &&
                               sql_[offset_ + 1] == '/') {
                        --depth;
                        offset_ += 2;
                    } else {
                        ++offset_;
                    }
                }
                if (depth != 0) return false;
                continue;
            }
            break;
        }
        return true;
    }

    CopyToken quotedToken(char quote, CopyToken::Kind kind) {
        std::string text;
        while (offset_ < sql_.size()) {
            const char ch = sql_[offset_++];
            if (ch != quote) {
                text.push_back(ch);
                continue;
            }
            if (offset_ < sql_.size() && sql_[offset_] == quote) {
                text.push_back(quote);
                ++offset_;
                continue;
            }
            return {kind, std::move(text), true};
        }
        return {CopyToken::Kind::Invalid,
                quote == '\'' ? "unterminated COPY string literal"
                               : "unterminated quoted COPY identifier",
                false};
    }

    const std::string& sql_;
    size_t offset_ = 0;
    std::optional<CopyToken> held_;
};

bool copyKeyword(const CopyToken& token, const std::string& keyword) {
    return token.kind == CopyToken::Kind::Word && !token.quoted &&
           token.text == keyword;
}

void copyError(CopyWirePlan& plan, std::string sqlState,
               std::string message) {
    plan.sqlState = std::move(sqlState);
    plan.error = std::move(message);
}

void copyUnsupported(CopyWirePlan& plan, const std::string& feature) {
    copyError(plan, "0A000",
              "COPY " + feature + " is not supported by the wire protocol");
}

bool parseCopyOptionValue(CopySqlScanner& scanner, CopyToken& value,
                          CopyWirePlan& plan) {
    if (scanner.peek().kind == CopyToken::Kind::Equal) (void)scanner.next();
    value = scanner.next();
    if (value.kind == CopyToken::Kind::Word ||
        value.kind == CopyToken::Kind::String) {
        return true;
    }
    copyError(plan, "42601", "COPY option requires a value");
    return false;
}

CopyWirePlan parseCopyWirePlan(const std::string& sql) {
    CopyWirePlan plan;
    CopySqlScanner scanner(sql);
    CopyToken token = scanner.next();
    if (!copyKeyword(token, "copy")) return plan;
    plan.matched = true;

    token = scanner.next();
    if (copyKeyword(token, "binary")) {
        copyUnsupported(plan, "BINARY");
        return plan;
    }
    if (token.kind == CopyToken::Kind::LeftParen) {
        copyUnsupported(plan, "query form");
        return plan;
    }
    if (copyKeyword(token, "only")) token = scanner.next();
    if (token.kind != CopyToken::Kind::Word) {
        copyError(plan, "42601", "COPY requires a relation name");
        return plan;
    }
    plan.relation = token.text;
    if (scanner.peek().kind == CopyToken::Kind::Dot) {
        (void)scanner.next();
        const CopyToken table = scanner.next();
        if (table.kind != CopyToken::Kind::Word) {
            copyError(plan, "42601", "malformed qualified relation name in COPY");
            return plan;
        }
        plan.relation += "." + table.text;
        if (scanner.peek().kind == CopyToken::Kind::Dot) {
            copyUnsupported(plan, "three-part relation names");
            return plan;
        }
    }

    if (scanner.peek().kind == CopyToken::Kind::LeftParen) {
        (void)scanner.next();
        while (true) {
            const CopyToken column = scanner.next();
            if (column.kind != CopyToken::Kind::Word) {
                copyError(plan, "42601", "malformed COPY column list");
                return plan;
            }
            plan.requestedColumns.push_back(column.text);
            token = scanner.next();
            if (token.kind == CopyToken::Kind::RightParen) break;
            if (token.kind != CopyToken::Kind::Comma) {
                copyError(plan, "42601", "malformed COPY column list");
                return plan;
            }
        }
        if (plan.requestedColumns.empty()) {
            copyError(plan, "42601", "COPY column list cannot be empty");
            return plan;
        }
    }

    token = scanner.next();
    if (copyKeyword(token, "freeze")) {
        copyUnsupported(plan, "FREEZE");
        return plan;
    }
    const bool from = copyKeyword(token, "from");
    const bool to = copyKeyword(token, "to");
    if (!from && !to) {
        copyError(plan, "42601", "COPY requires FROM or TO");
        return plan;
    }
    token = scanner.next();
    if (copyKeyword(token, "program")) {
        copyUnsupported(plan, "PROGRAM");
        return plan;
    }
    if (token.kind == CopyToken::Kind::String) {
        // Server-side file COPY remains owned by the SQL executor.  Do not
        // reinterpret it as a wire operation, but fail closed on option
        // syntax that the legacy file executor would otherwise ignore.
        token = scanner.next();
        if (token.kind == CopyToken::Kind::Semicolon) token = scanner.next();
        if (token.kind != CopyToken::Kind::End) {
            copyUnsupported(plan, "server-side file options");
        }
        return plan;
    }
    if (from && copyKeyword(token, "stdin")) {
        plan.direction = CopyWireDirection::FromStdin;
    } else if (to && copyKeyword(token, "stdout")) {
        plan.direction = CopyWireDirection::ToStdout;
    } else if (copyKeyword(token, "stdin") || copyKeyword(token, "stdout")) {
        copyError(plan, "42601", "COPY direction does not match STDIN/STDOUT");
        return plan;
    } else {
        copyError(plan, "42601", "COPY requires a file, PROGRAM, STDIN, or STDOUT");
        return plan;
    }
    plan.wire = true;

    token = scanner.next();
    if (token.kind == CopyToken::Kind::End ||
        token.kind == CopyToken::Kind::Semicolon) {
        if (token.kind == CopyToken::Kind::Semicolon &&
            scanner.next().kind != CopyToken::Kind::End) {
            copyError(plan, "42601", "trailing input after COPY statement");
        }
        return plan;
    }
    if (copyKeyword(token, "where")) {
        copyUnsupported(plan, "WHERE");
        return plan;
    }
    if (!copyKeyword(token, "with")) {
        copyUnsupported(plan, "legacy option syntax");
        return plan;
    }
    if (scanner.next().kind != CopyToken::Kind::LeftParen) {
        copyUnsupported(plan, "legacy option syntax");
        return plan;
    }

    std::set<std::string> seenOptions;
    while (true) {
        token = scanner.next();
        if (token.kind == CopyToken::Kind::RightParen) break;
        if (token.kind != CopyToken::Kind::Word || token.quoted) {
            copyError(plan, "42601", "malformed COPY option list");
            return plan;
        }
        const std::string option = token.text;
        if (!seenOptions.insert(option).second) {
            copyError(plan, "42601", "COPY option \"" + option +
                                      "\" specified more than once");
            return plan;
        }
        if (option == "binary" || option == "csv" || option == "freeze" ||
            option == "header" || option == "on_error" ||
            option == "reject_limit" || option == "log_verbosity" ||
            option == "oids" || option.rfind("force_", 0) == 0) {
            copyUnsupported(plan, option);
            return plan;
        }

        CopyToken value;
        if (!parseCopyOptionValue(scanner, value, plan)) return plan;
        if (option == "format") {
            std::string format = lowerAscii(value.text);
            if (format != "text") {
                copyUnsupported(plan, "FORMAT " + value.text);
                return plan;
            }
        } else if (option == "delimiter") {
            if (value.text.size() != 1 || value.text[0] == '\0' ||
                value.text[0] == '\n' || value.text[0] == '\r' ||
                value.text[0] == '\\') {
                copyError(plan, "22023",
                          "COPY delimiter must be one non-newline, non-backslash byte");
                return plan;
            }
            plan.delimiter = value.text[0];
        } else if (option == "null") {
            if (value.text.size() > 1024 ||
                value.text.find('\0') != std::string::npos ||
                value.text.find('\r') != std::string::npos ||
                value.text.find('\n') != std::string::npos) {
                copyError(plan, "22023", "invalid COPY null marker");
                return plan;
            }
            plan.nullMarker = value.text;
        } else if (option == "encoding") {
            std::string encoding = lowerAscii(value.text);
            encoding.erase(std::remove_if(
                encoding.begin(), encoding.end(), [](char ch) {
                    return ch == '-' || ch == '_';
                }), encoding.end());
            if (encoding != "utf8" && encoding != "unicode") {
                copyUnsupported(plan, "ENCODING " + value.text);
                return plan;
            }
        } else {
            copyUnsupported(plan, "option " + option);
            return plan;
        }

        token = scanner.next();
        if (token.kind == CopyToken::Kind::RightParen) break;
        if (token.kind != CopyToken::Kind::Comma) {
            copyError(plan, "42601", "malformed COPY option list");
            return plan;
        }
    }
    if (plan.nullMarker.find(plan.delimiter) != std::string::npos) {
        copyError(plan, "22023", "COPY null marker cannot contain the delimiter");
        return plan;
    }
    token = scanner.next();
    if (token.kind == CopyToken::Kind::Semicolon) token = scanner.next();
    if (token.kind != CopyToken::Kind::End) {
        copyError(plan, "42601", "trailing input after COPY options");
    }
    return plan;
}

bool copyTableIsTemporary(const Session& session,
                          const std::string& relation) {
    const size_t dot = relation.rfind('.');
    const std::string name = dot == std::string::npos
                                 ? relation
                                 : relation.substr(dot + 1);
    return session.tempTables.count(name) != 0 ||
           session.transientTempTables.count(name) != 0;
}

bool validateCopyWirePlan(CopyWirePlan& plan, Session& session) {
    if (!plan.error.empty() || !plan.wire) return plan.error.empty();
    try {
        plan.physicalTable = resolveTableName(session, plan.relation);
    } catch (const DbError& error) {
        copyError(plan, error.sqlState(), error.message());
        return false;
    }
    if (!g_engine.tableExists(session.currentDB, plan.physicalTable)) {
        copyError(plan, "42P01",
                  "relation \"" + plan.relation + "\" does not exist");
        return false;
    }
    const TableSchema table =
        g_engine.getTableSchema(session.currentDB, plan.physicalTable);
    if (table.partitionType != TableSchema::PartitionType::None) {
        copyUnsupported(plan, "partitioned relations");
        return false;
    }
    if (plan.direction == CopyWireDirection::FromStdin &&
        g_engine.rlsAppliesTo(session.currentDB, plan.physicalTable)) {
        copyUnsupported(plan, "FROM on row-security-enabled relations");
        return false;
    }

    std::set<size_t> selected;
    if (plan.requestedColumns.empty()) {
        for (size_t index = 0; index < table.len; ++index) {
            if (plan.direction == CopyWireDirection::FromStdin &&
                !table.cols[index].generatedExpr.empty()) {
                continue;
            }
            plan.columnIndexes.push_back(index);
        }
    } else {
        for (const auto& requested : plan.requestedColumns) {
            size_t found = table.len;
            for (size_t index = 0; index < table.len; ++index) {
                if (table.cols[index].dataName == requested) {
                    found = index;
                    break;
                }
            }
            if (found == table.len) {
                copyError(plan, "42703",
                          "column \"" + requested + "\" does not exist");
                return false;
            }
            if (!selected.insert(found).second) {
                copyError(plan, "42701",
                          "column \"" + requested + "\" specified more than once");
                return false;
            }
            if (plan.direction == CopyWireDirection::FromStdin &&
                !table.cols[found].generatedExpr.empty()) {
                copyError(plan, "428C9",
                          "cannot copy into generated column \"" + requested + "\"");
                return false;
            }
            plan.columnIndexes.push_back(found);
        }
    }
    if (plan.columnIndexes.empty()) {
        copyUnsupported(plan, "relations without writable/output columns");
        return false;
    }
    if (plan.columnIndexes.size() >
        static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
        copyError(plan, "54000", "too many columns for COPY protocol response");
        return false;
    }

    std::vector<std::string> columns;
    columns.reserve(plan.columnIndexes.size());
    for (const size_t index : plan.columnIndexes) {
        const Column& column = table.cols[index];
        columns.push_back(column.dataName);
        if (plan.direction == CopyWireDirection::FromStdin &&
            column.identityKind != 0) {
            copyUnsupported(plan, "FROM on identity columns");
            return false;
        }
    }
    const bool temporary = copyTableIsTemporary(session, plan.relation);
    const auto privilege = plan.direction == CopyWireDirection::FromStdin
        ? StorageEngine::TablePrivilege::Insert
        : StorageEngine::TablePrivilege::Select;
    if (!sessionIsAdmin(session) && !temporary &&
        !g_engine.hasColumnPermission(
            session.currentDB, plan.physicalTable,
            effectiveSessionRole(session), privilege, columns)) {
        copyError(plan, "42501",
                  "permission denied for table " + plan.relation);
        return false;
    }
    if (plan.direction == CopyWireDirection::FromStdin &&
        g_engine.inTransaction() && g_engine.isReadOnly() && !temporary) {
        copyError(plan, "25006",
                  "cannot execute COPY FROM in a read-only transaction");
        return false;
    }
    if (plan.direction == CopyWireDirection::FromStdin) {
        std::vector<StorageEngine::Trigger> before;
        std::vector<StorageEngine::Trigger> after;
        if (!g_engine.tryGetTriggers(session.currentDB, plan.physicalTable,
                                     "before", "insert", before) ||
            !g_engine.tryGetTriggers(session.currentDB, plan.physicalTable,
                                     "after", "insert", after)) {
            copyError(plan, "XX001", "could not read COPY target triggers");
            return false;
        }
        const auto unsupportedStatementTrigger = [](const auto& trigger) {
            return trigger.enabled && !trigger.forEachRow;
        };
        if (std::any_of(before.begin(), before.end(),
                        unsupportedStatementTrigger) ||
            std::any_of(after.begin(), after.end(),
                        unsupportedStatementTrigger)) {
            copyUnsupported(plan, "FROM with statement-level triggers");
            return false;
        }
    }
    return true;
}

bool validUtf8(const std::string& value) {
    for (size_t offset = 0; offset < value.size();) {
        const uint8_t lead = static_cast<uint8_t>(value[offset]);
        if (lead < 0x80) {
            if (lead == 0) return false;
            ++offset;
            continue;
        }
        size_t length = 0;
        uint32_t codepoint = 0;
        if ((lead & 0xe0) == 0xc0) {
            length = 2;
            codepoint = lead & 0x1f;
        } else if ((lead & 0xf0) == 0xe0) {
            length = 3;
            codepoint = lead & 0x0f;
        } else if ((lead & 0xf8) == 0xf0) {
            length = 4;
            codepoint = lead & 0x07;
        } else {
            return false;
        }
        if (offset + length > value.size()) return false;
        for (size_t index = 1; index < length; ++index) {
            const uint8_t next = static_cast<uint8_t>(value[offset + index]);
            if ((next & 0xc0) != 0x80) return false;
            codepoint = (codepoint << 6) | (next & 0x3f);
        }
        if ((length == 2 && codepoint < 0x80) ||
            (length == 3 && codepoint < 0x800) ||
            (length == 4 && codepoint < 0x10000) ||
            codepoint > 0x10ffff ||
            (codepoint >= 0xd800 && codepoint <= 0xdfff)) {
            return false;
        }
        offset += length;
    }
    return true;
}

bool decodeCopyTextField(const std::string& raw, std::string& value,
                         std::string& error) {
    value.clear();
    value.reserve(raw.size());
    const auto hexDigit = [](char ch) -> int {
        if (ch >= '0' && ch <= '9') return ch - '0';
        if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
        if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
        return -1;
    };
    for (size_t offset = 0; offset < raw.size(); ++offset) {
        const char ch = raw[offset];
        if (ch != '\\') {
            if (ch == '\0') {
                error = "COPY text data contains a NUL byte";
                return false;
            }
            value.push_back(ch);
            continue;
        }
        if (++offset == raw.size()) {
            error = "unterminated COPY backslash escape";
            return false;
        }
        const char escaped = raw[offset];
        switch (escaped) {
            case 'b': value.push_back('\b'); break;
            case 'f': value.push_back('\f'); break;
            case 'n': value.push_back('\n'); break;
            case 'r': value.push_back('\r'); break;
            case 't': value.push_back('\t'); break;
            case 'v': value.push_back('\v'); break;
            case 'x': {
                int result = 0;
                size_t digits = 0;
                while (digits < 2 && offset + 1 < raw.size()) {
                    const int digit = hexDigit(raw[offset + 1]);
                    if (digit < 0) break;
                    result = result * 16 + digit;
                    ++offset;
                    ++digits;
                }
                if (digits == 0 || result == 0) {
                    error = "invalid COPY hexadecimal escape";
                    return false;
                }
                value.push_back(static_cast<char>(result));
                break;
            }
            default:
                if (escaped >= '0' && escaped <= '7') {
                    int result = escaped - '0';
                    size_t digits = 1;
                    while (digits < 3 && offset + 1 < raw.size() &&
                           raw[offset + 1] >= '0' &&
                           raw[offset + 1] <= '7') {
                        result = result * 8 + (raw[++offset] - '0');
                        ++digits;
                    }
                    if (result == 0 || result > 255) {
                        error = "invalid COPY octal escape";
                        return false;
                    }
                    value.push_back(static_cast<char>(result));
                } else {
                    value.push_back(escaped);
                }
                break;
        }
    }
    if (!validUtf8(value)) {
        error = "invalid byte sequence for encoding UTF8 in COPY data";
        return false;
    }
    return true;
}

bool decodeCopyTextRecord(
        std::string record, const CopyWirePlan& plan,
        std::vector<std::optional<std::string>>& fields,
        std::string& error) {
    if (!record.empty() && record.back() == '\r') record.pop_back();
    std::vector<std::string> rawFields;
    std::string field;
    bool escaped = false;
    for (const char ch : record) {
        if (escaped) {
            field.push_back(ch);
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            field.push_back(ch);
            escaped = true;
            continue;
        }
        if (ch == plan.delimiter) {
            rawFields.push_back(std::move(field));
            field.clear();
            continue;
        }
        if (ch == '\0') {
            error = "COPY text data contains a NUL byte";
            return false;
        }
        field.push_back(ch);
    }
    rawFields.push_back(std::move(field));
    if (rawFields.size() != plan.columnIndexes.size()) {
        error = "COPY row has " + std::to_string(rawFields.size()) +
                " fields but expected " +
                std::to_string(plan.columnIndexes.size());
        return false;
    }
    fields.clear();
    fields.reserve(rawFields.size());
    for (const auto& raw : rawFields) {
        if (raw == plan.nullMarker) {
            fields.push_back(std::nullopt);
            continue;
        }
        std::string decoded;
        if (!decodeCopyTextField(raw, decoded, error)) return false;
        fields.emplace_back(std::move(decoded));
    }
    return true;
}

std::string encodeCopyTextField(const std::string& value, char delimiter) {
    std::string encoded;
    encoded.reserve(value.size());
    const char* octal = "01234567";
    for (const unsigned char byte : value) {
        switch (byte) {
            case '\b': encoded += "\\b"; break;
            case '\f': encoded += "\\f"; break;
            case '\n': encoded += "\\n"; break;
            case '\r': encoded += "\\r"; break;
            case '\t':
                if (delimiter == '\t') encoded += "\\t";
                else encoded.push_back('\t');
                break;
            case '\v': encoded += "\\v"; break;
            case '\\': encoded += "\\\\"; break;
            case 0:
                encoded += "\\000";
                break;
            default:
                if (byte == static_cast<unsigned char>(delimiter)) {
                    encoded.push_back('\\');
                    encoded.push_back(static_cast<char>(byte));
                } else if (byte < 0x20 && byte != '\t') {
                    encoded.push_back('\\');
                    encoded.push_back(octal[(byte >> 6) & 7]);
                    encoded.push_back(octal[(byte >> 3) & 7]);
                    encoded.push_back(octal[byte & 7]);
                } else {
                    encoded.push_back(static_cast<char>(byte));
                }
                break;
        }
    }
    return encoded;
}

struct CopyStreamResult {
    bool transportOk = true;
    bool success = false;
    bool syncConsumed = false;
    bool errorSent = false;
    size_t rows = 0;
    std::string sqlState;
    std::string error;
};

class CopyInterruptGuard {
public:
    explicit CopyInterruptGuard(
        Session& session,
        std::chrono::steady_clock::time_point deadline =
            std::chrono::steady_clock::time_point::max())
        : session_(session), state_(session.interruptState) {
        dbms::setCurrentSession(&session_);
        g_engine.setRLSUser(effectiveSessionRole(session_));
        state_->cancelRequested.store(false, std::memory_order_release);
        state_->timeoutRequested.store(false, std::memory_order_release);
        state_->queryActive.store(true, std::memory_order_release);
        dbms::setCurrentQueryInterruptState(state_);
        g_engine.getLockManager().setInterruptHandler([state = state_]() {
            if (state->terminateRequested.load(std::memory_order_acquire)) {
                throw DbError(
                    "57P01",
                    "terminating connection due to administrator command");
            }
            if (state->cancelRequested.load(std::memory_order_acquire)) {
                if (state->timeoutRequested.load(std::memory_order_acquire)) {
                    throw DbError(
                        "57014",
                        "canceling statement due to statement timeout");
                }
                throw DbError("57014", "canceling COPY due to user request");
            }
        });
        if (deadline != std::chrono::steady_clock::time_point::max()) {
            timeoutThread_ = std::thread([this, deadline]() {
                std::unique_lock<std::mutex> lock(timeoutMutex_);
                const bool completed = timeoutCv_.wait_until(
                    lock, deadline, [this]() { return complete_; });
                if (!completed && state_->queryActive.load(
                                      std::memory_order_acquire)) {
                    state_->timeoutRequested.store(
                        true, std::memory_order_release);
                    state_->cancelRequested.store(
                        true, std::memory_order_release);
                }
            });
        }
    }

    ~CopyInterruptGuard() {
        stopTimer();
        g_engine.getLockManager().clearInterruptHandler();
        dbms::setCurrentQueryInterruptState(nullptr);
        state_->queryActive.store(false, std::memory_order_release);
        state_->cancelRequested.store(false, std::memory_order_release);
        state_->timeoutRequested.store(false, std::memory_order_release);
    }

    void suspendForCleanup() {
        stopTimer();
        state_->queryActive.store(false, std::memory_order_release);
        state_->cancelRequested.store(false, std::memory_order_release);
        state_->timeoutRequested.store(false, std::memory_order_release);
    }

private:
    void stopTimer() {
        {
            std::lock_guard<std::mutex> lock(timeoutMutex_);
            complete_ = true;
        }
        timeoutCv_.notify_all();
        if (timeoutThread_.joinable()) timeoutThread_.join();
    }

    Session& session_;
    std::shared_ptr<SessionInterruptState> state_;
    std::mutex timeoutMutex_;
    std::condition_variable timeoutCv_;
    bool complete_ = false;
    std::thread timeoutThread_;
};

CopyStreamResult receiveCopyIn(PostgresProtocol& protocol,
                               const CopyWirePlan& plan,
                               Session& session,
                               const std::function<bool()>& abortBeforeError,
                               std::chrono::steady_clock::time_point deadline) {
    CopyStreamResult result;
    std::string pending;
    const TableSchema table =
        g_engine.getTableSchema(session.currentDB, plan.physicalTable);
    CopyInterruptGuard interruptGuard(session, deadline);
    const auto checkCopyInterrupt = [&]() {
        if (deadline != std::chrono::steady_clock::time_point::max() &&
            std::chrono::steady_clock::now() >= deadline) {
            throw DbError(
                "57014",
                "canceling statement due to statement timeout");
        }
        dbms::checkForQueryInterrupt();
    };

    const auto fail = [&](std::string state, std::string message) {
        if (!result.error.empty()) return;
        result.sqlState = std::move(state);
        result.error = std::move(message);
        // Interrupt cleanup itself must be allowed to release the failed
        // statement's locks and undo state.
        session.interruptState->timeoutRequested.store(
            false, std::memory_order_release);
        session.interruptState->cancelRequested.store(
            false, std::memory_order_release);
        interruptGuard.suspendForCleanup();
        // The frontend may stop sending CopyData as soon as it sees this
        // error. Release aborted transaction resources before responding,
        // not after waiting for CopyDone or Sync from that frontend.
        if (!abortBeforeError()) {
            protocol.sendNoticeResponse(
                "COPY transaction cleanup could not be completed",
                "WARNING", "XX000");
        }
        result.errorSent = protocol.sendErrorResponse(
            "ERROR", result.sqlState, result.error);
        if (!result.errorSent) result.transportOk = false;
        pending.clear();
    };
    const auto insertRecord = [&](std::string record) {
        if (!result.error.empty()) return;
        try {
            checkCopyInterrupt();
            std::vector<std::optional<std::string>> fields;
            std::string decodeError;
            if (!decodeCopyTextRecord(
                    std::move(record), plan, fields, decodeError)) {
                fail("22P04", decodeError);
                return;
            }
            StorageEngine::SqlRow values;
            for (size_t field = 0; field < fields.size(); ++field) {
                const Column& column = table.cols[plan.columnIndexes[field]];
                auto value = std::move(fields[field]);
                const uint32_t typeOid = mapBuiltinTypeNameToOid(column.dataType);
                if (value && !column.isArray &&
                    (typeOid == 21 || typeOid == 23 || typeOid == 20)) {
                    const unsigned bits = typeOid == 21 ? 16 : typeOid == 23 ? 32 : 64;
                    const auto parsed = parsePostgresIntegerText(*value, bits);
                    if (parsed.error == IntegerTextError::InvalidSyntax) {
                        throw DbError("22P02", "invalid input syntax for type " +
                            column.dataType + ": \"" + *value + "\"");
                    }
                    if (parsed.error == IntegerTextError::OutOfRange) {
                        throw DbError("22003", "value out of range for type " +
                            column.dataType + ": \"" + *value + "\"");
                    }
                    *value = std::to_string(parsed.value);
                }
                values[column.dataName] = std::move(value);
            }
            const DBStatus status = g_engine.insertRow(
                session.currentDB, plan.physicalTable, values);
            if (status != DBStatus::OK) {
                fail(sqlstateForDBStatus(status),
                     "COPY FROM failed while inserting row " +
                         std::to_string(result.rows + 1));
                return;
            }
            ++result.rows;
        } catch (const DbError& error) {
            fail(error.sqlState(), error.message());
        } catch (const std::exception& error) {
            fail("XX000", error.what());
        }
    };

    while (result.transportOk) {
        PgFrontendMessage message;
        std::string protocolError;
        bool partialMessage = false;
        const auto interrupt = session.interruptState;
        const ProtocolMessageReadResult readResult =
            protocol.readMessageUntil(
                message, protocolError, deadline,
                [interrupt]() {
                    return interrupt->terminateRequested.load(
                               std::memory_order_acquire) ||
                           interrupt->cancelRequested.load(
                               std::memory_order_acquire);
                },
                partialMessage);
        if (readResult != ProtocolMessageReadResult::Complete) {
            if (readResult == ProtocolMessageReadResult::TimedOut ||
                readResult == ProtocolMessageReadResult::Interrupted) {
                const bool terminated = interrupt->terminateRequested.load(
                    std::memory_order_acquire);
                const bool timedOut =
                    readResult == ProtocolMessageReadResult::TimedOut ||
                    interrupt->timeoutRequested.load(
                        std::memory_order_acquire) ||
                    (deadline != std::chrono::steady_clock::time_point::max() &&
                     std::chrono::steady_clock::now() >= deadline);
                fail(terminated ? "57P01" : "57014",
                     terminated
                         ? "terminating connection due to administrator command"
                         : timedOut
                             ? "canceling statement due to statement timeout"
                             : "canceling COPY due to user request");
                // Consumed a prefix of the frontend frame; after reporting the
                // timeout/cancel, close instead of misreading its remainder
                // as a new PostgreSQL message.
                if (partialMessage) result.transportOk = false;
                return result;
            }
            result.transportOk = false;
            result.error = protocolError;
            return result;
        }
        if (message.type == 'd') {
            if (!result.error.empty()) continue;
            for (const uint8_t byte : message.payload) {
                if (!result.error.empty()) break;
                if (byte == '\n') {
                    insertRecord(std::move(pending));
                    pending.clear();
                    continue;
                }
                if (pending.size() == kMaxCopyRecordBytes) {
                    fail("54000", "COPY input row exceeds 16 MiB limit");
                    break;
                }
                pending.push_back(static_cast<char>(byte));
            }
            continue;
        }
        if (message.type == 'c') {
            if (!message.payload.empty()) {
                fail("08P01", "malformed CopyDone message");
            } else if (result.error.empty() && !pending.empty()) {
                insertRecord(std::move(pending));
            }
            result.success = result.error.empty();
            return result;
        }
        if (message.type == 'f') {
            size_t offset = 0;
            std::string reason;
            if (!PostgresProtocol::readCString(
                    message.payload, offset, reason) ||
                offset != message.payload.size()) {
                fail("08P01", "malformed CopyFail message");
            } else {
                fail("57014", "COPY from stdin failed: " + reason);
            }
            return result;
        }
        if (message.type == 'S') {
            if (!message.payload.empty()) {
                fail("08P01", "malformed Sync message during COPY");
            } else if (result.error.empty()) {
                fail("08P01", "Sync received before CopyDone or CopyFail");
            }
            result.syncConsumed = true;
            return result;
        }
        if (message.type == 'H' && message.payload.empty()) continue;
        if (message.type == 'X') {
            result.transportOk = false;
            result.error = "client terminated connection during COPY";
            return result;
        }
        fail("08P01", "unexpected frontend message during COPY FROM");
    }
    return result;
}

CopyStreamResult sendCopyOut(PostgresProtocol& protocol,
                             const CopyWirePlan& plan,
                             Session& session,
                             std::chrono::steady_clock::time_point deadline) {
    CopyStreamResult result;
    const TableSchema table =
        g_engine.getTableSchema(session.currentDB, plan.physicalTable);
    CopyInterruptGuard interruptGuard(session, deadline);
    const auto checkCopyInterrupt = [&]() {
        if (deadline != std::chrono::steady_clock::time_point::max() &&
            std::chrono::steady_clock::now() >= deadline) {
            throw DbError(
                "57014",
                "canceling statement due to statement timeout");
        }
        dbms::checkForQueryInterrupt();
    };
    try {
        const bool scanned = g_engine.forEachVisibleRow(
            session.currentDB, plan.physicalTable, "SELECT",
            [&](uint32_t pageId, uint16_t slotId, const char* data,
                size_t length) {
                if (!result.transportOk || !result.error.empty()) return;
                checkCopyInterrupt();
                const int64_t rid = StorageEngine::encodeRid(pageId, slotId);
                StorageEngine::bindNullRow(
                    &g_engine, session.currentDB, plan.physicalTable, rid,
                    table.len);
                struct NullBindingGuard {
                    ~NullBindingGuard() { StorageEngine::unbindNullRow(); }
                } nullBindingGuard;
                const std::string row(data, length);
                std::string output;
                for (size_t selected = 0;
                     selected < plan.columnIndexes.size(); ++selected) {
                    if (selected != 0) output.push_back(plan.delimiter);
                    const size_t column = plan.columnIndexes[selected];
                    bool isNull = false;
                    const std::string value = g_engine.extractColumnValue(
                        row, table, column, session.currentDB, true, &isNull);
                    output += isNull
                        ? plan.nullMarker
                        : encodeCopyTextField(value, plan.delimiter);
                }
                output.push_back('\n');
                if (output.size() > kMaxCopyRecordBytes) {
                    result.sqlState = "54000";
                    result.error = "COPY output row exceeds 16 MiB limit";
                    return;
                }
                const auto interrupt = session.interruptState;
                const ProtocolMessageWriteResult writeResult =
                    protocol.sendCopyDataUntil(
                        output, deadline, [interrupt]() {
                            return interrupt->terminateRequested.load(
                                       std::memory_order_acquire) ||
                                   interrupt->cancelRequested.load(
                                       std::memory_order_acquire);
                        });
                if (writeResult != ProtocolMessageWriteResult::Complete) {
                    if (writeResult == ProtocolMessageWriteResult::TimedOut ||
                        interrupt->timeoutRequested.load(
                            std::memory_order_acquire)) {
                        result.sqlState = "57014";
                        result.error =
                            "canceling statement due to statement timeout";
                    } else if (writeResult ==
                               ProtocolMessageWriteResult::Interrupted) {
                        const bool terminated =
                            interrupt->terminateRequested.load(
                                std::memory_order_acquire);
                        result.sqlState = terminated ? "57P01" : "57014";
                        result.error = terminated
                            ? "terminating connection due to administrator command"
                            : "canceling COPY due to user request";
                    }
                    result.transportOk = false;
                    return;
                }
                ++result.rows;
            });
        if (!scanned && result.error.empty() && result.transportOk) {
            result.sqlState = "XX001";
            result.error = "COPY TO could not read the relation";
        }
        if (result.error.empty() && result.transportOk)
            checkCopyInterrupt();
    } catch (const DbError& error) {
        result.sqlState = error.sqlState();
        result.error = error.message();
    } catch (const std::exception& error) {
        result.sqlState = "XX000";
        result.error = error.what();
    }
    if (result.transportOk && result.error.empty()) {
        const auto interrupt = session.interruptState;
        const ProtocolMessageWriteResult writeResult =
            protocol.sendCopyDoneUntil(deadline, [interrupt]() {
                return interrupt->terminateRequested.load(
                           std::memory_order_acquire) ||
                       interrupt->cancelRequested.load(
                           std::memory_order_acquire);
            });
        result.transportOk =
            writeResult == ProtocolMessageWriteResult::Complete;
        result.success = result.transportOk;
        if (!result.transportOk &&
            (writeResult == ProtocolMessageWriteResult::TimedOut ||
             interrupt->timeoutRequested.load(std::memory_order_acquire))) {
            result.sqlState = "57014";
            result.error = "canceling statement due to statement timeout";
        }
    }
    return result;
}

} // namespace

QueryResult executeProtocolQuery(const std::string& sql, Session& session,
                                 int statementTimeoutOverrideMs = -1) {
    QueryResult result;
    dbms::clearLastDmlResult();
    // Stored-NULL row bindings must not leak across statements (a sort
    // rebind from the previous statement would poison extraction here).
    dbms::StorageEngine::unbindNullRow();
    std::string trimmed = trimText(sql);
    if (trimmed.empty()) {
        result.commandTag.clear();
        return result;
    }
    if (statementTimeoutOverrideMs == 0) {
        result.error = true;
        result.sqlState = "57014";
        result.errorMessage =
            "canceling statement due to statement timeout";
        return result;
    }

    bool executionError = false;
    bool structuredError = false;
    bool statementCommitError = false;
    std::string outputText;
    auto start = std::chrono::steady_clock::now();
    struct QueryInterruptGuard {
        std::shared_ptr<SessionInterruptState> state;
        dbms::LockManager& lockManager;
        std::mutex timeoutMutex;
        std::condition_variable timeoutCv;
        bool queryComplete = false;
        std::thread timeoutThread;
        explicit QueryInterruptGuard(
            std::shared_ptr<SessionInterruptState> interruptState,
            dbms::LockManager& manager, int statementTimeoutMs)
            : state(std::move(interruptState)), lockManager(manager) {
            state->cancelRequested.store(false, std::memory_order_release);
            state->timeoutRequested.store(false, std::memory_order_release);
            state->queryActive.store(true, std::memory_order_release);
            dbms::setCurrentQueryInterruptState(state);
            lockManager.setInterruptHandler([interruptState = state]() {
                if (interruptState->terminateRequested.load(
                        std::memory_order_acquire)) {
                    throw dbms::DbError(
                        "57P01",
                        "terminating connection due to administrator command");
                }
                if (interruptState->cancelRequested.load(
                        std::memory_order_acquire)) {
                    if (interruptState->timeoutRequested.load(
                            std::memory_order_acquire)) {
                        throw dbms::DbError(
                            "57014",
                            "canceling statement due to statement timeout");
                    }
                    throw dbms::DbError(
                        "57014", "canceling statement due to user request");
                }
            });
            if (statementTimeoutMs > 0) {
                timeoutThread = std::thread([this, statementTimeoutMs]() {
                    std::unique_lock<std::mutex> lock(timeoutMutex);
                    const bool completed = timeoutCv.wait_for(
                        lock, std::chrono::milliseconds(statementTimeoutMs),
                        [this]() { return queryComplete; });
                    if (!completed && state->queryActive.load(
                                          std::memory_order_acquire)) {
                        state->timeoutRequested.store(
                            true, std::memory_order_release);
                        state->cancelRequested.store(
                            true, std::memory_order_release);
                    }
                });
            }
        }
        ~QueryInterruptGuard() {
            {
                std::lock_guard<std::mutex> lock(timeoutMutex);
                queryComplete = true;
            }
            timeoutCv.notify_all();
            if (timeoutThread.joinable()) timeoutThread.join();
            lockManager.clearInterruptHandler();
            dbms::setCurrentQueryInterruptState(nullptr);
            state->queryActive.store(false, std::memory_order_release);
            state->cancelRequested.store(false, std::memory_order_release);
            state->timeoutRequested.store(false, std::memory_order_release);
        }
    } interruptGuard(session.interruptState, g_engine.getLockManager(),
                     statementTimeoutOverrideMs >= 0
                         ? statementTimeoutOverrideMs
                         : session.statementTimeoutMs);
    {
        std::ostringstream output;
        dbms::ScopedOutputCapture capture(output);
        try {
            // Catalog/type analysis can itself throw a structured error.
            // Keep it inside the same protocol exception boundary as execution
            // so a metadata failure never terminates the client connection.
            // A direct INSERT SELECT has a whole-statement metadata consumer
            // in DmlExecutor. Its projection input conversions can precede
            // WHERE name analysis; scanning WHERE in isolation here changes
            // the first error. Identify this grammar by strict AST only, and
            // leave raw WITH envelopes and all other statements unchanged.
            bool typedInsertSelect = false;
            if (SQLParser::classify(sql) == SqlCommand::Insert) {
                SQLParser parser;
                const auto parsed = parser.parseForBinding(sql);
                const auto* insert = parsed.success
                    ? dynamic_cast<const InsertStmt*>(parsed.stmt.get()) : nullptr;
                typedInsertSelect = insert && insert->selectSource;
            }
            if (!typedInsertSelect) {
                const auto functionError = whereUnknownFunctionError(sql, session);
                if (!functionError.empty()) throw DbError("42883", functionError);
            }
            executionError = execute(sql, session);
        } catch (const dbms::StatementCommitError& e) {
            executionError = true;
            structuredError = true;
            statementCommitError = true;
            result.sqlState = e.sqlState();
            result.errorMessage = e.message();
        } catch (const dbms::DbError& e) {
            executionError = true;
            structuredError = true;
            result.sqlState = e.sqlState();
            result.errorMessage = e.message();
        } catch (const std::exception& e) {
            executionError = true;
            result.errorMessage = e.what();
        } catch (...) {
            executionError = true;
            result.errorMessage = "unhandled executor exception";
        }
        outputText = output.str();
    }
    const dbms::DmlResult structuredDml = dbms::takeLastDmlResult();
    auto end = std::chrono::steady_clock::now();
    double elapsedMs = std::chrono::duration<double, std::milli>(end - start).count();
    if (elapsedMs > g_slowQueryThresholdMs) {
        logSlowQuery(sql, elapsedMs, session.username, session.currentDB);
    }
    dbms::recordSqlStat(sql, elapsedMs, session.currentDB);

    auto lines = outputLines(outputText);
    // The legacy executor reports successful diagnostics through stdout.
    // They are asynchronous protocol messages, not row data or command-tag
    // text.  Remove them before interpreting the remaining command output.
    lines.erase(std::remove_if(lines.begin(), lines.end(), [&](const std::string& line) {
        static const std::pair<const char*, const char*> prefixes[] = {
            {"NOTICE:", "NOTICE"}, {"WARNING:", "WARNING"}
        };
        for (const auto& entry : prefixes) {
            const size_t prefixLength = std::strlen(entry.first);
            if (line.compare(0, prefixLength, entry.first) != 0) continue;
            result.notices.push_back(QueryNotice{
                entry.second,
                std::string(entry.second) == "WARNING" ? "01000" : "00000",
                trimText(line.substr(prefixLength))
            });
            return true;
        }
        return false;
    }), lines.end());
    if (executionError || (!lines.empty() && lines.front().rfind("ERROR:", 0) == 0)) {
        result.error = true;
        if (result.errorMessage.empty()) {
            if (lines.empty()) {
                result.errorMessage = "query failed";
            } else {
                // A command can print its success tag before a pre-commit
                // hook rejects the transaction (for example a full NOTIFY
                // queue).  Prefer the explicit error line over earlier
                // informational output instead of exposing the SQL/tag as
                // an XX000 error message.
                const auto errorLine = std::find_if(
                    lines.begin(), lines.end(), [](const std::string& line) {
                        return line.rfind("ERROR:", 0) == 0 ||
                               line.rfind("SQL syntax error:", 0) == 0 ||
                               line.rfind("SQL error:", 0) == 0;
                    });
                std::string msg = errorLine == lines.end()
                    ? lines.front() : *errorLine;
                // Strip the CLI's severity prefix so the wire carries the bare
                // message like PG: "ERROR: x" -> "x", "SQL syntax error: y"
                // -> "y".  substr(6) used to mangle the latter to "ntax error".
                static const char* prefixes[] = { "ERROR:", "SQL syntax error:", "SQL error:" };
                bool stripped = false;
                for (const char* pre : prefixes) {
                    size_t n = std::strlen(pre);
                    if (msg.compare(0, n, pre) == 0) {
                        msg = trimText(msg.substr(n));
                        stripped = true;
                        break;
                    }
                }
                if (!stripped) msg = trimText(msg);
                // PG wording for missing relations: the CLI reports
                // "Table X not exist"; the wire rewrites it to PG's
                // relation "x" does not exist so clients see the same
                // message shape as reference PG.
                if (msg.rfind("Table ", 0) == 0 &&
                    msg.find(" not exist") != std::string::npos) {
                    std::string rel = msg.substr(6, msg.size() - 6 - 10);
                    for (auto& ch : rel)
                        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                    msg = "relation \"" + rel + "\" does not exist";
                }
                result.errorMessage = msg;
            }
        }
        std::string embeddedSqlState;
        const size_t stateMarker = result.errorMessage.find("(SQLSTATE ");
        if (stateMarker != std::string::npos &&
            stateMarker + 15 < result.errorMessage.size()) {
            const std::string candidate =
                result.errorMessage.substr(stateMarker + 10, 5);
            const bool valid = std::all_of(
                candidate.begin(), candidate.end(), [](unsigned char c) {
                    return std::isdigit(c) || (c >= 'A' && c <= 'Z');
                });
            if (valid && result.errorMessage[stateMarker + 15] == ')') {
                embeddedSqlState = candidate;
            }
        }
        if (structuredError) {
            // The executor's explicit code takes precedence over all legacy
            // wording heuristics, even if the message happens to match one.
        } else if (!embeddedSqlState.empty()) {
            // Legacy handlers that have not yet migrated to DbError still
            // declare an exact SQLSTATE in their diagnostic. Honor any valid
            // five-character code instead of maintaining an incomplete list.
            result.sqlState = embeddedSqlState;
        } else if (result.errorMessage.find(
                       "aggregate functions are not allowed in GROUP BY") !=
                   std::string::npos) {
            // 42803: grouping column references an aggregate.
            result.sqlState = "42803";
        } else if (result.errorMessage.find("does not exist") !=
                           std::string::npos &&
                       (result.errorMessage.rfind("relation ", 0) == 0 ||
                        result.errorMessage.rfind("table ", 0) == 0)) {
            // 42P01: undefined table/relation reference (SELECT uses
            // "relation", DROP TABLE uses "table").
            result.sqlState = "42P01";
        } else if (result.errorMessage.find("operator is not unique") !=
                       std::string::npos ||
                   result.errorMessage.find("(SQLSTATE 42725)") !=
                       std::string::npos) {
            // 42725: ambiguous operator or function resolution.
            result.sqlState = "42725";
        } else if (result.errorMessage.find("invalid input syntax") != std::string::npos) {
            // 22P02: strict type-coercion failures (checked before the generic
            // "syntax" branch below, which would otherwise also match).
            result.sqlState = "22P02";
        } else if (result.errorMessage.find("syntax") != std::string::npos ||
                   result.errorMessage.find("(SQLSTATE 42601)") !=
                       std::string::npos) {
            result.sqlState = "42601";
        } else if (result.errorMessage.find(
                       "more than one row returned by a subquery used as an expression")
                   != std::string::npos) {
            result.sqlState = "21000";
        } else if (result.errorMessage.find("feature not supported") !=
                       std::string::npos ||
                   result.errorMessage.find("(SQLSTATE 0A000)") !=
                       std::string::npos) {
            // DIV-14: capability gate refusals carry their own SQLSTATE.
            result.sqlState = "0A000";
        } else if (result.errorMessage.find(
                       "unrecognized configuration parameter") !=
                       std::string::npos ||
                   result.errorMessage.find("(SQLSTATE 42704)") !=
                       std::string::npos) {
            // DIV-06/DIV-12: unknown type or parameter errors.
            result.sqlState = "42704";
        } else if (result.errorMessage.find("division by zero") !=
                       std::string::npos ||
                   result.errorMessage.find("(SQLSTATE 22012)") !=
                       std::string::npos) {
            result.sqlState = "22012";
        } else if (result.errorMessage.find("invalid input syntax") !=
                       std::string::npos ||
                   result.errorMessage.find("(SQLSTATE 22P02)") !=
                       std::string::npos) {
            result.sqlState = "22P02";
        } else if (result.errorMessage.find("(SQLSTATE 42883)") !=
                       std::string::npos ||
                   (result.errorMessage.find("function ") !=
                        std::string::npos &&
                    result.errorMessage.find(" does not exist") !=
                        std::string::npos)) {
            // Undefined function (PG 42883).
            result.sqlState = "42883";
        } else if (result.errorMessage.find("(SQLSTATE 25001)") !=
                   std::string::npos) {
            result.sqlState = "25001";
        } else if (result.errorMessage.find("(SQLSTATE 25006)") !=
                   std::string::npos) {
            result.sqlState = "25006";
        } else if (result.errorMessage.find("(SQLSTATE 25P01)") !=
                   std::string::npos) {
            result.sqlState = "25P01";
        } else if (result.errorMessage.find("(SQLSTATE 22023)") !=
                   std::string::npos) {
            result.sqlState = "22023";
        } else if (result.errorMessage.find("(SQLSTATE 22P04)") !=
                   std::string::npos) {
            result.sqlState = "22P04";
        } else if (result.errorMessage.find("(SQLSTATE 23505)") !=
                   std::string::npos) {
            result.sqlState = "23505";
        } else if (result.errorMessage.find("(SQLSTATE 23514)") !=
                   std::string::npos) {
            result.sqlState = "23514";
        } else if (result.errorMessage.find("(SQLSTATE 23503)") !=
                   std::string::npos) {
            result.sqlState = "23503";
        } else if (result.errorMessage.find("(SQLSTATE 23P01)") !=
                   std::string::npos) {
            result.sqlState = "23P01";
        } else if (result.errorMessage.find("(SQLSTATE 54000)") !=
                   std::string::npos) {
            result.sqlState = "54000";
        } else {
            result.sqlState = "XX000";
        }
        if(structuredError && structuredDml.available && structuredDml.metadataOnly &&
            structuredDml.runtimeErrorMetadata && !structuredDml.columns.empty()) {
            result.resultSet=true;
            result.columns=structuredDml.columns;result.columnTypes=structuredDml.columnTypes;
            result.rows.clear();result.nulls.clear();result.commandTag.clear();
            result.columnDescriptions=describeProtocolColumns(result,sql,session);
        }
        if (statementCommitError && structuredDml.available &&
            !structuredDml.metadataOnly) {
            // PostgreSQL already delivered RETURNING before the implicit
            // commit recheck failed. Keep typed cells/NULLs, suppress the
            // success tag, and emit the error after these rows.
            result.resultSet = true;
            result.columns = structuredDml.columns;
            result.columnTypes = structuredDml.columnTypes;
            result.rows = structuredDml.rows;
            result.nulls = structuredDml.nulls;
            result.columnDescriptions = describeProtocolColumns(result, sql, session);
            result.commandTag.clear();
        }
        dbms::recordQueryExecution(sql, elapsedMs, session.currentDB, false,
                                  result.rows.size());
        return result;
    }

    if (structuredDml.available && !structuredDml.metadataOnly) {
        result.resultSet = true;
        result.columns = structuredDml.columns;
        result.columnTypes = structuredDml.columnTypes;
        result.rows = structuredDml.rows;
        result.nulls = structuredDml.nulls;
        result.columnDescriptions = describeProtocolColumns(result, sql, session);
        result.commandTag = structuredDml.commandTag;
        dbms::recordQueryExecution(sql, elapsedMs, session.currentDB, true,
                                   result.rows.size());
        return result;
    }

    const std::string keyword = firstSqlKeyword(sql);
    result.resultSet = keyword == "select" || keyword == "show" || keyword == "values" ||
                       keyword == "with" || keyword == "explain";
    if (result.resultSet && !lines.empty()) {
        if (keyword == "explain") {
            result.columns = {"QUERY PLAN"};
            for (const auto& line : lines) result.rows.push_back({line});
        } else {
            result.columns = splitProtocolFields(lines.front());
            if (structuredDml.available && structuredDml.metadataOnly) {
                if (!structuredDml.columns.empty())
                    result.columns = structuredDml.columns;
                result.columnTypes = structuredDml.columnTypes;
            }
            for (size_t i = 1; i < lines.size(); ++i) {
                // The legacy executor prints one column as the complete line.
                // Splitting it on whitespace corrupts timestamp/time-like
                // values that legitimately contain spaces.
                if (result.columns.size() == 1) {
                    // An all-NULL single-cell row survives as a single
                    // space; the empty string is the NULL representation.
                    std::string cell = lines[i];
                    if (cell == " ") cell.clear();
                    result.rows.push_back({cell});
                }
                else {
                    // The CLI renders cells as "v1 v2 ... " with single
                    // spaces; an empty cell yields a doubled space (or a
                    // leading space when the first cell is NULL).  Split on
                    // single spaces and drop only the trailing artifact so
                    // empty cells survive as empty fields.
                    std::vector<std::string> fields;
                    {
                        // Quote-aware single-space split: a double-quoted
                        // segment (multi-word data cell like "BASE TABLE")
                        // stays one field; empty cells (doubled spaces)
                        // survive as empty fields.
                        const std::string& raw = lines[i];
                        bool inDq = false;
                        std::string field;
                        for (size_t ci = 0; ci < raw.size(); ++ci) {
                            const char c = raw[ci];
                            if (c == 34) { inDq = !inDq; continue; }
                            if (!inDq && c == ' ') {
                                fields.push_back(field);
                                field.clear();
                                continue;
                            }
                            field += c;
                        }
                        fields.push_back(field);
                    }
                    if (!fields.empty() && fields.back().empty() &&
                        fields.size() > result.columns.size())
                        fields.pop_back();
                    fields.resize(result.columns.size());
                    result.rows.push_back(std::move(fields));
                }
            }
        }
        result.columnDescriptions = describeProtocolColumns(result, sql, session);
    }
    result.commandTag =
        structuredDml.available && structuredDml.metadataOnly &&
                !structuredDml.commandTag.empty()
            ? structuredDml.commandTag
            : commandTagFor(sql, lines, result.rows.size());
    dbms::recordQueryExecution(sql, elapsedMs, session.currentDB, true,
                               result.rows.size());
    return result;
}

bool isTransactionRecoveryCommand(const std::string& sql) {
    const std::string keyword = firstSqlKeyword(sql);
    // PREPARE TRANSACTION completion is a separate two-phase command and is
    // not a local recovery operation. In particular, do not rewrite
    // COMMIT/ROLLBACK PREPARED as an ordinary COMMIT/ROLLBACK while a backend
    // is in the failed-transaction state.
    if (startsWithSqlPhrase(sql, "commit prepared") ||
        startsWithSqlPhrase(sql, "rollback prepared")) {
        return false;
    }
    return keyword == "commit" || keyword == "end" || keyword == "rollback" ||
           keyword == "abort";
}

// PostgreSQL treats COMMIT in an aborted transaction as a rollback.  Preserve
// the chain option while routing it through the normal rollback executor so
// session-local objects, undo records, WAL and locks are all cleaned up by one
// transaction boundary.
std::string rollbackCommandForAbortedTransaction(const std::string& sql) {
    const size_t start = sqlCommandOffset(sql);
    const std::string statementSql = start == std::string::npos ? sql : sql.substr(start);
    SQLParser parser;
    auto parsed = parser.parse(statementSql);
    const auto* transaction = parsed.success
        ? dynamic_cast<const TransactionStmt*>(parsed.stmt.get()) : nullptr;
    // Parse the complete command before rewriting it. Keyword-prefix
    // matching must not erase an invalid suffix or accept COMMIT PREPARED
    // as an ordinary ending while the connection is failed.
    if (!transaction || (transaction->kind != TransactionStmt::Kind::Commit &&
                         transaction->kind != TransactionStmt::Kind::End)) {
        return statementSql;
    }
    if (!transaction->chainSpecified) return "ROLLBACK";
    return transaction->chain ? "ROLLBACK AND CHAIN" : "ROLLBACK AND NO CHAIN";
}

QueryResult transactionAbortedResult() {
    QueryResult result;
    result.error = true;
    result.sqlState = "25P02";
    result.errorMessage = "current transaction is aborted, commands ignored until end of transaction block";
    return result;
}

std::string messageCString(const PgFrontendMessage& message, size_t offset = 0) {
    std::string value;
    if (!PostgresProtocol::readCString(message.payload, offset, value)) return {};
    return value;
}

bool parseScramAttributes(const std::string& message,
                          std::map<char, std::string>& attributes) {
    attributes.clear();
    size_t begin = 0;
    while (begin <= message.size()) {
        const size_t end = message.find(',', begin);
        const std::string part = message.substr(begin, end == std::string::npos
                                                          ? std::string::npos
                                                          : end - begin);
        if (part.size() < 3 || part[1] != '=' || part[0] < 'a' || part[0] > 'z' ||
            attributes.count(part[0]) != 0) {
            return false;
        }
        attributes.emplace(part[0], part.substr(2));
        if (end == std::string::npos) break;
        begin = end + 1;
    }
    return true;
}

std::string unescapeScramName(const std::string& value) {
    std::string result;
    result.reserve(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        if (value[i] != '=') {
            result.push_back(value[i]);
            continue;
        }
        if (i + 2 >= value.size()) return {};
        const std::string escape = value.substr(i, 3);
        if (escape == "=2C") result.push_back(',');
        else if (escape == "=3D") result.push_back('=');
        else return {};
        i += 2;
    }
    return result;
}

bool readSaslInitialResponse(const PgFrontendMessage& message,
                             std::string& mechanism, std::string& initialResponse) {
    if (message.type != 'p') return false;
    size_t offset = 0;
    if (!PostgresProtocol::readCString(message.payload, offset, mechanism) ||
        offset + 4 > message.payload.size()) {
        return false;
    }
    const int32_t responseLength = PostgresProtocol::readInt32(message.payload, offset);
    offset += 4;
    if (responseLength < -1 ||
        (responseLength >= 0 &&
         static_cast<size_t>(responseLength) != message.payload.size() - offset)) {
        return false;
    }
    if (responseLength == -1) {
        initialResponse.clear();
    } else {
        initialResponse.assign(reinterpret_cast<const char*>(message.payload.data() + offset),
                               static_cast<size_t>(responseLength));
    }
    return true;
}

std::string randomScramNonceSuffix() {
    std::random_device random;
    return std::to_string(random()) + std::to_string(random());
}

bool authenticateScram(PostgresProtocol& protocol, const std::string& username,
                       const std::string& storedPassword, std::string& error) {
    dbms::scram::Verifier verifier;
    if (!dbms::scram::parseVerifier(storedPassword, verifier)) {
        error = "invalid SCRAM verifier";
        protocol.sendErrorResponse("FATAL", "XX000", error);
        return false;
    }
    if (!protocol.sendAuthenticationSasl({"SCRAM-SHA-256"})) return false;

    PgFrontendMessage initialMessage;
    if (!protocol.readMessage(initialMessage, error)) return false;
    std::string mechanism;
    std::string initialResponse;
    if (!readSaslInitialResponse(initialMessage, mechanism, initialResponse) ||
        mechanism != "SCRAM-SHA-256" || initialResponse.rfind("n,,", 0) != 0) {
        error = "malformed SCRAM client-first message";
        protocol.sendErrorResponse("FATAL", "08P01", error);
        return false;
    }

    const std::string clientFirstBare = initialResponse.substr(3);
    std::map<char, std::string> firstAttributes;
    if (!parseScramAttributes(clientFirstBare, firstAttributes) ||
        firstAttributes.count('n') == 0 || firstAttributes.count('r') == 0 ||
        firstAttributes['r'].empty()) {
        error = "malformed SCRAM client-first attributes";
        protocol.sendErrorResponse("FATAL", "08P01", error);
        return false;
    }
    if (unescapeScramName(firstAttributes['n']) != username) {
        error = "SCRAM username does not match startup user";
        protocol.sendErrorResponse("FATAL", "28P01", "password authentication failed");
        return false;
    }
    const std::string clientNonce = firstAttributes['r'];
    if (clientNonce.find(',') != std::string::npos || clientNonce.find('=') != std::string::npos) {
        error = "invalid SCRAM client nonce";
        protocol.sendErrorResponse("FATAL", "08P01", error);
        return false;
    }
    const std::string serverNonce = clientNonce + randomScramNonceSuffix();
    const std::string serverFirst = "r=" + serverNonce + ",s=" +
                                    dbms::scram::base64Encode(verifier.salt) + ",i=" +
                                    std::to_string(verifier.iterations);
    if (!protocol.sendAuthenticationSaslContinue(serverFirst)) return false;

    PgFrontendMessage finalMessage;
    if (!protocol.readMessage(finalMessage, error) || finalMessage.type != 'p') {
        error = "expected SCRAM client-final message";
        protocol.sendErrorResponse("FATAL", "08P01", error);
        return false;
    }
    const std::string clientFinal(reinterpret_cast<const char*>(finalMessage.payload.data()),
                                  finalMessage.payload.size());
    std::map<char, std::string> finalAttributes;
    if (!parseScramAttributes(clientFinal, finalAttributes) ||
        finalAttributes.count('c') == 0 || finalAttributes.count('r') == 0 ||
        finalAttributes.count('p') == 0 || finalAttributes['c'] != "biws" ||
        finalAttributes['r'] != serverNonce || finalAttributes['p'].empty()) {
        error = "malformed SCRAM client-final message";
        protocol.sendErrorResponse("FATAL", "08P01", error);
        return false;
    }
    const size_t proofOffset = clientFinal.rfind(",p=");
    if (proofOffset == std::string::npos) {
        error = "malformed SCRAM proof";
        protocol.sendErrorResponse("FATAL", "08P01", error);
        return false;
    }
    const std::string clientFinalWithoutProof = clientFinal.substr(0, proofOffset);
    const std::string authMessage = clientFirstBare + "," + serverFirst + "," +
                                    clientFinalWithoutProof;
    std::string serverSignature;
    if (!dbms::scram::verifyClientProof(verifier, authMessage, finalAttributes['p'],
                                        serverSignature)) {
        error = "password authentication failed";
        protocol.sendErrorResponse("FATAL", "28P01", error);
        return false;
    }
    return protocol.sendAuthenticationSaslFinal("v=" + serverSignature) &&
           protocol.sendAuthenticationOk();
}

bool readRawExact(int fd, void* destination, size_t length) {
    auto* bytes = static_cast<uint8_t*>(destination);
    size_t received = 0;
    while (received < length) {
        ssize_t n = ::recv(fd, bytes + received, length - received, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        received += static_cast<size_t>(n);
    }
    return true;
}

bool writeRawAll(int fd, const void* data, size_t length) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    size_t written = 0;
    while (written < length) {
        ssize_t n = ::send(fd, bytes + written, length - written, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        written += static_cast<size_t>(n);
    }
    return true;
}

uint32_t rawUInt32(const uint8_t* bytes) {
    return (static_cast<uint32_t>(bytes[0]) << 24) |
           (static_cast<uint32_t>(bytes[1]) << 16) |
           (static_cast<uint32_t>(bytes[2]) << 8) |
           static_cast<uint32_t>(bytes[3]);
}

// PostgreSQL clients send SSLRequest before the StartupMessage. The server
// must answer on the raw socket before wrapping that same descriptor in TLS.
bool establishClientTransport(int clientFd, TLSServerContext& tlsContext,
                              bool allowPlaintext, SecureSocket& socket,
                              bool& cancelRequest) {
    cancelRequest = false;
    uint8_t header[8]{};
    // MSG_PEEK may return a short prefix when TCP segmentation delivers the
    // startup packet in multiple reads. Keep peeking from offset zero until
    // the complete 8-byte discriminator is available; do not consume it.
    while (true) {
        ssize_t peeked = ::recv(clientFd, header, sizeof(header), MSG_PEEK);
        if (peeked <= 0) return false;
        if (peeked == static_cast<ssize_t>(sizeof(header))) break;
    }
    const uint32_t length = rawUInt32(header);
    const uint32_t code = rawUInt32(header + 4);
    constexpr uint32_t sslRequestCode = 80877103;
    constexpr uint32_t cancelRequestCode = 80877102;

    if (length == 8 && code == sslRequestCode) {
        uint8_t request[8]{};
        if (!readRawExact(clientFd, request, sizeof(request))) return false;
        if (!tlsContext.enabled()) {
            const char response = 'N';
            if (!writeRawAll(clientFd, &response, 1)) return false;
            if (!allowPlaintext) return false;
            socket = SecureSocket(clientFd);
            return true;
        }
        const char response = 'S';
        if (!writeRawAll(clientFd, &response, 1)) return false;
        socket = SecureSocket(clientFd, tlsContext.ctx());
        return socket.handshake();
    }
    if (length == 16 && code == cancelRequestCode) {
        uint8_t request[16]{};
        if (!readRawExact(clientFd, request, sizeof(request))) return false;
        cancelRequest = true;
        (void)cancelBackend(rawUInt32(request + 8), rawUInt32(request + 12));
        return false;
    }
    if (tlsContext.enabled()) return false;
    if (!allowPlaintext) return false;
    socket = SecureSocket(clientFd);
    return true;
}

void sendQueryResult(PostgresProtocol& protocol, const QueryResult& result,
                     char transactionStatus, bool sendReady = true) {
    for (const auto& notice : result.notices) {
        protocol.sendNoticeResponse(notice.message, notice.severity,
                                    notice.sqlState);
    }
    if (!result.error && result.commandTag.empty() && !result.resultSet) {
        protocol.sendEmptyQueryResponse();
        if (sendReady) protocol.sendReadyForQuery(transactionStatus);
        return;
    }
    if (result.resultSet) {
        std::vector<PgColumnDescription> columns = result.columnDescriptions;
        if (columns.size() != result.columns.size()) {
            columns.clear();
            columns.reserve(result.columns.size());
            for (const auto& name : result.columns) columns.push_back(PgColumnDescription{name});
        }
        protocol.sendRowDescription(columns);
        for (size_t rowIndex = 0; rowIndex < result.rows.size(); ++rowIndex) {
            const auto& row = result.rows[rowIndex];
            std::vector<std::string> normalized = row;
            normalized.resize(result.columns.size());
            const std::vector<bool> nulls = rowIndex < result.nulls.size()
                ? result.nulls[rowIndex] : std::vector<bool>{};
            protocol.sendDataRow(normalized, columns, nulls);
        }
    }
    if (result.error) {
        protocol.sendErrorResponse("ERROR", result.sqlState, trimText(result.errorMessage));
        if (sendReady) protocol.sendReadyForQuery(transactionStatus);
        return;
    }
    protocol.sendCommandComplete(result.commandTag);
    if (sendReady) protocol.sendReadyForQuery(transactionStatus);
}

bool sendPendingNotifications(PostgresProtocol& protocol,
                              uint64_t backendId) {
    for (const auto& notification :
         notificationManager().takePending(backendId)) {
        if (!protocol.sendNotificationResponse(
                notification.senderPid, notification.channel,
                notification.payload)) {
            return false;
        }
    }
    return true;
}

void handleClient(SecureSocket socket, std::string clientHost) {
    g_stats.totalConnections++;
    PostgresProtocol protocol(socket);
    PgStartupMessage startup;
    std::string protocolError;
    if (!protocol.readStartup(startup, protocolError)) {
        protocol.sendErrorResponse("FATAL", "08P01", protocolError);
        return;
    }

    const auto userIt = startup.parameters.find("user");
    const std::string username = userIt == startup.parameters.end() ? "" : userIt->second;
    if (username.empty()) {
        protocol.sendErrorResponse("FATAL", "28000", "startup packet is missing user");
        return;
    }

    const char* configuredHba = std::getenv("DBMS_PG_HBA");
    const std::string hbaPath = configuredHba && *configuredHba ? configuredHba : "pg_hba.conf";
    const auto hbaRecords = PgHbaFile::parse(hbaPath);
    std::string clientIp = clientHost;
    const size_t portSeparator = clientIp.rfind(':');
    if (portSeparator != std::string::npos) clientIp.resize(portSeparator);
    const HbaMethod authMethod = PgHbaFile::match(
        hbaRecords, socket.tlsOK ? "hostssl" : "hostnossl",
        startup.parameters.count("database") ? startup.parameters.at("database") : username,
        username, clientIp,
        [](const std::string& member, const std::string& role) {
            return userIsMemberOfRole(member, role);
        });
    if (hbaRecords.empty() || authMethod == HbaMethod::Reject) {
        protocol.sendErrorResponse("FATAL", "28000",
                                   "no pg_hba.conf entry for host " + clientIp +
                                   ", user " + username);
        return;
    }

    bool authenticationCompleted = false;
    if (authMethod == HbaMethod::Trust) {
        std::string ignoredPassword;
        if (!getStoredUserPassword(username, ignoredPassword)) {
            protocol.sendErrorResponse("FATAL", "28000", "role does not exist or cannot log in");
            return;
        }
        authenticationCompleted = protocol.sendAuthenticationOk();
    } else {
        std::string storedPassword;
        if (!getStoredUserPassword(username, storedPassword)) {
            protocol.sendErrorResponse("FATAL", "28P01", "password authentication failed");
            return;
        }
        if (authMethod == HbaMethod::ScramSha256 ||
            (authMethod == HbaMethod::Md5 && storedPassword.rfind("SCRAM-SHA-256$", 0) == 0)) {
            authenticationCompleted = authenticateScram(protocol, username, storedPassword,
                                                        protocolError);
        } else if (authMethod == HbaMethod::Password) {
            if (!protocol.sendAuthenticationCleartextPassword()) return;
            PgFrontendMessage passwordMessage;
            if (!protocol.readMessage(passwordMessage, protocolError) || passwordMessage.type != 'p') {
                protocol.sendErrorResponse("FATAL", "08P01", "expected PasswordMessage");
                return;
            }
            const std::string password = messageCString(passwordMessage);
            if (password.empty() || !verifyUserPassword(username, password)) {
                protocol.sendErrorResponse("FATAL", "28P01", "password authentication failed");
                return;
            }
            authenticationCompleted = protocol.sendAuthenticationOk();
        } else {
            protocol.sendErrorResponse("FATAL", "0A000",
                                       "pg_hba authentication method is not implemented");
            return;
        }
    }
    if (!authenticationCompleted) return;
    constexpr uint32_t supportedProtocolVersion = 0x00030000;
    if ((startup.protocolVersion & 0xffffU) != 0 ||
        !startup.unsupportedProtocolOptions.empty()) {
        if (!protocol.sendNegotiateProtocolVersion(
                supportedProtocolVersion,
                startup.unsupportedProtocolOptions)) {
            return;
        }
    }

    Session session;
    session.compatibilityMode = dbms::defaultCompatibilityMode();
    struct BackendSessionGuard {
        Session* session;
        ~BackendSessionGuard() {
            // Restore/abort the live transaction before removing session
            // objects, so its undo cannot recreate already cleaned temp data.
            g_engine.endBackendSession();
            if (session) {
                if (session->advisoryOwnerId != 0)
                    advisoryLockManager().releaseAll(
                        session->advisoryOwnerId);
                notificationManager().disconnect(session->pid);
                if (session->tempNamespaceCreated) {
                    g_engine.dropSessionTemporaryObjects(session->currentDB, session->pid);
                }
                for (const auto& name : session->tempTables) {
                    g_engine.dropTable(session->currentDB,
                                       tempTablePrefix(*session, name));
                }
                for (const auto& name : session->transientTempTables) {
                    g_engine.dropTable(session->currentDB,
                                       tempTablePrefix(*session, name));
                }
                session->tempTables.clear();
                session->transientTempTables.clear();
            }
        }
    } backendSessionGuard{&session};
    session.username = username;
    session.permission = permissionQuery(username);
    session.authenticatedUser = username;
    session.authenticatedPermission = session.permission;
    auto dbIt = startup.parameters.find("database");
    session.currentDB = (dbIt == startup.parameters.end() || dbIt->second.empty())
                            ? username : dbIt->second;
    session.originalRole = username;
    session.statementTimeoutMs = g_config.statementTimeoutMs;
    session.defaultStatementTimeoutMs = g_config.statementTimeoutMs;
    session.lockTimeoutMs = g_config.lockTimeoutMs;
    session.deadlockTimeoutMs = g_config.deadlockTimeoutMs;
    std::string startupSqlState = "22023";
    std::string startupError;
    if (!applyStartupParameters(startup, session, startupSqlState,
                                startupError)) {
        protocol.sendErrorResponse("FATAL", startupSqlState, startupError);
        return;
    }
    g_engine.getLockManager().setResourceNamespace(session.currentDB);
    g_engine.getLockManager().setLockTimeout(session.lockTimeoutMs);
    g_engine.getLockManager().setDeadlockTimeout(session.deadlockTimeoutMs);

    if (!g_engine.databaseExists(session.currentDB)) {
        protocol.sendErrorResponse("FATAL", "3D000", "database \"" + session.currentDB + "\" does not exist");
        return;
    }

    const auto account = authCatalog().getAuthIdByName(username);
    if (!account || !tryReserveRoleConnection(*account)) {
        protocol.sendErrorResponse("FATAL", "53300", "too many connections for role \"" + username + "\"");
        return;
    }
    struct RoleConnectionGuard {
        std::string roleName;
        ~RoleConnectionGuard() { releaseRoleConnection(roleName); }
    } roleConnectionGuard{username};

    const BackendRegistration registration = registerProcess(
        username, clientHost, session.currentDB, session.interruptState);
    if (registration.pid == 0) {
        const bool exists = g_engine.databaseExists(session.currentDB);
        protocol.sendErrorResponse(
            "FATAL", exists ? "55006" : "3D000",
            exists ? "database is being dropped"
                   : "database \"" + session.currentDB + "\" does not exist");
        return;
    }
    const uint64_t pid = registration.pid;
    session.pid = pid;
    if (!protocol.sendParameterStatus(
            "server_version", std::string("18.0 DBMS-C++ ") + DBMS_VERSION_STRING) ||
        !protocol.sendParameterStatus("server_encoding", "UTF8") ||
        !protocol.sendParameterStatus("client_encoding", session.clientEncoding) ||
        !protocol.sendParameterStatus("application_name", session.applicationName) ||
        !protocol.sendParameterStatus("DateStyle", "ISO, MDY") ||
        !protocol.sendParameterStatus("IntervalStyle", "postgres") ||
        !protocol.sendParameterStatus("is_superuser", account->rolsuper ? "on" : "off") ||
        !protocol.sendParameterStatus("session_authorization", username) ||
        !protocol.sendParameterStatus("default_transaction_read_only", "off") ||
        !protocol.sendParameterStatus("in_hot_standby", "off") ||
        !protocol.sendParameterStatus("integer_datetimes", "on") ||
        !protocol.sendParameterStatus("standard_conforming_strings", "on") ||
        !protocol.sendParameterStatus("scram_iterations", std::to_string(
            dbms::scram::kDefaultIterations)) ||
        !protocol.sendParameterStatus("search_path", session.searchPath) ||
        !protocol.sendParameterStatus("TimeZone", "UTC") ||
        !protocol.sendBackendKeyData(static_cast<uint32_t>(pid), registration.secretKey) ||
        !protocol.sendReadyForQuery()) {
        unregisterProcess(pid);
        return;
    }
    auto reportedParameterStatuses =
        mutableProtocolParameterStatuses(session);
    std::string reportedEffectiveRole = session.currentRole.empty()
        ? session.username : session.currentRole;
    const auto sendChangedParameterStatuses = [&]() -> bool {
        const std::string effectiveRole = session.currentRole.empty()
            ? session.username : session.currentRole;
        const auto priorSuperuser = reportedParameterStatuses.find("is_superuser");
        const std::string* unchangedSuperuser =
            g_engine.databaseTransactionOwnershipDeferred() &&
            effectiveRole == reportedEffectiveRole &&
            priorSuperuser != reportedParameterStatuses.end()
                ? &priorSuperuser->second : nullptr;
        const auto current = mutableProtocolParameterStatuses(
            session, unchangedSuperuser);
        for (const auto& parameter : current) {
            const auto previous = reportedParameterStatuses.find(
                parameter.first);
            if (previous != reportedParameterStatuses.end() &&
                previous->second == parameter.second) {
                continue;
            }
            if (!protocol.sendParameterStatus(parameter.first,
                                              parameter.second)) {
                return false;
            }
        }
        reportedParameterStatuses = current;
        reportedEffectiveRole = effectiveRole;
        return true;
    };

    // ---- Pooled backend contexts (PgBouncer-style) ------------------
    // Session mode: one backend rented for the client's whole lifetime,
    // discarded on disconnect (temp tables die with it — same as an
    // unpooled backend). Transaction/statement mode: a backend is rented
    // while a transaction/statement runs and returned between statements,
    // so idle clients do not hold backend resources. Known limitation
    // (shared with PgBouncer's transaction mode): session-level state
    // such as PREPARE ... names does not travel across rentals.
    auto& pool = ConnectionPool::instance();
    const bool sessionMode = pool.mode() == ConnectionPool::Mode::Session;
    std::shared_ptr<BackendContext> backend;
    if (sessionMode) {
        backend = pool.acquire(username, session.currentDB);
        if (backend) {
            // The client's freshly authenticated session is authoritative;
            // the backend slot only tracks the rental for accounting.
            backend->session = session;
        }
    }
    struct BackendRelease {
        ConnectionPool& pool;
        std::shared_ptr<BackendContext>& backend;
        bool sessionMode;
        std::shared_ptr<BackendContext>& held;
        ~BackendRelease() {
            if (held) {
                if (sessionMode) pool.discard(held);
                else {
                    ConnectionPool::resetForReuse(*held);
                    pool.release(held);
                }
            }
        }
    } backendRelease{pool, backend, sessionMode, backend};
    bool transactionFailed = false;
    bool extendedQueryError = false;
    bool extendedImplicitTransaction = false;
    bool extendedQueryTimeoutActive = false;
    std::chrono::steady_clock::time_point extendedQueryDeadline{};
    const auto clearExtendedQueryTimeout = [&]() {
        extendedQueryTimeoutActive = false;
    };
    const auto startExtendedQueryTimeout = [&]() {
        if (extendedQueryTimeoutActive || session.statementTimeoutMs <= 0)
            return;
        extendedQueryTimeoutActive = true;
        extendedQueryDeadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(session.statementTimeoutMs);
    };
    const auto remainingExtendedQueryTimeoutMs = [&]() -> int {
        if (!extendedQueryTimeoutActive) return -1;
        const auto now = std::chrono::steady_clock::now();
        if (now >= extendedQueryDeadline) return 0;
        const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(
            extendedQueryDeadline - now).count();
        return static_cast<int>(std::min<int64_t>(
            remaining, std::numeric_limits<int>::max()));
    };
    const auto extendedQueryTimeoutExpired = [&]() {
        return extendedQueryTimeoutActive &&
            std::chrono::steady_clock::now() >= extendedQueryDeadline;
    };
    // Parse/Bind can own a snapshot without having entered an implicit
    // transaction block through Execute. DISCARD ALL distinguishes these.
    bool extendedImplicitExecuted = false;
    // Connection-local extended-query state. In transaction/statement pool
    // modes these do not travel across backend rentals (PgBouncer has the
    // same restriction for session-level features).
    std::map<std::string, ProtocolPortal> portals;
    const auto readyStatus = [&]() -> char {
        if (transactionFailed) return 'E';
        return g_engine.inTransaction() ? 'T' : 'I';
    };
    const auto updateProcessIdle = [&]() {
        const char status = readyStatus();
        const std::string state = status == 'E'
            ? "idle in transaction (aborted)"
            : status == 'T' ? "idle in transaction" : "idle";
        updateProcessInfo(pid, "Idle", state, "");
    };
    const auto abortActiveTransaction = [&]() -> bool {
        // Short-rent pooling may have executed on a now-returned Session.
        // Snapshot restoration must use this connection's live state.
        dbms::setCurrentSession(&session);
        const auto recovery = g_engine.latestUserSavepoint();
        if (recovery) {
            if (g_engine.rollbackToSavepoint(*recovery) != DBStatus::OK)
                return false;
            (void)notificationManager().rollbackToSavepoint(
                session.pid, *recovery);
            if (session.advisoryOwnerId != 0) {
                advisoryLockManager().rollbackToSavepoint(
                    session.advisoryOwnerId, *recovery);
            }
            return true;
        }
        if (g_engine.inTransaction()) {
            (void)g_engine.rollbackTransaction();
        }
        if (g_engine.inTransaction()) return false;
        notificationManager().rollbackTransaction(session.pid);
        if (session.advisoryOwnerId != 0) {
            advisoryLockManager().releaseTransaction(session.advisoryOwnerId);
        }
        return true;
    };
    const auto sendExtendedProtocolError = [&](const std::string& sqlState,
                                                const std::string& errorMessage) -> bool {
        clearExtendedQueryTimeout();
        // Parse, Bind, Describe, and wire validation can fail before
        // executeForProtocol runs. They must use the same abort boundary,
        // before ErrorResponse, not defer lock/undo cleanup until Sync.
        if (g_engine.inTransaction()) {
            (void)abortActiveTransaction();
            transactionFailed = true;
        }
        session.failedTransactionBlock = transactionFailed;
        extendedQueryError = true;
        return protocol.sendErrorResponse("ERROR", sqlState, errorMessage);
    };
    const auto executeForProtocol = [&](const std::string& sql) -> QueryResult {
        // Portal objects live in this connection loop rather than Session.
        // Mirror their count while a command executes so a database-context
        // replacement cannot leave a portal bound to the previous database.
        session.openProtocolPortals = portals.size();
        session.failedTransactionBlock = transactionFailed;
        const std::string lexicalError = SQLParser::lexicalError(sql);
        if (!lexicalError.empty()) {
            if (g_engine.inTransaction()) {
                (void)abortActiveTransaction();
                transactionFailed = true;
                session.failedTransactionBlock = true;
            }
            QueryResult error;
            error.error = true;
            error.sqlState = "42601";
            error.errorMessage = lexicalError;
            return error;
        }
        if (transactionFailed && !isTransactionRecoveryCommand(sql)) {
            // PostgreSQL reports raw syntax errors even in an aborted
            // block. A valid BEGIN still receives 25P02, and must not
            // restart the transaction or emit a nested-BEGIN warning.
            const std::string keyword = firstSqlKeyword(sql);
            if (keyword == "begin" || keyword == "start" ||
                (keyword == "set" && SQLParser::isSetTransactionStatement(sql))) {
                const size_t offset = sqlCommandOffset(sql);
                SQLParser parser;
                auto parsed = parser.parse(offset == std::string::npos ? sql : sql.substr(offset));
                if (!parsed.success) {
                    QueryResult error;
                    error.error = true;
                    error.sqlState = "42601";
                    error.errorMessage = parsed.error;
                    return error;
                }
            }
            return transactionAbortedResult();
        }
        const bool wasInTransaction = g_engine.inTransaction();
        bool repeatedTransactionStart = false;
        if (wasInTransaction && !extendedImplicitTransaction &&
            (firstSqlKeyword(sql) == "begin" || startsWithSqlPhrase(sql, "start transaction"))) {
            const size_t offset = sqlCommandOffset(sql);
            SQLParser parser;
            auto parsed = parser.parse(offset == std::string::npos ? sql : sql.substr(offset));
            const auto* transaction = parsed.success
                ? dynamic_cast<const TransactionStmt*>(parsed.stmt.get()) : nullptr;
            repeatedTransactionStart = transaction &&
                (transaction->kind == TransactionStmt::Kind::Begin ||
                 transaction->kind == TransactionStmt::Kind::Start);
        }
        if (isTransactionRecoveryCommand(sql)) {
            SQLParser parser;
            auto parsed = parser.parse(sql);
            const auto* transaction = parsed.success
                ? dynamic_cast<const TransactionStmt*>(parsed.stmt.get()) : nullptr;
            if (transaction && transaction->chain &&
                !transactionFailed && (!wasInTransaction || extendedImplicitTransaction)) {
                if (wasInTransaction) (void)abortActiveTransaction();
                QueryResult error;
                error.error = true;
                error.sqlState = "25P01";
                error.errorMessage = "AND CHAIN can only be used in transaction blocks";
                return error;
            }
        }
        const std::string effectiveSql = transactionFailed
                                             ? rollbackCommandForAbortedTransaction(sql)
                                             : sql;
        // Short-rent pooling: borrow a backend for the statement, run on
        // its session state, then hand it back. Between statements this
        // client holds no backend at all.
        std::shared_ptr<BackendContext> rented;
        Session* execSession = &session;
        if (!sessionMode) {
            rented = pool.acquire(username, session.currentDB);
            if (rented) {
                rented->session = session;  // project client state onto backend
                execSession = &rented->session;
            }
        }
        QueryResult result = executeProtocolQuery(
            effectiveSql, *execSession,
            remainingExtendedQueryTimeoutMs());
        if (!result.error && (!wasInTransaction || extendedImplicitTransaction) &&
            SQLParser::isSetTransactionStatement(sql)) {
            SQLParser parser;
            const auto parsed = parser.parse(sql);
            if (parsed.success) {
                result.notices.erase(std::remove_if(result.notices.begin(), result.notices.end(),
                    [](const QueryNotice& notice) {
                        return notice.severity == "WARNING" && notice.sqlState == "01000";
                    }), result.notices.end());
                result.notices.push_back(QueryNotice{"WARNING", "25P01",
                    "SET TRANSACTION can only be used in transaction blocks"});
            }
        }
        if (repeatedTransactionStart) {
            // This diagnostic derives from validated statement/block state,
            // not captured output, and precedes a SET-option error as in PG.
            result.notices.push_back(QueryNotice{
                "WARNING", "25001", "there is already a transaction in progress"});
        }
        if (!sessionMode && rented) {
            session = rented->session;      // carry back GUC/DB changes
            const bool statementEndsTran =
                !g_engine.inTransaction() && !wasInTransaction;
            (void)statementEndsTran;
            ConnectionPool::resetForReuse(*rented);
            pool.release(rented);
            rented.reset();
        }
        if (result.error && wasInTransaction) {
            // Only a transaction-ending command may return idle on error.
            // Ordinary DML may internally roll back the engine after a lock
            // failure, but the SQL connection must stay failed until ROLLBACK.
            const std::string keyword = firstSqlKeyword(sql);
            const bool transactionEnding =
                (keyword == "commit" && !startsWithSqlPhrase(sql, "commit prepared")) ||
                keyword == "end" || startsWithSqlPhrase(sql, "prepare transaction");
            if (!transactionEnding) {
                // Abort the active user subtransaction now, before sending
                // ErrorResponse. Its locks must not block other backends
                // while this connection waits for an explicit ROLLBACK TO.
                // Keep the recovery point and the protocol's failed state.
                (void)abortActiveTransaction();
            }
            transactionFailed = !transactionEnding || g_engine.inTransaction();
        } else if (!result.error && isTransactionRecoveryCommand(sql)) {
            transactionFailed = false;
        }
        session.failedTransactionBlock = transactionFailed;
        if (!result.error && (firstSqlKeyword(sql) == "begin" ||
                              startsWithSqlPhrase(sql, "start transaction"))) {
            // A user BEGIN inside an extended-query implicit transaction
            // promotes the block; Sync must no longer end it automatically.
            extendedImplicitTransaction = false;
            extendedImplicitExecuted = false;
        }
        return result;
    };
    const auto prepareExtendedQuerySnapshot = [&](const std::string& sql) -> bool {
        // Parse analysis and Bind planning may take the first snapshot
        // before Execute. Keep an implicit block alive until Sync.
        // Existing failed blocks must not be silently restarted here.
        if (transactionFailed) return true;
        try {
            SQLParser parser;
            const auto parsed = parser.parseForBinding(sql);
            const bool databaseIndependent = parsed.success && parsed.stmt &&
                SQLParser::isDatabaseIndependentQuery(*parsed.stmt);
            if (!g_engine.inTransaction()) {
                std::unique_ptr<StorageEngine::DatabaseIndependentBeginScope> deferredBegin;
                if (databaseIndependent)
                    deferredBegin = std::make_unique<StorageEngine::DatabaseIndependentBeginScope>(g_engine);
                const QueryResult begin = executeForProtocol("BEGIN");
                if (begin.error) {
                    sendExtendedProtocolError(begin.sqlState, begin.errorMessage);
                    return false;
                }
                extendedImplicitTransaction = true;
                extendedImplicitExecuted = false;
            }
            if (!databaseIndependent) {
                const auto status = g_engine.ensureDatabaseTransactionOwnership();
                if (status != DBStatus::OK)
                    throw DbError(sqlstateForDBStatus(status), "could not acquire database transaction ownership");
            }
            g_engine.noteQuerySnapshot();
            return true;
        } catch (const DbError& error) {
            sendExtendedProtocolError(error.sqlState(), error.message());
        } catch (const std::exception& error) {
            sendExtendedProtocolError("XX000", error.what());
        }
        return false;
    };
    struct CopyStatementBoundary {
        bool startedTransaction = false;
        bool savepointCreated = false;
        bool originallyInTransaction = false;
        std::string savepoint;
    };
    const auto beginCopyBoundary = [&](CopyStatementBoundary& boundary,
                                       QueryResult& error) -> bool {
        boundary.originallyInTransaction = g_engine.inTransaction();
        if (!boundary.originallyInTransaction) {
            QueryResult begin = executeForProtocol("BEGIN");
            if (begin.error) {
                error = std::move(begin);
                return false;
            }
            boundary.startedTransaction = true;
        }
        static std::atomic<uint64_t> sequence{0};
        boundary.savepoint = "__dbms_wire_copy_" +
            std::to_string(session.pid) + "_" +
            std::to_string(sequence.fetch_add(1));
        dbms::setCurrentSession(&session);
        try {
            const DBStatus status = g_engine.createStatementSavepoint(
                boundary.savepoint);
            if (status != DBStatus::OK) {
                error.error = true;
                error.sqlState = sqlstateForDBStatus(status);
                error.errorMessage = "could not create COPY statement savepoint";
            } else {
                boundary.savepointCreated = true;
                if (!notificationManager().savepoint(session.pid, boundary.savepoint)) {
                    notificationManager().beginTransaction(session.pid);
                    (void)notificationManager().savepoint(session.pid, boundary.savepoint);
                }
                if (session.advisoryOwnerId != 0) {
                    advisoryLockManager().savepoint(
                        session.advisoryOwnerId, boundary.savepoint);
                }
            }
        } catch (const DbError& failure) {
            error.error = true;
            error.sqlState = failure.sqlState();
            error.errorMessage = failure.message();
        } catch (const std::exception& failure) {
            error.error = true;
            error.sqlState = "XX000";
            error.errorMessage = failure.what();
        }
        if (error.error) {
            if (boundary.startedTransaction) {
                (void)executeForProtocol("ROLLBACK");
            }
            return false;
        }
        return true;
    };
    const auto finishCopyBoundary = [&](CopyStatementBoundary& boundary,
                                        bool success,
                                        QueryResult& error) -> bool {
        if (!success) {
            if (boundary.savepointCreated) {
                QueryResult rollback = executeForProtocol(
                    "ROLLBACK TO SAVEPOINT " + boundary.savepoint);
                if (rollback.error && !error.error) error = std::move(rollback);
                QueryResult release = executeForProtocol(
                    "RELEASE SAVEPOINT " + boundary.savepoint);
                if (release.error && !error.error) error = std::move(release);
            }
            if (boundary.startedTransaction) {
                QueryResult rollback = executeForProtocol("ROLLBACK");
                if (rollback.error && !error.error) error = std::move(rollback);
            }
            return !error.error;
        }
        if (boundary.savepointCreated) {
            QueryResult release = executeForProtocol(
                "RELEASE SAVEPOINT " + boundary.savepoint);
            if (release.error) {
                error = std::move(release);
                if (boundary.startedTransaction) {
                    (void)executeForProtocol("ROLLBACK");
                }
                return false;
            }
        }
        if (boundary.startedTransaction) {
            QueryResult commit = executeForProtocol("COMMIT");
            if (commit.error) {
                error = std::move(commit);
                if (transactionFailed || g_engine.inTransaction()) {
                    (void)executeForProtocol("ROLLBACK");
                }
                return false;
            }
        }
        return true;
    };
    struct CopyExecutionOutcome {
        bool connectionOk = true;
        bool success = false;
        bool syncConsumed = false;
        bool failedInExistingTransaction = false;
    };
    const auto executeCopyWire = [&](CopyWirePlan plan)
        -> CopyExecutionOutcome {
        CopyExecutionOutcome outcome;
        const auto copyDeadline = extendedQueryTimeoutActive
            ? extendedQueryDeadline
            : session.statementTimeoutMs > 0
                ? std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(session.statementTimeoutMs)
                : std::chrono::steady_clock::time_point::max();
        outcome.failedInExistingTransaction = g_engine.inTransaction();
        if (transactionFailed) {
            const QueryResult error = transactionAbortedResult();
            outcome.connectionOk = protocol.sendErrorResponse(
                "ERROR", error.sqlState, trimText(error.errorMessage));
            return outcome;
        }
        CopyStatementBoundary boundary;
        boundary.originallyInTransaction = outcome.failedInExistingTransaction;
        const auto abortCopyTransaction = [&]() -> bool {
            if (boundary.originallyInTransaction) transactionFailed = true;
            bool cleaned = false;
            try {
                cleaned = abortActiveTransaction();
            } catch (...) {
                // Preserve the original COPY error and let the statement
                // boundary attempt its remaining cleanup after input drains.
            }
            if (cleaned) {
                // User rollback or top-level abort removed the internal COPY
                // guard. Do not issue recovery SQL for that vanished guard.
                boundary.savepointCreated = false;
                boundary.startedTransaction = false;
            }
            return cleaned;
        };
        if (!validateCopyWirePlan(plan, session)) {
            if (outcome.failedInExistingTransaction)
                (void)abortCopyTransaction();
            outcome.connectionOk = protocol.sendErrorResponse(
                "ERROR", plan.sqlState.empty() ? "XX000" : plan.sqlState,
                plan.error);
            return outcome;
        }

        QueryResult boundaryError;
        if (!beginCopyBoundary(boundary, boundaryError)) {
            (void)abortCopyTransaction();
            outcome.connectionOk = protocol.sendErrorResponse(
                "ERROR", boundaryError.sqlState,
                trimText(boundaryError.errorMessage));
            return outcome;
        }
        outcome.failedInExistingTransaction =
            boundary.originallyInTransaction;

        const std::vector<uint16_t> formats(plan.columnIndexes.size(), 0);
        if (plan.direction == CopyWireDirection::FromStdin) {
            if (!protocol.sendCopyInResponse(0, formats)) {
                QueryResult ignored;
                (void)finishCopyBoundary(boundary, false, ignored);
                outcome.connectionOk = false;
                return outcome;
            }
            CopyStreamResult stream = receiveCopyIn(
                protocol, plan, session, abortCopyTransaction,
                copyDeadline);
            outcome.connectionOk = stream.transportOk;
            outcome.syncConsumed = stream.syncConsumed;
            QueryResult finishError;
            const bool finished = finishCopyBoundary(
                boundary, stream.success, finishError);
            if (!outcome.connectionOk) return outcome;
            if (!stream.success) {
                if (!stream.errorSent) {
                    (void)abortCopyTransaction();
                    outcome.connectionOk = protocol.sendErrorResponse(
                        "ERROR",
                        stream.sqlState.empty() ? "XX000" : stream.sqlState,
                        stream.error.empty() ? "COPY FROM failed"
                                             : stream.error);
                }
                if (!finished && !finishError.errorMessage.empty()) {
                    protocol.sendNoticeResponse(
                        "COPY rollback failed: " +
                            trimText(finishError.errorMessage),
                        "WARNING", "XX000");
                }
                return outcome;
            }
            if (!finished) {
                (void)abortCopyTransaction();
                outcome.connectionOk = protocol.sendErrorResponse(
                    "ERROR", finishError.sqlState,
                    trimText(finishError.errorMessage));
                return outcome;
            }
            outcome.success = protocol.sendCommandComplete(
                "COPY " + std::to_string(stream.rows));
            outcome.connectionOk = outcome.success;
            return outcome;
        }

        if (!protocol.sendCopyOutResponse(0, formats)) {
            QueryResult ignored;
            (void)finishCopyBoundary(boundary, false, ignored);
            outcome.connectionOk = false;
            return outcome;
        }
        CopyStreamResult stream = sendCopyOut(
            protocol, plan, session, copyDeadline);
        QueryResult finishError;
        const bool finished = finishCopyBoundary(
            boundary, stream.success, finishError);
        outcome.connectionOk = stream.transportOk;
        if (!outcome.connectionOk) return outcome;
        if (!stream.success) {
            (void)abortCopyTransaction();
            // COPY TO may already have filled the peer's receive window. Do
            // not re-enter an unbounded blocking write while reporting a
            // timeout or another mid-stream error; send opportunistically,
            // then close if the client still cannot drain the response.
            outcome.connectionOk = protocol.sendErrorResponseUntil(
                "ERROR", stream.sqlState.empty() ? "XX000" : stream.sqlState,
                stream.error.empty() ? "COPY TO failed" : stream.error,
                std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(100));
            return outcome;
        }
        if (!finished) {
            (void)abortCopyTransaction();
            outcome.connectionOk = protocol.sendErrorResponse(
                "ERROR", finishError.sqlState,
                trimText(finishError.errorMessage));
            return outcome;
        }
        outcome.success = protocol.sendCommandComplete(
            "COPY " + std::to_string(stream.rows));
        outcome.connectionOk = outcome.success;
        return outcome;
    };
    const auto finishExtendedImplicitTransaction = [&](bool failed) -> QueryResult {
        QueryResult endResult;
        if (extendedImplicitTransaction) {
            endResult = executeForProtocol(failed ? "ROLLBACK" : "COMMIT");
            extendedImplicitTransaction = false;
            portals.clear();
            if (endResult.error && (transactionFailed || g_engine.inTransaction())) {
                // Once the statement has failed, its deadline must not block
                // protocol-owned rollback cleanup.
                clearExtendedQueryTimeout();
                (void)executeForProtocol("ROLLBACK");
            }
        }
        extendedImplicitExecuted = false;
        return endResult;
    };
    const auto finishExtendedSync = [&]() -> bool {
        const QueryResult endResult = finishExtendedImplicitTransaction(extendedQueryError);
        if (endResult.error) {
            if (!protocol.sendErrorResponse(
                    "ERROR", endResult.sqlState,
                    trimText(endResult.errorMessage))) return false;
        } else if (!g_engine.inTransaction()) {
            portals.clear();
        }
        if (!sendPendingNotifications(protocol, session.pid)) return false;
        if (!sendChangedParameterStatuses()) return false;
        if (!protocol.sendReadyForQuery(readyStatus())) return false;
        // PostgreSQL cancels an extended-protocol statement timeout only
        // after the Sync response has completed.
        clearExtendedQueryTimeout();
        extendedQueryError = false;
        return true;
    };
    while (true) {
        if (extendedQueryError) clearExtendedQueryTimeout();
        if (extendedQueryTimeoutExpired()) {
            if (!sendExtendedProtocolError(
                    "57014", "canceling statement due to statement timeout"))
                break;
            continue;
        }
        if (!socket.hasBufferedInput()) {
            pollfd descriptor{};
            descriptor.fd = socket.fd;
            descriptor.events = POLLIN;
            int ready = 0;
            do {
                const int pollTimeoutMs = extendedQueryTimeoutActive
                    ? std::min(100, std::max(
                          1, remainingExtendedQueryTimeoutMs()))
                    : 100;
                ready = ::poll(&descriptor, 1, pollTimeoutMs);
            } while (ready < 0 && errno == EINTR);
            if (ready < 0) break;
            if (ready == 0) {
                if (!sendPendingNotifications(protocol, session.pid)) break;
                continue;
            }
            if ((descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 &&
                (descriptor.revents & POLLIN) == 0) {
                break;
            }
        }
        PgFrontendMessage message;
        if (!protocol.readMessage(message, protocolError)) break;
        if (!extendedQueryError &&
            (message.type == 'P' || message.type == 'B' ||
             message.type == 'D' || message.type == 'E')) {
            // Start at message arrival, before notification processing or
            // Parse/Bind/Describe work can consume the statement's budget.
            startExtendedQueryTimeout();
        }
        if (!sendPendingNotifications(protocol, session.pid)) break;
        if (message.type == 'X') {
            if (!message.payload.empty()) {
                protocol.sendErrorResponse("FATAL", "08P01",
                                           "malformed Terminate message");
            }
            break;
        }
        if (extendedQueryError && message.type != 'S') continue;
        if (extendedQueryTimeoutExpired()) {
            if (!sendExtendedProtocolError(
                    "57014", "canceling statement due to statement timeout"))
                break;
            // Sync itself is the other protocol boundary that cancels the
            // timeout, so continue processing it after reporting expiry.
            if (message.type != 'S') continue;
        }
        if (message.type == 'Q') {
            // A simple-query message executes its statements under their own
            // per-statement timers, independent of a pending extended cycle.
            clearExtendedQueryTimeout();
            size_t queryOffset = 0;
            std::string sql;
            if (!PostgresProtocol::readCString(message.payload, queryOffset, sql) ||
                queryOffset != message.payload.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Query message");
                protocol.sendReadyForQuery(readyStatus());
                continue;
            }
            updateProcessInfo(pid, "Query", "active", trimText(sql));
            // A Simple Query invalidates the unnamed extended-query objects.
            session.preparedStmts.erase("");
            session.preparedStmtTypes.erase("");
            session.preparedStmtParameterOids.erase("");
            portals.erase("");
            if (!SQLParser::lexicalError(sql).empty()) {
                const QueryResult result = executeForProtocol(sql);
                (void)finishExtendedImplicitTransaction(true);
                updateProcessIdle();
                sendQueryResult(protocol, result, readyStatus());
                continue;
            }
            const std::vector<std::string> statements =
                splitSimpleQueryStatements(sql);
            if (statements.empty()) {
                const QueryResult completion = finishExtendedImplicitTransaction(false);
                updateProcessIdle();
                QueryResult empty;
                sendQueryResult(protocol, completion.error ? completion : empty, readyStatus());
                continue;
            }

            if (statements.size() == 1 &&
                isStandaloneDiscardAllStatement(statements.front()) &&
                extendedImplicitTransaction && !extendedImplicitExecuted &&
                !transactionFailed) {
                const QueryResult completion = finishExtendedImplicitTransaction(false);
                if (completion.error) {
                    updateProcessIdle();
                    sendQueryResult(protocol, completion, readyStatus());
                    continue;
                }
            }

            std::vector<CopyWirePlan> copyPlans;
            copyPlans.reserve(statements.size());
            bool hasWireCopy = false;
            bool hasCopyGateError = false;
            for (const auto& statement : statements) {
                CopyWirePlan plan = parseCopyWirePlan(statement);
                hasWireCopy = hasWireCopy || plan.wire;
                hasCopyGateError = hasCopyGateError ||
                    (plan.matched && !plan.error.empty());
                copyPlans.push_back(std::move(plan));
            }
            if (statements.size() > 1 &&
                (hasWireCopy || hasCopyGateError)) {
                const bool failedTransaction = g_engine.inTransaction();
                if (failedTransaction) {
                    (void)abortActiveTransaction();
                    transactionFailed = true;
                }
                protocol.sendErrorResponse(
                    "ERROR", "0A000",
                    "wire COPY cannot be combined with other statements in one Query message");
                (void)finishExtendedImplicitTransaction(true);
                updateProcessIdle();
                protocol.sendReadyForQuery(readyStatus());
                continue;
            }
            if (statements.size() == 1 && copyPlans.front().matched &&
                !copyPlans.front().error.empty()) {
                const bool failedTransaction = g_engine.inTransaction();
                if (failedTransaction) {
                    (void)abortActiveTransaction();
                    transactionFailed = true;
                }
                protocol.sendErrorResponse(
                    "ERROR", copyPlans.front().sqlState,
                    copyPlans.front().error);
                (void)finishExtendedImplicitTransaction(true);
                updateProcessIdle();
                protocol.sendReadyForQuery(readyStatus());
                continue;
            }
            if (statements.size() == 1 && copyPlans.front().wire) {
                CopyExecutionOutcome copy =
                    executeCopyWire(std::move(copyPlans.front()));
                if (!copy.connectionOk) break;
                if (!copy.success && copy.failedInExistingTransaction) {
                    transactionFailed = true;
                }
                const QueryResult completion = finishExtendedImplicitTransaction(!copy.success);
                if (completion.error && !protocol.sendErrorResponse(
                        "ERROR", completion.sqlState, trimText(completion.errorMessage))) break;
                if (!g_engine.inTransaction()) portals.clear();
                updateProcessDb(pid, session.currentDB);
                updateProcessIdle();
                if (!sendPendingNotifications(protocol, session.pid)) break;
                if (!sendChangedParameterStatuses()) break;
                if (!protocol.sendReadyForQuery(readyStatus())) break;
                continue;
            }

            const bool implicitBatchTransaction =
                statements.size() > 1 && !g_engine.inTransaction() &&
                std::none_of(statements.begin(), statements.end(),
                             isTransactionControlStatement);
            std::vector<QueryResult> results;
            results.reserve(statements.size());
            bool batchError = false;
            if (implicitBatchTransaction) {
                QueryResult beginResult = executeForProtocol("BEGIN");
                if (beginResult.error) {
                    results.push_back(std::move(beginResult));
                    batchError = true;
                }
            }
            if (!batchError) {
                for (const auto& statement : statements) {
                    updateProcessInfo(pid, "Query", "active", trimText(statement));
                    results.push_back(executeForProtocol(statement));
                    if (results.back().error) {
                        batchError = true;
                        break;
                    }
                }
            }
            if (implicitBatchTransaction) {
                if (batchError) {
                    (void)executeForProtocol("ROLLBACK");
                } else {
                    QueryResult commitResult = executeForProtocol("COMMIT");
                    if (commitResult.error) {
                        results.push_back(std::move(commitResult));
                        batchError = true;
                        if (transactionFailed || g_engine.inTransaction()) {
                            (void)executeForProtocol("ROLLBACK");
                        }
                    }
                }
            }
            // Parse/Bind may have opened a physical transaction before this
            // Simple Query. End it now, unless a user BEGIN promoted it to
            // an explicit block. Its portals end with the transaction too.
            QueryResult completion = finishExtendedImplicitTransaction(batchError);
            if (completion.error) results.push_back(std::move(completion));
            if (!g_engine.inTransaction()) {
                // Named portals are transaction-scoped even when COMMIT or
                // ROLLBACK arrived through the Simple Query protocol.
                portals.clear();
            }
            updateProcessDb(pid, session.currentDB);
            updateProcessIdle();
            if (!sendPendingNotifications(protocol, session.pid)) break;
            for (const auto& result : results) {
                sendQueryResult(protocol, result, readyStatus(), false);
            }
            if (!sendChangedParameterStatuses()) break;
            protocol.sendReadyForQuery(readyStatus());
            continue;
        }
        if (message.type == 'P') {
            size_t offset = 0;
            std::string statement;
            std::string sql;
            if (!PostgresProtocol::readCString(message.payload, offset, statement) ||
                !PostgresProtocol::readCString(message.payload, offset, sql) ||
                offset + 2 > message.payload.size()) {
                sendExtendedProtocolError("08P01", "malformed Parse message");
                extendedQueryError = true;
                continue;
            }
            const uint16_t parameterCount = PostgresProtocol::readUInt16(message.payload, offset);
            offset += 2;
            if (message.payload.size() - offset !=
                static_cast<size_t>(parameterCount) * 4) {
                sendExtendedProtocolError("08P01", "malformed Parse parameter type list");
                extendedQueryError = true;
                continue;
            }
            std::vector<uint32_t> parameterTypes;
            parameterTypes.reserve(parameterCount);
            uint32_t unknownParameterType = 0;
            for (uint16_t i = 0; i < parameterCount; ++i) {
                const uint32_t parameterType =
                    PostgresProtocol::readUInt32(message.payload, offset);
                offset += 4;
                parameterTypes.push_back(parameterType);
                if (parameterType != 0 &&
                    !dbms::isBuiltinTypeOid(parameterType) &&
                    g_engine.catalogService().get(session.currentDB).findType(parameterType) == nullptr) {
                    unknownParameterType = parameterType;
                }
            }
            if (unknownParameterType != 0) {
                sendExtendedProtocolError("42704",
                    "type with OID " + std::to_string(unknownParameterType) +
                        " does not exist");
                extendedQueryError = true;
                continue;
            }
            const std::string lexicalError = SQLParser::lexicalError(sql);
            if (!lexicalError.empty()) {
                sendExtendedProtocolError("42601", lexicalError);
                continue;
            }
            if (splitSimpleQueryStatements(sql).size() > 1) {
                sendExtendedProtocolError("42601",
                    "cannot insert multiple commands into a prepared statement");
                extendedQueryError = true;
                continue;
            }
            size_t inferredParameterCount = 0;
            std::string parameterError;
            if (!dbms::ps_analyzeDollarParams(
                    sql, inferredParameterCount, parameterError)) {
                sendExtendedProtocolError("42P02", parameterError);
                extendedQueryError = true;
                continue;
            }
            if (inferredParameterCount >
                static_cast<size_t>(std::numeric_limits<uint16_t>::max())) {
                sendExtendedProtocolError("54000", "too many prepared statement parameters");
                extendedQueryError = true;
                continue;
            }
            // Parse may omit all or a suffix of its parameter OIDs.  Those
            // slots are unspecified (OID zero) until a full analyzer can
            // infer a stronger type from the expression context.
            parameterTypes.resize(
                std::max(parameterTypes.size(), inferredParameterCount), 0);
            if (!statement.empty() &&
                session.preparedStmts.count(statement) != 0) {
                sendExtendedProtocolError("42P05",
                    "prepared statement \"" + statement + "\" already exists");
                extendedQueryError = true;
                continue;
            }
            if (SQLParser::requiresQuerySnapshot(sql) &&
                !prepareExtendedQuerySnapshot(sql)) {
                continue;
            }
            if (const auto duplicate = SQLParser::duplicateCteName(sql)) {
                if (transactionFailed) {
                    const auto aborted = transactionAbortedResult();
                    sendExtendedProtocolError(aborted.sqlState, aborted.errorMessage);
                } else {
                    sendExtendedProtocolError("42712", "WITH query name \"" +
                        *duplicate + "\" specified more than once");
                }
                continue;
            }
            try {
                notePreparedTemporaryObjectAccess(sql, session);
                if(parameterTypes.empty()) {
                    // Whole provider analysis precedes publishing a prepared
                    // statement. A failed call never leaves a named object
                    // behind or turns its next Parse into spurious 42P05.
                    std::vector<PgColumnDescription> staticColumns;
                    if (!describePreparedSetResult(sql,session,staticColumns))
                        (void)describePreparedQueryHostResult(sql,session,staticColumns);
                }
            } catch (const DbError& error) {
                sendExtendedProtocolError(error.sqlState(), error.message());
                continue;
            }
            if (statement.empty()) {
                // A new unnamed statement replaces the old unnamed statement
                // and invalidates the unnamed portal. Named portals retain
                // their already-bound query text.
                session.preparedStmts.erase(statement);
                session.preparedStmtTypes.erase(statement);
                session.preparedStmtParameterOids.erase(statement);
                portals.erase("");
            }
            session.preparedStmts[statement] = std::move(sql);
            session.preparedStmtTypes[statement] =
                std::vector<std::string>(parameterTypes.size());
            session.preparedStmtParameterOids[statement] =
                std::move(parameterTypes);
            protocol.sendParseComplete();
            continue;
        }
        if (message.type == 'B') {
            size_t offset = 0;
            std::string portal;
            std::string statement;
            if (!PostgresProtocol::readCString(message.payload, offset, portal) ||
                !PostgresProtocol::readCString(message.payload, offset, statement)) {
                sendExtendedProtocolError("08P01", "malformed Bind message");
                extendedQueryError = true;
                continue;
            }
            const auto statementIt = session.preparedStmts.find(statement);
            const auto oidIt = session.preparedStmtParameterOids.find(statement);
            if (statementIt == session.preparedStmts.end() ||
                oidIt == session.preparedStmtParameterOids.end()) {
                sendExtendedProtocolError("26000", "prepared statement does not exist");
                extendedQueryError = true;
                continue;
            }
            const std::string& preparedSql = statementIt->second;
            const std::vector<uint32_t>& preparedParameterTypes = oidIt->second;
            const bool bareValues = firstSqlKeyword(preparedSql) == "values";
            if (offset + 2 > message.payload.size()) {
                sendExtendedProtocolError("08P01", "malformed Bind message");
                extendedQueryError = true;
                continue;
            }
            const uint16_t parameterFormatCount =
                PostgresProtocol::readUInt16(message.payload, offset);
            offset += 2;
            std::vector<uint16_t> parameterFormats;
            if (parameterFormatCount > 0) {
                if (parameterFormatCount > message.payload.size() / 2 ||
                    offset + static_cast<size_t>(parameterFormatCount) * 2 > message.payload.size()) {
                    sendExtendedProtocolError("08P01", "malformed Bind format list");
                    extendedQueryError = true;
                    continue;
                }
                parameterFormats.reserve(parameterFormatCount);
                for (uint16_t i = 0; i < parameterFormatCount; ++i) {
                    parameterFormats.push_back(
                        PostgresProtocol::readUInt16(message.payload, offset));
                    offset += 2;
                }
            }
            if (offset + 2 > message.payload.size()) {
                sendExtendedProtocolError("08P01", "malformed Bind parameter count");
                extendedQueryError = true;
                continue;
            }
            const uint16_t valueCount = PostgresProtocol::readUInt16(message.payload, offset);
            offset += 2;
            if (valueCount != preparedParameterTypes.size()) {
                sendExtendedProtocolError("08P01", "bind message supplies a different number of parameters");
                extendedQueryError = true;
                continue;
            }
            if (!parameterFormats.empty() && parameterFormats.size() != 1 &&
                parameterFormats.size() != valueCount) {
                sendExtendedProtocolError("08P01", "bind message has an invalid parameter format count");
                extendedQueryError = true;
                continue;
            }
            if ((valueCount != 0 || SQLParser::requiresQuerySnapshot(preparedSql)) &&
                !prepareExtendedQuerySnapshot(preparedSql)) {
                continue;
            }
            std::vector<std::string> literals;
            literals.reserve(valueCount);
            bool bindError = false;
            std::string bindErrorMessage;
            std::string bindErrorSqlstate = "0A000";
            for (uint16_t i = 0; i < valueCount; ++i) {
                if (offset + 4 > message.payload.size()) {
                    bindError = true;
                    bindErrorMessage = "malformed Bind parameter value";
                    bindErrorSqlstate = "08P01";
                    break;
                }
                const int32_t valueLength = PostgresProtocol::readInt32(message.payload, offset);
                offset += 4;
                if (valueLength < -1 ||
                    (valueLength >= 0 &&
                     static_cast<size_t>(valueLength) > message.payload.size() - offset)) {
                    bindError = true;
                    bindErrorMessage = "malformed Bind parameter value length";
                    bindErrorSqlstate = "08P01";
                    break;
                }
                uint16_t format = 0;
                if (parameterFormats.size() == 1) format = parameterFormats.front();
                else if (parameterFormats.size() == valueCount) format = parameterFormats[i];
                if (format != 0 && format != 1) {
                    bindError = true;
                    bindErrorMessage = "unsupported parameter format code";
                    break;
                }
                if (valueLength == -1) {
                    literals.push_back("NULL");
                } else {
                    std::vector<uint8_t> raw(
                        message.payload.begin() + static_cast<std::ptrdiff_t>(offset),
                        message.payload.begin() + static_cast<std::ptrdiff_t>(offset + valueLength));
                    offset += static_cast<size_t>(valueLength);
                    std::string literal = protocolParameterLiteral(
                        preparedParameterTypes[i], raw, format == 1,
                        session.lcMonetary, bindErrorMessage);
                    if (!bindErrorMessage.empty()) {
                        bindError = true;
                        bindErrorSqlstate =
                            protocolParameterErrorSqlstate(bindErrorMessage);
                        break;
                    }
                    literals.push_back(std::move(literal));
                }
                if (bareValues) {
                    if (const char* castType =
                            valuesParameterCastType(preparedParameterTypes[i])) {
                        literals.back() = "CAST(" + literals.back() +
                            " AS " + castType + ")";
                    }
                }
            }
            if (bindError) {
                sendExtendedProtocolError(bindErrorSqlstate, bindErrorMessage);
                extendedQueryError = true;
                continue;
            }
            if (offset + 2 > message.payload.size()) {
                sendExtendedProtocolError("08P01", "malformed Bind result format count");
                extendedQueryError = true;
                continue;
            }
            const uint16_t resultFormatCount = PostgresProtocol::readUInt16(message.payload, offset);
            offset += 2;
            if (offset + static_cast<size_t>(resultFormatCount) * 2 != message.payload.size()) {
                sendExtendedProtocolError("08P01", "malformed Bind result format list");
                extendedQueryError = true;
                continue;
            }
            std::vector<uint16_t> resultFormats;
            resultFormats.reserve(resultFormatCount);
            for (uint16_t i = 0; i < resultFormatCount; ++i) {
                const uint16_t format = PostgresProtocol::readUInt16(message.payload, offset);
                offset += 2;
                if (format > 1) {
                    sendExtendedProtocolError("08P01", "unsupported result format code");
                    extendedQueryError = true;
                    resultFormats.clear();
                    break;
                }
                resultFormats.push_back(format);
            }
            if (extendedQueryError) continue;
            std::string expandedSql;
            std::string substitutionError;
            if (!dbms::ps_substituteDollarParams(
                    preparedSql, literals, expandedSql, substitutionError)) {
                sendExtendedProtocolError("42P02", substitutionError);
                extendedQueryError = true;
                continue;
            }
            try {
                // Bind planning may fold immutable primitive expressions.
                // Evaluate only after the *whole* AST proves that it contains
                // no relation, routine, parameter or opaque SQL child. This
                // cannot execute a stored writer or a CTE during preparation.
                SQLParser parser;
                auto parsed = parser.parseForBinding(expandedSql);
                if (parsed.success && parsed.stmt &&
                    SQLParser::isDatabaseIndependentQuery(*parsed.stmt)) {
                    const auto& select = static_cast<const SelectStmt&>(*parsed.stmt);
                    ExprEvaluator evaluator;
                    RowContext row;
                    for (const auto& item : select.selectList)
                        (void)evaluator.eval(item.expr, row);
                    if (select.whereClause) (void)evaluator.eval(select.whereClause, row);
                    for (const auto& item : select.orderBy)
                        (void)evaluator.eval(item.expr, row);
                    for (const auto& expression : select.distinctOn)
                        (void)evaluator.eval(expression, row);
                }
            } catch (const DbError& error) {
                sendExtendedProtocolError(error.sqlState(), error.message());
                continue;
            }
            if (!portal.empty() && portals.count(portal) != 0) {
                sendExtendedProtocolError("42P03",
                    "portal \"" + portal + "\" already exists");
                extendedQueryError = true;
                continue;
            }
            portals[portal] = ProtocolPortal{
                statement, std::move(expandedSql), preparedSql,
                preparedParameterTypes, std::move(resultFormats)};
            protocol.sendBindComplete();
            continue;
        }
        if (message.type == 'E') {
            size_t offset = 0;
            std::string portal;
            if (!PostgresProtocol::readCString(message.payload, offset, portal)) {
                sendExtendedProtocolError("08P01", "malformed Execute message");
                extendedQueryError = true;
                continue;
            }
            if (offset + 4 != message.payload.size()) {
                sendExtendedProtocolError("08P01", "malformed Execute max-rows field");
                extendedQueryError = true;
                continue;
            }
            const int32_t maxRows = PostgresProtocol::readInt32(message.payload, offset);
            if (maxRows < 0) {
                sendExtendedProtocolError("08P01", "negative portal row limit");
                extendedQueryError = true;
                continue;
            }
            auto portalIt = portals.find(portal);
            if (portalIt == portals.end()) {
                sendExtendedProtocolError("34000", "portal does not exist");
                extendedQueryError = true;
                continue;
            }
            if (!portalIt->second.executed &&
                isStandaloneDiscardAllStatement(portalIt->second.sql) &&
                extendedImplicitTransaction && !extendedImplicitExecuted &&
                !transactionFailed) {
                // Finishing the planning transaction expires all its
                // portals. Keep only this utility's value state, never a
                // reference into the map that the completion clears.
                ProtocolPortal utilityPortal = portalIt->second;
                const QueryResult completion = finishExtendedImplicitTransaction(false);
                if (completion.error) {
                    sendExtendedProtocolError(completion.sqlState, completion.errorMessage);
                    extendedQueryError = true;
                    continue;
                }
                portalIt = portals.emplace(portal, std::move(utilityPortal)).first;
            }
            auto& portalState = portalIt->second;
            if (!portalState.executed) {
                CopyWirePlan copyPlan = parseCopyWirePlan(portalState.sql);
                if (copyPlan.matched && !copyPlan.error.empty()) {
                    if (g_engine.inTransaction()) {
                        (void)abortActiveTransaction();
                        transactionFailed = true;
                    }
                    protocol.sendErrorResponse(
                        "ERROR", copyPlan.sqlState, copyPlan.error);
                    portalState.executed = true;
                    portalState.completed = true;
                    extendedQueryError = true;
                    continue;
                }
                if (copyPlan.wire) {
                    if (maxRows != 0) {
                        if (g_engine.inTransaction()) {
                            (void)abortActiveTransaction();
                            transactionFailed = true;
                        }
                        protocol.sendErrorResponse(
                            "ERROR", "0A000",
                            "wire COPY does not support portal row limits");
                        portalState.executed = true;
                        portalState.completed = true;
                        extendedQueryError = true;
                        continue;
                    }
                    if (std::any_of(
                            portalState.resultFormats.begin(),
                            portalState.resultFormats.end(),
                            [](uint16_t format) { return format != 0; })) {
                        if (g_engine.inTransaction()) {
                            (void)abortActiveTransaction();
                            transactionFailed = true;
                        }
                        protocol.sendErrorResponse(
                            "ERROR", "0A000",
                            "wire COPY binary result format is not supported");
                        portalState.executed = true;
                        portalState.completed = true;
                        extendedQueryError = true;
                        continue;
                    }
                    if (!g_engine.inTransaction()) {
                        QueryResult beginResult = executeForProtocol("BEGIN");
                        if (beginResult.error) {
                            protocol.sendErrorResponse(
                                "ERROR", beginResult.sqlState,
                                beginResult.errorMessage);
                            portalState.executed = true;
                            portalState.completed = true;
                            extendedQueryError = true;
                            continue;
                        }
                        extendedImplicitTransaction = true;
                        extendedImplicitExecuted = false;
                    }
                    if (extendedImplicitTransaction) extendedImplicitExecuted = true;
                    CopyExecutionOutcome copy =
                        executeCopyWire(std::move(copyPlan));
                    portalState.executed = true;
                    portalState.completed = true;
                    if (!copy.connectionOk) break;
                    if (!copy.success) {
                        extendedQueryError = true;
                        if (copy.failedInExistingTransaction) {
                            transactionFailed = true;
                        }
                    }
                    if (copy.syncConsumed && !finishExtendedSync()) break;
                    continue;
                }
                if (!g_engine.inTransaction() &&
                    !isTransactionControlStatement(portalState.sql) &&
                    !SQLParser::isSetTransactionStatement(portalState.sql) &&
                    !isStandaloneDiscardAllStatement(portalState.sql)) {
                    QueryResult beginResult = executeForProtocol("BEGIN");
                    if (beginResult.error) {
                        protocol.sendErrorResponse(
                            "ERROR", beginResult.sqlState,
                            beginResult.errorMessage);
                        extendedQueryError = true;
                        continue;
                    }
                    extendedImplicitTransaction = true;
                    extendedImplicitExecuted = false;
                }
                if (extendedImplicitTransaction) extendedImplicitExecuted = true;
                updateProcessInfo(pid, "Query", "active",
                                  trimText(portalState.sql));
                portalState.result = executeForProtocol(portalState.sql);
                updateProcessIdle();
                portalState.executed = true;
            }
            if (!sendPendingNotifications(protocol, session.pid)) break;
            QueryResult& result = portalState.result;
            for (const auto& notice : result.notices) {
                protocol.sendNoticeResponse(notice.message, notice.severity,
                                            notice.sqlState);
            }
            result.notices.clear();
            if (result.error) {
                protocol.sendErrorResponse("ERROR", result.sqlState, result.errorMessage);
                extendedQueryError = true;
            }
            else if (result.resultSet) {
                std::vector<PgColumnDescription> columns = result.columnDescriptions;
                if (columns.size() != result.columns.size()) {
                    columns.clear();
                    for (const auto& name : result.columns) {
                        columns.push_back(PgColumnDescription{name});
                    }
                }
                if (!portalState.resultFormats.empty() &&
                    portalState.resultFormats.size() != 1 &&
                    portalState.resultFormats.size() != columns.size()) {
                    sendExtendedProtocolError("08P01", "result format count does not match result columns");
                    extendedQueryError = true;
                    continue;
                }
                if (portalState.resultFormats.size() == 1) {
                    for (auto& column : columns) {
                        column.formatCode = static_cast<int16_t>(portalState.resultFormats.front());
                    }
                } else if (!portalState.resultFormats.empty()) {
                    for (size_t i = 0; i < columns.size(); ++i) {
                        columns[i].formatCode = static_cast<int16_t>(portalState.resultFormats[i]);
                    }
                }
                const size_t remaining = result.rows.size() - portalState.rowOffset;
                const size_t batchSize = maxRows == 0
                                             ? remaining
                                             : std::min(remaining, static_cast<size_t>(maxRows));
                bool rowsSent = true;
                for (size_t i = 0; i < batchSize; ++i) {
                    const size_t rowIndex = portalState.rowOffset + i;
                    const auto& row = result.rows[rowIndex];
                    std::vector<std::string> normalized = row;
                    normalized.resize(columns.size());
                    const std::vector<bool> nulls =
                        rowIndex < result.nulls.size()
                            ? result.nulls[rowIndex]
                            : std::vector<bool>{};
                    if (!protocol.sendDataRow(normalized, columns, nulls)) {
                        sendExtendedProtocolError("0A000", "binary result type is not supported");
                        extendedQueryError = true;
                        rowsSent = false;
                        break;
                    }
                }
                if (!rowsSent) continue;
                portalState.rowOffset += batchSize;
                if (portalState.rowOffset < result.rows.size()) {
                    protocol.sendPortalSuspended();
                } else {
                    portalState.completed = true;
                    protocol.sendCommandComplete(result.commandTag);
                }
            } else {
                portalState.completed = true;
                if (result.commandTag.empty()) {
                    protocol.sendEmptyQueryResponse();
                } else {
                    protocol.sendCommandComplete(result.commandTag);
                }
            }
            if (!result.error && isStandaloneDiscardAllStatement(portalState.sql)) {
                // DISCARD ALL commits immediately, even before Sync/Flush.
                portals.clear();
                extendedImplicitTransaction = false;
                extendedImplicitExecuted = false;
            }
            clearExtendedQueryTimeout();
            continue;
        }
        if (message.type == 'D') {
            if (message.payload.empty()) {
                sendExtendedProtocolError("08P01", "malformed Describe message");
                extendedQueryError = true;
                continue;
            }
            const char target = static_cast<char>(message.payload[0]);
            size_t offset = 1;
            std::string name;
            if (!PostgresProtocol::readCString(message.payload, offset, name) ||
                offset != message.payload.size()) {
                sendExtendedProtocolError("08P01", "malformed Describe message");
                extendedQueryError = true;
                continue;
            }
            if (target == 'S') {
                const auto statementIt = session.preparedStmts.find(name);
                const auto oidIt = session.preparedStmtParameterOids.find(name);
                if (statementIt == session.preparedStmts.end() ||
                    oidIt == session.preparedStmtParameterOids.end()) {
                    sendExtendedProtocolError("26000", "prepared statement does not exist");
                    extendedQueryError = true;
                    continue;
                }
                std::vector<PgColumnDescription> columns;
                bool hasColumns;
                try {
                    hasColumns = describePreparedResult(statementIt->second, session,
                                                        columns, oidIt->second);
                } catch (const DbError& error) {
                    sendExtendedProtocolError(error.sqlState(), error.message());
                    continue;
                }
                if (!protocol.sendParameterDescription(oidIt->second) ||
                    !(hasColumns ? protocol.sendRowDescription(columns)
                                 : protocol.sendNoData())) {
                    extendedQueryError = true;
                }
                continue;
            }
            if (target == 'P') {
                auto portalIt = portals.find(name);
                if (portalIt == portals.end()) {
                    sendExtendedProtocolError("34000", "portal does not exist");
                    extendedQueryError = true;
                    continue;
                }
                std::vector<PgColumnDescription> columns;
                bool hasColumns;
                try {
                    hasColumns = describePreparedResult(portalIt->second.preparedSql, session, columns,
                                                        portalIt->second.parameterOids);
                } catch (const DbError& error) {
                    sendExtendedProtocolError(error.sqlState(), error.message());
                    continue;
                }
                if (!hasColumns) {
                    if (!protocol.sendNoData()) extendedQueryError = true;
                    continue;
                }
                const auto& formats = portalIt->second.resultFormats;
                if (!formats.empty() && formats.size() != 1 &&
                    formats.size() != columns.size()) {
                    sendExtendedProtocolError("08P01",
                        "result format count does not match result columns");
                    extendedQueryError = true;
                    continue;
                }
                for (size_t i = 0; i < columns.size(); ++i) {
                    if (!formats.empty()) columns[i].formatCode =
                        static_cast<int16_t>(formats.size() == 1
                                                 ? formats.front() : formats[i]);
                }
                if (!protocol.sendRowDescription(columns)) extendedQueryError = true;
                continue;
            }
            sendExtendedProtocolError("08P01", "invalid Describe target");
            extendedQueryError = true;
            continue;
        }
        if (message.type == 'C') {
            if (message.payload.empty()) {
                sendExtendedProtocolError("08P01", "malformed Close message");
                extendedQueryError = true;
                continue;
            }
            const char target = static_cast<char>(message.payload[0]);
            size_t offset = 1;
            std::string name;
            if (!PostgresProtocol::readCString(message.payload, offset, name) ||
                offset != message.payload.size()) {
                sendExtendedProtocolError("08P01", "malformed Close message");
                extendedQueryError = true;
                continue;
            }
            if (target == 'S') {
                // A bound portal owns its query independently. Closing the
                // source statement must not invalidate an existing portal.
                session.preparedStmts.erase(name);
                session.preparedStmtTypes.erase(name);
                session.preparedStmtParameterOids.erase(name);
            } else if (target == 'P') {
                portals.erase(name);
            } else {
                sendExtendedProtocolError("08P01", "invalid Close target");
                extendedQueryError = true;
                continue;
            }
            protocol.sendCloseComplete();
            continue;
        }
        if (message.type == 'S') {
            if (!message.payload.empty()) {
                sendExtendedProtocolError("08P01",
                                           "malformed Sync message");
                protocol.sendReadyForQuery(readyStatus());
                extendedQueryError = false;
                continue;
            }
            if (!finishExtendedSync()) break;
            continue;
        }
        if (message.type == 'H') {
            if (!message.payload.empty()) {
                sendExtendedProtocolError("08P01",
                                           "malformed Flush message");
                extendedQueryError = true;
            }
            continue;
        }
        sendExtendedProtocolError("08P01", "unsupported frontend message");
        extendedQueryError = true;
    }

    unregisterProcess(pid);
}

} // namespace

namespace {

std::string environmentOr(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value && *value ? value : fallback;
}

} // namespace

void requestServerShutdown() {
    g_serverStopRequested.store(true, std::memory_order_relaxed);
    const int listenFd = g_listenFd.load(std::memory_order_relaxed);
    if (listenFd >= 0) ::shutdown(listenFd, SHUT_RDWR);
    shutdownActiveClients();
}

bool serverShutdownRequested() {
    return g_signalStopRequested != 0 ||
           g_serverStopRequested.load(std::memory_order_relaxed);
}

bool startServer(int port, bool allowPlaintext) {
    g_serverStopRequested.store(false, std::memory_order_relaxed);
    g_signalStopRequested = 0;

    // TLS is fail-closed. Certificate paths are deployment configuration, not
    // generated at runtime, so a fresh server cannot accidentally expose a
    // private key or downgrade an authenticated connection to plaintext.
    TLSServerContext tlsCtx;
    std::string certFile = environmentOr("DBMS_TLS_CERT", "server.crt");
    std::string keyFile = environmentOr("DBMS_TLS_KEY", "server.key");
    if (std::filesystem::exists(certFile) && std::filesystem::exists(keyFile)) {
        if (tlsCtx.init(certFile, keyFile)) {
            std::cout << "TLS encryption enabled (certificate: " << certFile << ")" << std::endl;
        } else {
            std::cerr << "TLS initialization failed; refusing to start" << std::endl;
        }
    } else if (!allowPlaintext) {
        std::cerr << "TLS certificate/key not found (DBMS_TLS_CERT=" << certFile
                  << ", DBMS_TLS_KEY=" << keyFile
                  << "); refusing to start without --insecure" << std::endl;
        return false;
    }

    if (!isServerTransportAllowed(tlsCtx.enabled(), allowPlaintext)) {
        std::cerr << "TLS is unavailable; refusing to start without --insecure" << std::endl;
        return false;
    }

    int serverFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return false;
    }

    int opt = 1;
    ::setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(serverFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "Failed to bind to port " << port << std::endl;
        ::close(serverFd);
        return false;
    }

    if (::listen(serverFd, 10) < 0) {
        std::cerr << "Failed to listen" << std::endl;
        ::close(serverFd);
        return false;
    }

    ServerSignalGuard signalGuard;
    if (!signalGuard.install()) {
        std::cerr << "Failed to install server signal handlers" << std::endl;
        ::close(serverFd);
        return false;
    }
    g_listenFd.store(serverFd, std::memory_order_relaxed);

    std::cout << "DBMS server (version " << DBMS_VERSION_STRING
              << ", compatibility mode "
              << dbms::defaultCompatibilityMode()
              << ") listening on port " << port;
    if (tlsCtx.enabled()) {
        std::cout << " (TLS enabled)";
    } else {
        std::cout << " (INSECURE plaintext; explicitly enabled)";
    }
    std::cout << std::endl;
    std::cout << "note: this is not a PostgreSQL server cluster; the data "
                 "directory layout, wire defaults and tooling differ"
              << std::endl;

    struct Worker {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> done;
    };
    std::vector<std::unique_ptr<Worker>> workers;
    auto reapWorkers = [&]() {
        for (auto it = workers.begin(); it != workers.end();) {
            if (!(*it)->done->load(std::memory_order_acquire)) {
                ++it;
                continue;
            }
            (*it)->thread.join();
            it = workers.erase(it);
        }
    };

    bool acceptLoopHealthy = true;
    while (!serverShutdownRequested()) {
        reapWorkers();
        struct pollfd listenPoll{};
        listenPoll.fd = serverFd;
        listenPoll.events = POLLIN;
        const int pollResult = ::poll(&listenPoll, 1, 250);
        if (pollResult < 0) {
            if (errno == EINTR) continue;
            std::cerr << "Listen poll failed: " << std::strerror(errno) << std::endl;
            acceptLoopHealthy = false;
            break;
        }
        if (pollResult == 0 || serverShutdownRequested()) continue;
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = ::accept(serverFd, reinterpret_cast<sockaddr*>(&clientAddr), &clientLen);
        if (clientFd < 0) {
            if (serverShutdownRequested() || errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
            std::cerr << "Accept failed: " << std::strerror(errno) << std::endl;
            acceptLoopHealthy = false;
            break;
        }

        if (!tryReserveConnectionSlot()) {
            // Refuse immediately. A rejected connection must not consume a
            // worker slot while waiting for a client that cannot be served.
            ::shutdown(clientFd, SHUT_RDWR);
            ::close(clientFd);
            g_stats.rejectedConnections++;
            continue;
        }
        auto& pool = ConnectionPool::instance();
        if (!pool.tryReserveClientSlot()) {
            // Pool-level client limit (max_client_conn) reached.
            ::shutdown(clientFd, SHUT_RDWR);
            ::close(clientFd);
            releaseConnectionSlot();
            g_stats.rejectedConnections++;
            continue;
        }
        std::string clientHost = inet_ntoa(clientAddr.sin_addr);
        clientHost += ":" + std::to_string(ntohs(clientAddr.sin_port));

        // Disable Nagle on the accepted socket: protocol responses are
        // written as several small sends (row data, then command status,
        // then ReadyForQuery).  With Nagle + the client's delayed ACK the
        // second segment waits out the ~40ms delayed-ACK timer, which
        // showed up as a flat 41ms floor on EVERY statement — including
        // an empty query — and compounded into multi-second stalls under
        // concurrent load.
        int nodelay = 1;
        ::setsockopt(clientFd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        registerClientFd(clientFd);
        auto done = std::make_shared<std::atomic<bool>>(false);
        try {
            auto worker = std::make_unique<Worker>();
            worker->done = done;
            worker->thread = std::thread([clientFd, &tlsCtx, allowPlaintext, clientHost, done]() {
                struct SlotGuard {
                    ~SlotGuard() {
                        ConnectionPool::instance().releaseClientSlot();
                        releaseConnectionSlot();
                    }
                } slotGuard;
                SecureSocket socket;
                bool cancelRequest = false;
                if (!establishClientTransport(clientFd, tlsCtx, allowPlaintext,
                                              socket, cancelRequest)) {
                    if (socket.fd < 0) ::close(clientFd);
                    if (!cancelRequest) {
                        std::cerr << "client transport negotiation failed" << std::endl;
                    }
                } else {
                    handleClient(std::move(socket), clientHost);
                }
                // Close before removing the descriptor from the shutdown
                // registry. Otherwise a new accept could reuse this fd and
                // the SecureSocket destructor would close the new client.
                socket.close();
                unregisterClientFd(clientFd);
                done->store(true, std::memory_order_release);
            });
            workers.push_back(std::move(worker));
        } catch (...) {
            unregisterClientFd(clientFd);
            ::shutdown(clientFd, SHUT_RDWR);
            ::close(clientFd);
            pool.releaseClientSlot();
            releaseConnectionSlot();
            std::cerr << "Failed to create client worker" << std::endl;
            acceptLoopHealthy = false;
            requestServerShutdown();
            break;
        }
    }

    g_listenFd.store(-1, std::memory_order_relaxed);
    ::close(serverFd);
    shutdownActiveClients();
    for (auto& worker : workers) worker->thread.join();
    ConnectionPool::instance().shutdown();
    return acceptLoopHealthy;
}

} // namespace dbms
