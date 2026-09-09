#include "NetworkServer.h"
#include "common/version.h"
#include "common/DbError.h"
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
#include "PostgresNumeric.h"
#include "process/SqlStats.h"
#include "process/RuntimeStats.h"
#include "process/OutputCapture.h"
#include "Config.h"
#include <netinet/tcp.h>

#include <algorithm>
#include <arpa/inet.h>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
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
#include <unistd.h>

// External globals from main.cpp
extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

// Forward declare execute() and logSlowQuery() from main.cpp
extern bool execute(const std::string& rawSql, Session& s);
extern double g_slowQueryThresholdMs;
extern void logSlowQuery(const std::string& sql, double ms,
                         const std::string& username,
                         const std::string& dbname);

namespace dbms {

static ServerStats g_stats;

// Process list: active connections
static std::mutex g_processMutex;
static std::map<uint64_t, ProcessInfo> g_processList;
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

uint64_t registerProcess(const std::string& user, const std::string& host, const std::string& db) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    uint64_t pid = g_nextProcessId++;
    ProcessInfo info;
    info.id = pid;
    info.user = user;
    info.host = host;
    info.db = db;
    info.command = "Sleep";
    info.timeSec = 0.0;
    info.state = "";
    info.info = "";
    info.connectTime = std::chrono::steady_clock::now();
    g_processList[pid] = std::move(info);
    return pid;
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
}

bool cancelBackend(uint64_t pid) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    auto it = g_processList.find(pid);
    if (it == g_processList.end()) return false;
    it->second.cancelRequested = true;
    return true;
}

bool terminateBackend(uint64_t pid) {
    std::lock_guard<std::mutex> lock(g_processMutex);
    auto it = g_processList.find(pid);
    if (it == g_processList.end()) return false;
    it->second.terminateRequested = true;
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
    std::string commandTag;
};

struct ProtocolPreparedStatement {
    std::string sql;
    std::vector<uint32_t> parameterTypes;
};

struct ProtocolPortal {
    ProtocolPortal() = default;
    ProtocolPortal(std::string statementName, std::string query,
                   std::vector<uint16_t> formats)
        : statement(std::move(statementName)),
          sql(std::move(query)),
          resultFormats(std::move(formats)) {}

    std::string statement;
    std::string sql;
    std::vector<uint16_t> resultFormats;
    QueryResult result;
    size_t rowOffset = 0;
    bool executed = false;
    bool rowDescriptionSent = false;
    bool completed = false;
};

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
                                           std::string& error) {
    uint64_t bits = 0;
    switch (typeOid) {
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
                                     std::string& error) {
    if (binary) return binaryProtocolParameterLiteral(typeOid, raw, error);
    std::string value(raw.begin(), raw.end());
    if (value.find('\0') != std::string::npos) {
        error = "parameter contains a NUL byte";
        return {};
    }
    if (typeOid == 16) {
        std::string lower = value;
        for (char& c : lower) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (lower == "t" || lower == "true" || lower == "1") return "TRUE";
        if (lower == "f" || lower == "false" || lower == "0") return "FALSE";
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
    return quoteProtocolText(value, error);
}

bool substituteProtocolParameters(const std::string& sql,
                                  const std::vector<std::string>& literals,
                                  std::string& expanded,
                                  std::string& error) {
    expanded.clear();
    expanded.reserve(sql.size());
    bool singleQuoted = false;
    bool doubleQuoted = false;
    for (size_t i = 0; i < sql.size(); ++i) {
        const char c = sql[i];
        if (singleQuoted) {
            expanded.push_back(c);
            if (c == '\'' && i + 1 < sql.size() && sql[i + 1] == '\'') {
                expanded.push_back(sql[++i]);
            } else if (c == '\'') {
                singleQuoted = false;
            }
            continue;
        }
        if (doubleQuoted) {
            expanded.push_back(c);
            if (c == '"' && i + 1 < sql.size() && sql[i + 1] == '"') {
                expanded.push_back(sql[++i]);
            } else if (c == '"') {
                doubleQuoted = false;
            }
            continue;
        }
        if (c == '\'') {
            singleQuoted = true;
            expanded.push_back(c);
            continue;
        }
        if (c == '"') {
            doubleQuoted = true;
            expanded.push_back(c);
            continue;
        }
        if (c != '$' || i + 1 >= sql.size() ||
            !std::isdigit(static_cast<unsigned char>(sql[i + 1]))) {
            expanded.push_back(c);
            continue;
        }
        size_t number = 0;
        size_t end = i + 1;
        while (end < sql.size() && std::isdigit(static_cast<unsigned char>(sql[end]))) {
            const size_t digit = static_cast<size_t>(sql[end] - '0');
            if (number > (std::numeric_limits<size_t>::max() - digit) / 10) {
                error = "parameter number is too large";
                return false;
            }
            number = number * 10 + digit;
            ++end;
        }
        if (number == 0 || number > literals.size()) {
            error = "there is no parameter $" + std::to_string(number);
            return false;
        }
        expanded += literals[number - 1];
        i = end - 1;
    }
    return true;
}

std::string trimText(const std::string& value) {
    size_t first = 0;
    while (first < value.size() && std::isspace(static_cast<unsigned char>(value[first]))) ++first;
    size_t last = value.size();
    while (last > first && std::isspace(static_cast<unsigned char>(value[last - 1]))) --last;
    return value.substr(first, last - first);
}

size_t skipSqlTrivia(const std::string& sql, size_t pos) {
    while (true) {
        while (pos < sql.size() &&
               std::isspace(static_cast<unsigned char>(sql[pos]))) ++pos;
        if (pos + 1 >= sql.size()) return pos;
        if (sql[pos] == '-' && sql[pos + 1] == '-') {
            pos += 2;
            while (pos < sql.size() && sql[pos] != '\n' && sql[pos] != '\r') {
                ++pos;
            }
            continue;
        }
        if (sql[pos] == '/' && sql[pos + 1] == '*') {
            pos += 2;
            size_t depth = 1;
            while (pos < sql.size() && depth != 0) {
                if (pos + 1 < sql.size() && sql[pos] == '/' &&
                    sql[pos + 1] == '*') {
                    ++depth;
                    pos += 2;
                } else if (pos + 1 < sql.size() && sql[pos] == '*' &&
                           sql[pos + 1] == '/') {
                    --depth;
                    pos += 2;
                } else {
                    ++pos;
                }
            }
            if (depth != 0) return std::string::npos;
            continue;
        }
        return pos;
    }
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
    if (!readSqlKeyword(sql, position, keyword)) return {};
    return keyword;
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

std::vector<std::string> leadingSqlKeywords(const std::string& sql,
                                            size_t maximum) {
    std::vector<std::string> keywords;
    size_t position = 0;
    while (keywords.size() < maximum) {
        std::string keyword;
        if (!readSqlKeyword(sql, position, keyword)) break;
        keywords.push_back(std::move(keyword));
    }
    return keywords;
}

std::string protocolRelationFromQuery(const std::string& sql) {
    const std::string lower = lowerProtocolText(sql);
    size_t from = lower.find(" from ");
    if (from == std::string::npos) return {};
    size_t begin = from + 6;
    while (begin < sql.size() && std::isspace(static_cast<unsigned char>(sql[begin]))) ++begin;
    if (begin >= sql.size() || sql[begin] == '(') return {};
    size_t end = begin;
    if (sql[end] == '"') {
        ++end;
        while (end < sql.size()) {
            if (sql[end] == '"' && end + 1 < sql.size() && sql[end + 1] == '"') {
                end += 2;
                continue;
            }
            if (sql[end++] == '"') break;
        }
    } else {
        while (end < sql.size() && !std::isspace(static_cast<unsigned char>(sql[end])) &&
               sql[end] != ',' && sql[end] != ';' && sql[end] != ')') {
            ++end;
        }
    }
    if (end <= begin) return {};
    std::string relation = trimText(sql.substr(begin, end - begin));
    if (relation.size() >= 2 && relation.front() == '"' && relation.back() == '"') {
        relation = relation.substr(1, relation.size() - 2);
    }
    const size_t dot = relation.rfind('.');
    if (dot != std::string::npos && dot + 1 < relation.size()) {
        relation = relation.substr(dot + 1);
    }
    return relation;
}

int16_t protocolTypeSize(uint32_t typeOid, const Column& column) {
    switch (typeOid) {
        case 16: return 1;   // bool
        case 20: return 8;   // int8
        case 21: return 2;   // int2
        case 23: return 4;   // int4
        case 700: return 4;  // float4
        case 701: return 8;  // float8
        case 1082: return 4; // date
        case 1083: return 8; // time
        case 1114: case 1184: return 8; // timestamp/timestamptz
        case 1700: return -1; // numeric
        case 2950: return 16; // uuid
        default: return column.isVariableLength ? -1 : static_cast<int16_t>(column.dsize);
    }
}

std::vector<PgColumnDescription> describeProtocolColumns(const QueryResult& result,
                                                          const std::string& sql,
                                                          const Session& session) {
    std::vector<PgColumnDescription> descriptions;
    descriptions.reserve(result.columns.size());

    const std::string relationName = protocolRelationFromQuery(sql);
    TableSchema table;
    if (!relationName.empty() && g_engine.tableExists(session.currentDB, relationName)) {
        table = g_engine.getTableSchema(session.currentDB, relationName);
    }

    Oid relationOid = INVALID_OID;
    std::vector<PgAttributeRow> catalogAttributes;
    if (!relationName.empty()) {
        auto& catalog = g_engine.catalogService().get(session.currentDB);
        for (const auto& relation : catalog.listClasses()) {
            if (relation.relname == relationName) {
                relationOid = relation.oid;
                catalogAttributes = catalog.findAttributes(relation.oid);
                break;
            }
        }
    }

    for (const auto& name : result.columns) {
        PgColumnDescription description;
        description.name = name;
        const size_t columnIndex = descriptions.size();
        const bool hasStructuredType =
            columnIndex < result.columnTypes.size() &&
            !result.columnTypes[columnIndex].empty();
        if (hasStructuredType) {
            Column expressionColumn;
            expressionColumn.dataType = result.columnTypes[columnIndex];
            expressionColumn.isVariableLength = true;
            description.typeOid = mapBuiltinTypeNameToOid(
                lowerProtocolText(expressionColumn.dataType));
            if (description.typeOid == INVALID_OID) description.typeOid = 25;
            description.typeSize = protocolTypeSize(description.typeOid, expressionColumn);
        }
        for (size_t i = 0; i < table.len; ++i) {
            const Column& column = table.cols[i];
            if (lowerProtocolText(column.dataName) != lowerProtocolText(name)) continue;
            if (!hasStructuredType) {
                description.typeOid = mapBuiltinTypeNameToOid(lowerProtocolText(column.dataType));
                if (description.typeOid == INVALID_OID) description.typeOid = 25;
                description.typeSize = protocolTypeSize(description.typeOid, column);
                description.typeModifier = column.isVariableLength && column.dsize > 0
                                               ? static_cast<int32_t>(column.dsize + 4)
                                               : -1;
            }
            description.tableOid = relationOid;
            description.attributeNumber = static_cast<uint16_t>(i + 1);
            for (const auto& attribute : catalogAttributes) {
                if (attribute.attname == column.dataName) {
                    description.tableOid = relationOid;
                    description.attributeNumber = static_cast<uint16_t>(attribute.attnum);
                    if (!hasStructuredType) {
                        if (attribute.atttypid != INVALID_OID) description.typeOid = attribute.atttypid;
                        if (attribute.attlen != 0) description.typeSize = attribute.attlen;
                        if (attribute.atttypmod >= 0) description.typeModifier = attribute.atttypmod;
                    }
                    break;
                }
            }
            break;
        }
        descriptions.push_back(std::move(description));
    }
    return descriptions;
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
    if (keyword == "begin" || keyword == "start") return "BEGIN";
    if (keyword == "commit" || keyword == "end") return "COMMIT";
    if (keyword == "rollback") return "ROLLBACK";
    if (keyword == "set") return "SET";
    if (keyword == "create" || keyword == "alter" || keyword == "drop" ||
        keyword == "truncate" || keyword == "grant" || keyword == "revoke") {
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
// PG 42883 pre-check: an unknown function referenced in a WHERE
// clause is an immediate error ("function nosuchfn(integer) does
// not exist").  Scans the statement's WHERE region for fn(...) -
// shaped tokens; when the name is not a known scalar/aggregate and
// not a column of the FROM table, the first argument's type is
// resolved from the table schema (int -> integer, text/varchar ->
// text, else unknown) and PG's exact message is returned.  Empty
// string means no violation found.
std::string whereUnknownFunctionError(const std::string& sql,
                                       const std::string& dbname) {
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
        "unnest", "array_lower", "array_upper", "array_length", "cardinality",
        "row_number", "rank", "dense_rank", "ntile", "lag", "lead",
        "first_value", "last_value", "nth_value"
    };
    std::string low;
    for (char c : sql) low += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    std::vector<bool> quoted(sql.size(), false);
    char activeQuote = '\0';
    for (size_t i = 0; i < sql.size(); ++i) {
        const char c = sql[i];
        if (activeQuote != '\0') {
            quoted[i] = true;
            if (c == activeQuote) {
                if (i + 1 < sql.size() && sql[i + 1] == activeQuote) {
                    quoted[++i] = true;
                } else {
                    activeQuote = '\0';
                }
            }
        } else if (c == '\'' || c == '"') {
            activeQuote = c;
            quoted[i] = true;
        }
    }
    size_t wpos = low.find(" where ");
    if (wpos == std::string::npos) return "";
    size_t wend = low.size();
    for (size_t kw = wpos; kw + 1 < low.size(); ++kw) {
        if (low.compare(kw, 8, " group b") == 0 ||
            low.compare(kw, 8, " order b") == 0 ||
            low.compare(kw, 7, " limit ") == 0)
            { wend = kw; break; }
    }
    // FROM table for type resolution (first table after ' from ').
    std::string table;
    {
        size_t fpos = low.find(" from ");
        if (fpos != std::string::npos && fpos < wpos) {
            size_t rest = fpos + 6;
            while (rest < low.size() && std::isspace(static_cast<unsigned char>(low[rest]))) ++rest;
            size_t tend = rest;
            while (tend < low.size() &&
                   (std::isalnum(static_cast<unsigned char>(low[tend])) || low[tend] == '_')) ++tend;
            table = sql.substr(rest, tend - rest);
        }
    }
    for (size_t p = wpos + 7; p < wend; ++p) {
        if (quoted[p]) continue;
        if (!std::isalpha(static_cast<unsigned char>(low[p])) && low[p] != '_') continue;
        size_t start = p;
        while (p < wend && (std::isalnum(static_cast<unsigned char>(low[p])) || low[p] == '_')) ++p;
        size_t np = p;
        while (np < wend && std::isspace(static_cast<unsigned char>(low[np]))) ++np;
        if (np >= wend || low[np] != '(') continue;
        std::string fn = low.substr(start, p - start);
        // IN / EXISTS / ANY / ALL are predicate keywords, not calls.
        static const char* kw47[] = { "in", "not", "and", "or", "exists",
                                      "any", "all", "between", "like",
                                      "isnull", "notnull", "case", "when",
                                      "then", "else", "end", "null", "true",
                                      "false", "cast", "interval" };
        bool isKw = false;
        for (const char* k : kw47)
            if (fn == k) { isKw = true; break; }
        if (isKw) continue;
        bool isKnown = false;
        for (const char* k : known)
            if (fn == k) { isKnown = true; break; }
        if (isKnown) continue;
        // Unknown name called as a function: resolve first arg type.
        size_t argStart = np + 1;
        while (argStart < wend && std::isspace(static_cast<unsigned char>(low[argStart]))) ++argStart;
        size_t argEnd = argStart;
        while (argEnd < wend &&
               (std::isalnum(static_cast<unsigned char>(low[argEnd])) || low[argEnd] == '_')) ++argEnd;
        std::string arg = low.substr(argStart, argEnd - argStart);
        std::string type = "unknown";
        if (!arg.empty() && !table.empty()) {
            dbms::StorageEngine& eng = g_engine;
            dbms::TableSchema sch = eng.getTableSchema(dbname, table);
            for (size_t ci = 0; ci < sch.len; ++ci) {
                if (sch.cols[ci].dataName != arg) continue;
                std::string dt = sch.cols[ci].dataType;
                for (auto& ch : dt) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                if (dt.find("int") != std::string::npos) type = "integer";
                else if (dt.find("varchar") != std::string::npos ||
                         dt.find("character varying") != std::string::npos)
                    type = "character varying";
                else if (dt.find("char") != std::string::npos)
                    type = "character";
                else if (dt.find("text") != std::string::npos) type = "text";
                else if (dt.find("numeric") != std::string::npos || dt.find("decimal") != std::string::npos)
                    type = "numeric";
                break;
            }
        }
        return "function " + fn + "(" + type + ") does not exist";
    }
    return "";
}
}  // namespace

QueryResult executeProtocolQuery(const std::string& sql, Session& session) {
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

    bool executionError = false;
    bool structuredError = false;
    std::string outputText;
    // PG 42883: unknown function in WHERE fails before execution.
    {
        std::string ufErr = whereUnknownFunctionError(sql, session.currentDB);
        if (!ufErr.empty()) {
            result.error = true;
            result.errorMessage = ufErr;
            result.sqlState = "42883";
            return result;
        }
    }
    auto start = std::chrono::steady_clock::now();
    {
        std::ostringstream output;
        dbms::ScopedOutputCapture capture(output);
        try {
            executionError = execute(sql, session);
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
    if (executionError || (!lines.empty() && lines.front().rfind("ERROR:", 0) == 0)) {
        result.error = true;
        if (result.errorMessage.empty()) {
            if (lines.empty()) {
                result.errorMessage = "query failed";
            } else {
                std::string msg = lines.front();
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
        if (structuredError) {
            // The executor's explicit code takes precedence over all legacy
            // wording heuristics, even if the message happens to match one.
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
        } else if (result.errorMessage.find("syntax") != std::string::npos) {
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
        } else {
            result.sqlState = "XX000";
        }
        dbms::recordQueryExecution(sql, elapsedMs, session.currentDB, false);
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
                !structuredDml.commandTag.empty() &&
                (keyword == "insert" || keyword == "update" ||
                 keyword == "delete" || keyword == "merge")
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
    const std::vector<std::string> keywords = leadingSqlKeywords(sql, 5);
    if (keywords.empty() ||
        (keywords.front() != "commit" && keywords.front() != "end")) {
        const size_t start = sqlCommandOffset(sql);
        return start == std::string::npos ? sql : sql.substr(start);
    }
    size_t option = 1;
    if (option < keywords.size() &&
        (keywords[option] == "work" || keywords[option] == "transaction")) {
        ++option;
    }
    if (option < keywords.size() && keywords[option] == "and") {
        ++option;
        bool noChain = false;
        if (option < keywords.size() && keywords[option] == "no") {
            noChain = true;
            ++option;
        }
        if (option < keywords.size() && keywords[option] == "chain") {
            return noChain ? "ROLLBACK AND NO CHAIN" : "ROLLBACK AND CHAIN";
        }
    }
    return "ROLLBACK";
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
                              bool allowPlaintext, SecureSocket& socket) {
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
    if (length == 16 && code == cancelRequestCode) return false;
    if (tlsContext.enabled()) return false;
    if (!allowPlaintext) return false;
    socket = SecureSocket(clientFd);
    return true;
}

void sendQueryResult(PostgresProtocol& protocol, const QueryResult& result,
                     char transactionStatus) {
    if (result.error) {
        protocol.sendErrorResponse("ERROR", result.sqlState, trimText(result.errorMessage));
        protocol.sendReadyForQuery(transactionStatus);
        return;
    }
    if (result.commandTag.empty() && !result.resultSet) {
        protocol.sendEmptyQueryResponse();
        protocol.sendReadyForQuery(transactionStatus);
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
    protocol.sendCommandComplete(result.commandTag);
    protocol.sendReadyForQuery(transactionStatus);
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
        startup.parameters.count("database") ? startup.parameters.at("database") : "info",
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

    Session session;
    session.compatibilityMode = dbms::defaultCompatibilityMode();
    struct BackendSessionGuard {
        Session* session;
        ~BackendSessionGuard() {
            if (session) {
                notificationManager().disconnect(session->pid);
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
            g_engine.endBackendSession();
        }
    } backendSessionGuard{&session};
    session.username = username;
    session.permission = permissionQuery(username);
    session.authenticatedUser = username;
    session.authenticatedPermission = session.permission;
    auto dbIt = startup.parameters.find("database");
    session.currentDB = (dbIt == startup.parameters.end() || dbIt->second.empty())
                            ? "info" : dbIt->second;
    session.originalRole = username;
    session.statementTimeoutMs = g_config.statementTimeoutMs;
    session.defaultStatementTimeoutMs = g_config.statementTimeoutMs;
    session.lockTimeoutMs = g_config.lockTimeoutMs;
    session.deadlockTimeoutMs = g_config.deadlockTimeoutMs;
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

    const uint64_t pid = registerProcess(username, clientHost, session.currentDB);
    session.pid = pid;
    if (!protocol.sendParameterStatus("server_version", "DBMS-C++ protocol/3.0") ||
        !protocol.sendParameterStatus("client_encoding", "UTF8") ||
        !protocol.sendParameterStatus("DateStyle", "ISO, MDY") ||
        !protocol.sendParameterStatus("integer_datetimes", "on") ||
        !protocol.sendParameterStatus("standard_conforming_strings", "on") ||
        !protocol.sendParameterStatus("TimeZone", "UTC") ||
        !protocol.sendBackendKeyData(static_cast<uint32_t>(pid), static_cast<uint32_t>(pid ^ 0x9e3779b9U)) ||
        !protocol.sendReadyForQuery()) {
        unregisterProcess(pid);
        return;
    }

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
    // Connection-local extended-query state. In transaction/statement pool
    // modes these do not travel across backend rentals (PgBouncer has the
    // same restriction for session-level features).
    std::map<std::string, ProtocolPreparedStatement> preparedStatements;
    std::map<std::string, ProtocolPortal> portals;
    const auto readyStatus = [&]() -> char {
        if (transactionFailed) return 'E';
        return g_engine.inTransaction() ? 'T' : 'I';
    };
    const auto executeForProtocol = [&](const std::string& sql) -> QueryResult {
        if (transactionFailed && !isTransactionRecoveryCommand(sql)) {
            return transactionAbortedResult();
        }
        const bool wasInTransaction = g_engine.inTransaction();
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
        QueryResult result = executeProtocolQuery(effectiveSql, *execSession);
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
            transactionFailed = true;
        } else if (!result.error && isTransactionRecoveryCommand(sql)) {
            transactionFailed = false;
        }
        return result;
    };
    while (true) {
        PgFrontendMessage message;
        if (!protocol.readMessage(message, protocolError)) break;
        if (message.type == 'X') break;
        if (extendedQueryError && message.type != 'S') continue;
        if (message.type == 'Q') {
            std::string sql = messageCString(message);
            if (sql.empty() && message.payload.size() != 1) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Query message");
                protocol.sendReadyForQuery('E');
                continue;
            }
            updateProcessInfo(pid, "Query", "executing", trimText(sql));
            QueryResult result = executeForProtocol(sql);
            updateProcessDb(pid, session.currentDB);
            updateProcessInfo(pid, "Idle", "", "");
            sendQueryResult(protocol, result, readyStatus());
            continue;
        }
        if (message.type == 'P') {
            size_t offset = 0;
            std::string statement;
            std::string sql;
            if (!PostgresProtocol::readCString(message.payload, offset, statement) ||
                !PostgresProtocol::readCString(message.payload, offset, sql) ||
                offset + 2 > message.payload.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Parse message");
                extendedQueryError = true;
                continue;
            }
            const uint16_t parameterCount = PostgresProtocol::readUInt16(message.payload, offset);
            offset += 2;
            if (parameterCount > 1024 ||
                message.payload.size() - offset != static_cast<size_t>(parameterCount) * 4) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Parse parameter type list");
                extendedQueryError = true;
                continue;
            }
            ProtocolPreparedStatement prepared;
            prepared.sql = std::move(sql);
            prepared.parameterTypes.reserve(parameterCount);
            for (uint16_t i = 0; i < parameterCount; ++i) {
                prepared.parameterTypes.push_back(
                    PostgresProtocol::readUInt32(message.payload, offset));
                offset += 4;
            }
            preparedStatements[statement] = std::move(prepared);
            if (!protocol.sendParameterDescription(preparedStatements[statement].parameterTypes)) {
                extendedQueryError = true;
                continue;
            }
            protocol.sendParseComplete();
            continue;
        }
        if (message.type == 'B') {
            size_t offset = 0;
            std::string portal;
            std::string statement;
            if (!PostgresProtocol::readCString(message.payload, offset, portal) ||
                !PostgresProtocol::readCString(message.payload, offset, statement) ||
                preparedStatements.find(statement) == preparedStatements.end()) {
                protocol.sendErrorResponse("ERROR", "26000", "prepared statement does not exist");
                extendedQueryError = true;
                continue;
            }
            const auto& prepared = preparedStatements.at(statement);
            if (offset + 2 > message.payload.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Bind message");
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
                    protocol.sendErrorResponse("ERROR", "08P01", "malformed Bind format list");
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
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Bind parameter count");
                extendedQueryError = true;
                continue;
            }
            const uint16_t valueCount = PostgresProtocol::readUInt16(message.payload, offset);
            offset += 2;
            if (valueCount != prepared.parameterTypes.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "bind message supplies a different number of parameters");
                extendedQueryError = true;
                continue;
            }
            if (!parameterFormats.empty() && parameterFormats.size() != 1 &&
                parameterFormats.size() != valueCount) {
                protocol.sendErrorResponse("ERROR", "08P01", "bind message has an invalid parameter format count");
                extendedQueryError = true;
                continue;
            }
            std::vector<std::string> literals;
            literals.reserve(valueCount);
            bool bindError = false;
            std::string bindErrorMessage;
            for (uint16_t i = 0; i < valueCount; ++i) {
                if (offset + 4 > message.payload.size()) {
                    bindError = true;
                    bindErrorMessage = "malformed Bind parameter value";
                    break;
                }
                const int32_t valueLength = PostgresProtocol::readInt32(message.payload, offset);
                offset += 4;
                if (valueLength < -1 ||
                    (valueLength >= 0 &&
                     static_cast<size_t>(valueLength) > message.payload.size() - offset)) {
                    bindError = true;
                    bindErrorMessage = "malformed Bind parameter value length";
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
                        prepared.parameterTypes[i], raw, format == 1, bindErrorMessage);
                    if (!bindErrorMessage.empty()) {
                        bindError = true;
                        break;
                    }
                    literals.push_back(std::move(literal));
                }
            }
            if (bindError) {
                protocol.sendErrorResponse("ERROR", "0A000", bindErrorMessage);
                extendedQueryError = true;
                continue;
            }
            if (offset + 2 > message.payload.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Bind result format count");
                extendedQueryError = true;
                continue;
            }
            const uint16_t resultFormatCount = PostgresProtocol::readUInt16(message.payload, offset);
            offset += 2;
            if (offset + static_cast<size_t>(resultFormatCount) * 2 != message.payload.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Bind result format list");
                extendedQueryError = true;
                continue;
            }
            std::vector<uint16_t> resultFormats;
            resultFormats.reserve(resultFormatCount);
            for (uint16_t i = 0; i < resultFormatCount; ++i) {
                const uint16_t format = PostgresProtocol::readUInt16(message.payload, offset);
                offset += 2;
                if (format > 1) {
                    protocol.sendErrorResponse("ERROR", "08P01", "unsupported result format code");
                    extendedQueryError = true;
                    resultFormats.clear();
                    break;
                }
                resultFormats.push_back(format);
            }
            if (extendedQueryError) continue;
            std::string expandedSql;
            std::string substitutionError;
            if (!substituteProtocolParameters(prepared.sql, literals, expandedSql, substitutionError)) {
                protocol.sendErrorResponse("ERROR", "42P02", substitutionError);
                extendedQueryError = true;
                continue;
            }
            portals[portal] = ProtocolPortal{statement, std::move(expandedSql), std::move(resultFormats)};
            protocol.sendBindComplete();
            continue;
        }
        if (message.type == 'E') {
            size_t offset = 0;
            std::string portal;
            if (!PostgresProtocol::readCString(message.payload, offset, portal)) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Execute message");
                extendedQueryError = true;
                continue;
            }
            if (offset + 4 != message.payload.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Execute max-rows field");
                extendedQueryError = true;
                continue;
            }
            const int32_t maxRows = PostgresProtocol::readInt32(message.payload, offset);
            if (maxRows < 0) {
                protocol.sendErrorResponse("ERROR", "08P01", "negative portal row limit");
                extendedQueryError = true;
                continue;
            }
            auto portalIt = portals.find(portal);
            if (portalIt == portals.end()) {
                protocol.sendErrorResponse("ERROR", "34000", "portal does not exist");
                extendedQueryError = true;
                continue;
            }
            auto& portalState = portalIt->second;
            if (!portalState.executed) {
                portalState.result = executeForProtocol(portalState.sql);
                portalState.executed = true;
            }
            QueryResult& result = portalState.result;
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
                    protocol.sendErrorResponse("ERROR", "08P01", "result format count does not match result columns");
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
                if (!portalState.rowDescriptionSent) {
                    if (!protocol.sendRowDescription(columns)) {
                        extendedQueryError = true;
                        continue;
                    }
                    portalState.rowDescriptionSent = true;
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
                        protocol.sendErrorResponse("ERROR", "0A000", "binary result type is not supported");
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
                protocol.sendCommandComplete(result.commandTag);
            }
            continue;
        }
        if (message.type == 'D') {
            if (message.payload.empty()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Describe message");
                extendedQueryError = true;
                continue;
            }
            const char target = static_cast<char>(message.payload[0]);
            size_t offset = 1;
            std::string name;
            if (!PostgresProtocol::readCString(message.payload, offset, name) ||
                offset != message.payload.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Describe message");
                extendedQueryError = true;
                continue;
            }
            if (target == 'S') {
                auto statementIt = preparedStatements.find(name);
                if (statementIt == preparedStatements.end()) {
                    protocol.sendErrorResponse("ERROR", "26000", "prepared statement does not exist");
                    extendedQueryError = true;
                    continue;
                }
                if (!protocol.sendParameterDescription(statementIt->second.parameterTypes) ||
                    !protocol.sendNoData()) {
                    extendedQueryError = true;
                }
                continue;
            }
            if (target == 'P') {
                if (portals.find(name) == portals.end()) {
                    protocol.sendErrorResponse("ERROR", "34000", "portal does not exist");
                    extendedQueryError = true;
                    continue;
                }
                protocol.sendNoData();
                continue;
            }
            protocol.sendErrorResponse("ERROR", "08P01", "invalid Describe target");
            extendedQueryError = true;
            continue;
        }
        if (message.type == 'C') {
            if (message.payload.empty()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Close message");
                extendedQueryError = true;
                continue;
            }
            const char target = static_cast<char>(message.payload[0]);
            size_t offset = 1;
            std::string name;
            if (!PostgresProtocol::readCString(message.payload, offset, name) ||
                offset != message.payload.size()) {
                protocol.sendErrorResponse("ERROR", "08P01", "malformed Close message");
                extendedQueryError = true;
                continue;
            }
            if (target == 'S') {
                if (preparedStatements.erase(name) == 0) {
                    protocol.sendErrorResponse("ERROR", "26000", "prepared statement does not exist");
                    extendedQueryError = true;
                    continue;
                }
                for (auto portalIt = portals.begin(); portalIt != portals.end();) {
                    if (portalIt->second.statement == name) portalIt = portals.erase(portalIt);
                    else ++portalIt;
                }
            } else if (target == 'P') {
                if (portals.erase(name) == 0) {
                    protocol.sendErrorResponse("ERROR", "34000", "portal does not exist");
                    extendedQueryError = true;
                    continue;
                }
            } else {
                protocol.sendErrorResponse("ERROR", "08P01", "invalid Close target");
                extendedQueryError = true;
                continue;
            }
            protocol.sendCloseComplete();
            continue;
        }
        if (message.type == 'S') {
            protocol.sendReadyForQuery(readyStatus());
            extendedQueryError = false;
            continue;
        }
        if (message.type == 'H') continue;
        protocol.sendErrorResponse("ERROR", "08P01", "unsupported frontend message");
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
                if (!establishClientTransport(clientFd, tlsCtx, allowPlaintext, socket)) {
                    if (socket.fd < 0) ::close(clientFd);
                    std::cerr << "client transport negotiation failed" << std::endl;
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
