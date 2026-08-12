#include "NetworkServer.h"
#include "commands/DmlExecutor.h"
#include "TableManage.h"
#include "permissions.h"
#include "PostgresProtocol.h"
#include "Session.h"
#include "TLSWrapper.h"
#include "utils/pg_hba.h"
#include "common/scram_sha256.h"
#include "catalog/CatalogService.h"
#include "catalog/systables.h"
#include "common/DateType.h"
#include "PostgresNumeric.h"
#include "process/SqlStats.h"
#include "process/RuntimeStats.h"
#include "process/OutputCapture.h"

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

std::string firstSqlKeyword(const std::string& sql) {
    std::string trimmed = trimText(sql);
    size_t end = 0;
    while (end < trimmed.size() && !std::isspace(static_cast<unsigned char>(trimmed[end]))) ++end;
    std::string keyword = trimmed.substr(0, end);
    for (char& c : keyword) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return keyword;
}

std::vector<std::string> splitProtocolFields(const std::string& line) {
    std::vector<std::string> fields;
    std::istringstream input(line);
    std::string field;
    while (input >> field) fields.push_back(std::move(field));
    return fields;
}

std::vector<std::string> outputLines(const std::string& output) {
    std::vector<std::string> lines;
    std::istringstream input(output);
    std::string line;
    while (std::getline(input, line)) {
        line = trimText(line);
        if (!line.empty()) lines.push_back(std::move(line));
    }
    return lines;
}

std::string lowerProtocolText(std::string value) {
    for (char& c : value) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return value;
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
        }
        return "UPDATE 0";
    }
    if (keyword == "delete") {
        for (const auto& line : lines) {
            if (line.rfind("Delete done (", 0) == 0) return "DELETE 0";
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

QueryResult executeProtocolQuery(const std::string& sql, Session& session) {
    QueryResult result;
    dbms::clearLastDmlResult();
    std::string trimmed = trimText(sql);
    if (trimmed.empty()) {
        result.commandTag.clear();
        return result;
    }

    bool executionError = false;
    std::string outputText;
    auto start = std::chrono::steady_clock::now();
    {
        std::ostringstream output;
        dbms::ScopedOutputCapture capture(output);
        try {
            executionError = execute(sql, session);
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
            result.errorMessage = lines.empty() ? "query failed" : lines.front().substr(6);
        }
        if (result.errorMessage.find("syntax") != std::string::npos) {
            result.sqlState = "42601";
        } else if (result.errorMessage.find(
                       "more than one row returned by a subquery used as an expression")
                   != std::string::npos) {
            result.sqlState = "21000";
        } else {
            result.sqlState = "XX000";
        }
        dbms::recordQueryExecution(sql, elapsedMs, session.currentDB, false);
        return result;
    }

    if (structuredDml.available) {
        result.resultSet = true;
        result.columns = structuredDml.columns;
        result.columnTypes = structuredDml.columnTypes;
        result.rows = structuredDml.rows;
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
            for (size_t i = 1; i < lines.size(); ++i) {
                // The legacy executor prints one column as the complete line.
                // Splitting it on whitespace corrupts timestamp/time-like
                // values that legitimately contain spaces.
                if (result.columns.size() == 1) result.rows.push_back({lines[i]});
                else result.rows.push_back(splitProtocolFields(lines[i]));
            }
        }
        result.columnDescriptions = describeProtocolColumns(result, sql, session);
    }
    result.commandTag = commandTagFor(sql, lines, result.rows.size());
    dbms::recordQueryExecution(sql, elapsedMs, session.currentDB, true,
                               result.rows.size());
    return result;
}

bool isTransactionRecoveryCommand(const std::string& sql) {
    const std::string keyword = firstSqlKeyword(sql);
    return keyword == "commit" || keyword == "end" || keyword == "rollback";
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
        for (const auto& row : result.rows) {
            std::vector<std::string> normalized = row;
            normalized.resize(result.columns.size());
            protocol.sendDataRow(normalized);
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
    struct BackendSessionGuard {
        Session* session;
        ~BackendSessionGuard() {
            if (session) {
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

    std::map<std::string, ProtocolPreparedStatement> preparedStatements;
    std::map<std::string, ProtocolPortal> portals;
    bool transactionFailed = false;
    bool extendedQueryError = false;
    const auto readyStatus = [&]() -> char {
        if (transactionFailed) return 'E';
        return g_engine.inTransaction() ? 'T' : 'I';
    };
    const auto executeForProtocol = [&](const std::string& sql) -> QueryResult {
        if (transactionFailed && !isTransactionRecoveryCommand(sql)) {
            return transactionAbortedResult();
        }
        const bool wasInTransaction = g_engine.inTransaction();
        QueryResult result = executeProtocolQuery(sql, session);
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
                    const auto& row = result.rows[portalState.rowOffset + i];
                    std::vector<std::string> normalized = row;
                    normalized.resize(columns.size());
                    if (!protocol.sendDataRow(normalized, columns)) {
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

    std::cout << "DBMS server listening on port " << port;
    if (tlsCtx.enabled()) {
        std::cout << " (TLS enabled)";
    } else {
        std::cout << " (INSECURE plaintext; explicitly enabled)";
    }
    std::cout << std::endl;

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

        std::string clientHost = inet_ntoa(clientAddr.sin_addr);
        clientHost += ":" + std::to_string(ntohs(clientAddr.sin_port));

        registerClientFd(clientFd);
        auto done = std::make_shared<std::atomic<bool>>(false);
        try {
            auto worker = std::make_unique<Worker>();
            worker->done = done;
            worker->thread = std::thread([clientFd, &tlsCtx, allowPlaintext, clientHost, done]() {
                struct SlotGuard {
                    ~SlotGuard() { releaseConnectionSlot(); }
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
    return acceptLoopHealthy;
}

} // namespace dbms
