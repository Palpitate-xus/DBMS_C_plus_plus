#include "NetworkServer.h"
#include "TableManage.h"
#include "permissions.h"
#include "PostgresProtocol.h"
#include "Session.h"
#include "TLSWrapper.h"
#include "utils/pg_hba.h"
#include "common/scram_sha256.h"
#include "catalog/CatalogService.h"

#include <arpa/inet.h>
#include <cctype>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
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
extern void recordSqlStat(const std::string& sql, double ms, const std::string& dbname);

namespace dbms {

static ServerStats g_stats;

// Process list: active connections
static std::mutex g_processMutex;
static std::map<uint64_t, ProcessInfo> g_processList;
static uint64_t g_nextProcessId = 1;
static std::mutex g_outputCaptureMutex;
static std::mutex g_roleConnectionMutex;
static std::unordered_map<std::string, int> g_roleConnections;

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
    std::vector<std::vector<std::string>> rows;
    std::string commandTag;
};

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

std::string commandTagFor(const std::string& sql, const std::vector<std::string>& lines,
                          size_t rowCount) {
    std::string keyword = firstSqlKeyword(sql);
    if (keyword == "insert") {
        for (const auto& line : lines) {
            if (line.rfind("INSERT ", 0) == 0) return line;
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
    std::string trimmed = trimText(sql);
    if (trimmed.empty()) {
        result.commandTag.clear();
        return result;
    }

    bool executionError = false;
    std::string outputText;
    auto start = std::chrono::steady_clock::now();
    {
        // execute() still writes through std::cout. Serializing this legacy
        // boundary prevents concurrent sessions from corrupting each other's
        // result capture while the executor is migrated to structured output.
        std::lock_guard<std::mutex> lock(g_outputCaptureMutex);
        auto* oldBuffer = std::cout.rdbuf();
        std::ostringstream output;
        std::cout.rdbuf(output.rdbuf());
        try {
            executionError = execute(sql, session);
        } catch (const std::exception& e) {
            executionError = true;
            result.errorMessage = e.what();
        } catch (...) {
            executionError = true;
            result.errorMessage = "unhandled executor exception";
        }
        std::cout.rdbuf(oldBuffer);
        outputText = output.str();
    }
    auto end = std::chrono::steady_clock::now();
    double elapsedMs = std::chrono::duration<double, std::milli>(end - start).count();
    if (elapsedMs > g_slowQueryThresholdMs) {
        logSlowQuery(sql, elapsedMs, session.username, session.currentDB);
    }
    recordSqlStat(sql, elapsedMs, session.currentDB);

    auto lines = outputLines(outputText);
    if (executionError || (!lines.empty() && lines.front().rfind("ERROR:", 0) == 0)) {
        result.error = true;
        if (result.errorMessage.empty()) {
            result.errorMessage = lines.empty() ? "query failed" : lines.front().substr(6);
        }
        result.sqlState = (result.errorMessage.find("syntax") != std::string::npos)
                              ? "42601" : "XX000";
        return result;
    }

    const std::string keyword = firstSqlKeyword(sql);
    result.resultSet = keyword == "select" || keyword == "show" || keyword == "values" ||
                       keyword == "explain";
    if (result.resultSet && !lines.empty()) {
        if (keyword == "explain") {
            result.columns = {"QUERY PLAN"};
            for (const auto& line : lines) result.rows.push_back({line});
        } else {
            result.columns = splitProtocolFields(lines.front());
            for (size_t i = 1; i < lines.size(); ++i) {
                result.rows.push_back(splitProtocolFields(lines[i]));
            }
        }
    }
    result.commandTag = commandTagFor(sql, lines, result.rows.size());
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
        std::vector<PgColumnDescription> columns;
        columns.reserve(result.columns.size());
        for (const auto& name : result.columns) {
            PgColumnDescription column;
            column.name = name;
            columns.push_back(std::move(column));
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
            return userHasRole(member, role);
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
        ~BackendSessionGuard() { g_engine.endBackendSession(); }
    } backendSessionGuard;
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

    std::map<std::string, std::string> portals;
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
            if (parameterCount != 0) {
                protocol.sendErrorResponse("ERROR", "0A000", "parameters are not yet supported");
                extendedQueryError = true;
                continue;
            }
            session.preparedStmts[statement] = sql;
            protocol.sendParseComplete();
            continue;
        }
        if (message.type == 'B') {
            size_t offset = 0;
            std::string portal;
            std::string statement;
            if (!PostgresProtocol::readCString(message.payload, offset, portal) ||
                !PostgresProtocol::readCString(message.payload, offset, statement) ||
                session.preparedStmts.find(statement) == session.preparedStmts.end()) {
                protocol.sendErrorResponse("ERROR", "26000", "prepared statement does not exist");
                extendedQueryError = true;
                continue;
            }
            portals[portal] = session.preparedStmts[statement];
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
            auto portalIt = portals.find(portal);
            if (portalIt == portals.end()) {
                protocol.sendErrorResponse("ERROR", "34000", "portal does not exist");
                extendedQueryError = true;
                continue;
            }
            QueryResult result = executeForProtocol(portalIt->second);
            if (result.error) {
                protocol.sendErrorResponse("ERROR", result.sqlState, result.errorMessage);
                extendedQueryError = true;
            }
            else if (result.resultSet) {
                std::vector<PgColumnDescription> columns;
                for (const auto& name : result.columns) columns.push_back(PgColumnDescription{name});
                protocol.sendRowDescription(columns);
                for (const auto& row : result.rows) protocol.sendDataRow(row);
                protocol.sendCommandComplete(result.commandTag);
            } else {
                protocol.sendCommandComplete(result.commandTag);
            }
            continue;
        }
        if (message.type == 'D') {
            protocol.sendNoData();
            continue;
        }
        if (message.type == 'C') {
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

void startServer(int port, bool allowPlaintext) {
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
        return;
    }

    if (!isServerTransportAllowed(tlsCtx.enabled(), allowPlaintext)) {
        std::cerr << "TLS is unavailable; refusing to start without --insecure" << std::endl;
        return;
    }

    int serverFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return;
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
        return;
    }

    if (::listen(serverFd, 10) < 0) {
        std::cerr << "Failed to listen" << std::endl;
        ::close(serverFd);
        return;
    }

    std::cout << "DBMS server listening on port " << port;
    if (tlsCtx.enabled()) {
        std::cout << " (TLS enabled)";
    } else {
        std::cout << " (INSECURE plaintext; explicitly enabled)";
    }
    std::cout << std::endl;

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = ::accept(serverFd, reinterpret_cast<sockaddr*>(&clientAddr), &clientLen);
        if (clientFd < 0) continue;

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

        std::thread([clientFd, &tlsCtx, allowPlaintext, clientHost]() {
            struct SlotGuard {
                ~SlotGuard() { releaseConnectionSlot(); }
            } slotGuard;
            SecureSocket socket;
            if (!establishClientTransport(clientFd, tlsCtx, allowPlaintext, socket)) {
                std::cerr << "client transport negotiation failed" << std::endl;
                return;
            }
            handleClient(std::move(socket), clientHost);
        }).detach();
    }

    ::close(serverFd);
}

} // namespace dbms
