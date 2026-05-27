#include "NetworkServer.h"
#include "TableManage.h"
#include "permissions.h"
#include "Session.h"
#include "TLSWrapper.h"

#include <arpa/inet.h>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

// External globals from main.cpp
extern dbms::StorageEngine g_engine;

// Forward declare execute() and logSlowQuery() from main.cpp
extern bool execute(const std::string& rawSql, Session& s);
extern double g_slowQueryThresholdMs;
extern int g_checkpointInterval;
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

static void sendLine(SecureSocket& sock, const std::string& msg) {
    std::string line = msg + "\n";
    size_t sent = 0;
    while (sent < line.size()) {
        ssize_t n = sock.send(line.data() + sent, line.size() - sent);
        if (n <= 0) break;
        sent += static_cast<size_t>(n);
    }
}

static std::string recvLine(SecureSocket& sock) {
    std::string line;
    char ch;
    while (true) {
        ssize_t n = sock.recv(&ch, 1);
        if (n <= 0) return line;
        if (ch == '\n') break;
        if (ch != '\r') line.push_back(ch);
    }
    return line;
}

static void handleClient(SecureSocket sock, std::string clientHost) {
    g_stats.activeConnections++;
    g_stats.totalConnections++;

    Session s;

    // Login phase
    sendLine(sock, "login");
    std::string creds = recvLine(sock);
    std::stringstream ss(creds);
    std::string username, password;
    ss >> username >> password;

    if (!login(username, password)) {
        sendLine(sock, "wrong username or password");
        sock.close();
        g_stats.activeConnections--;
        return;
    }

    s.username = username;
    s.permission = permissionQuery(username);
    s.currentDB = "info";
    sendLine(sock, "successfully login");

    // Register in process list
    uint64_t pid = registerProcess(username, clientHost, s.currentDB);

    // SQL execution loop
    int sqlCount = 0;
    while (true) {
        std::string sql = recvLine(sock);
        if (sql.empty()) break;
        std::string trimmed;
        {
            size_t a = 0;
            while (a < sql.size() && std::isspace(static_cast<unsigned char>(sql[a]))) ++a;
            size_t b = sql.size();
            while (b > a && std::isspace(static_cast<unsigned char>(sql[b - 1]))) --b;
            trimmed = sql.substr(a, b - a);
        }
        if (trimmed == "exit") break;

        // Update process info before execution
        updateProcessInfo(pid, "Query", "executing", trimmed);

        auto start = std::chrono::steady_clock::now();

        // Redirect cout to capture output
        auto* oldBuf = std::cout.rdbuf();
        std::stringstream output;
        std::cout.rdbuf(output.rdbuf());
        execute(sql, s);
        std::cout.rdbuf(oldBuf);

        auto end = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        if (ms > g_slowQueryThresholdMs) logSlowQuery(sql, ms, s.username, s.currentDB);
        recordSqlStat(sql, ms, s.currentDB);

        // Update DB if USE DATABASE was executed
        updateProcessDb(pid, s.currentDB);

        // Mark process as idle after execution
        updateProcessInfo(pid, "Sleep", "", "");

        // Auto checkpoint
        if (g_checkpointInterval > 0 && !s.currentDB.empty()) {
            if (++sqlCount >= g_checkpointInterval) {
                dbms::StorageEngine engine;
                engine.checkpoint(s.currentDB);
                sqlCount = 0;
            }
        }

        std::string result = output.str();
        // Send result back to client
        size_t pos = 0;
        while (pos < result.size()) {
            size_t nl = result.find('\n', pos);
            if (nl == std::string::npos) {
                sendLine(sock, result.substr(pos));
                break;
            }
            sendLine(sock, result.substr(pos, nl - pos));
            pos = nl + 1;
        }
    }

    unregisterProcess(pid);
    sock.close();
    g_stats.activeConnections--;
}

void startServer(int port) {
    // Initialize TLS context
    TLSServerContext tlsCtx;
    std::string certFile = "server.crt";
    std::string keyFile = "server.key";
    if (!std::filesystem::exists(certFile) || !std::filesystem::exists(keyFile)) {
        std::cout << "Generating self-signed TLS certificate..." << std::endl;
        if (!TLSServerContext::generateSelfSignedCert(certFile, keyFile)) {
            std::cerr << "Warning: failed to generate TLS certificate, running without encryption" << std::endl;
        }
    }
    if (std::filesystem::exists(certFile) && std::filesystem::exists(keyFile)) {
        if (tlsCtx.init(certFile, keyFile)) {
            std::cout << "TLS encryption enabled (certificate: " << certFile << ")" << std::endl;
        } else {
            std::cerr << "Warning: TLS initialization failed, running without encryption" << std::endl;
        }
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
        std::cout << " (plaintext - no TLS)";
    }
    std::cout << std::endl;

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = ::accept(serverFd, reinterpret_cast<sockaddr*>(&clientAddr), &clientLen);
        if (clientFd < 0) continue;

        if (g_stats.activeConnections >= g_stats.maxConnections.load()) {
            SecureSocket tmp(clientFd);
            sendLine(tmp, "too many connections");
            tmp.close();
            g_stats.rejectedConnections++;
            continue;
        }

        std::string clientHost = inet_ntoa(clientAddr.sin_addr);
        clientHost += ":" + std::to_string(ntohs(clientAddr.sin_port));

        std::thread([clientFd, &tlsCtx, clientHost]() {
            SecureSocket sock(clientFd, tlsCtx.ctx());
            if (tlsCtx.enabled()) {
                if (!sock.handshake()) {
                    std::cerr << "TLS handshake failed" << std::endl;
                    sock.close();
                    g_stats.activeConnections--;
                    return;
                }
            }
            handleClient(std::move(sock), clientHost);
        }).detach();
    }

    ::close(serverFd);
}

} // namespace dbms
