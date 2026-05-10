#include "NetworkServer.h"
#include "TableManage.h"
#include "permissions.h"
#include "Session.h"

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
extern void logSlowQuery(const std::string& sql, double ms);

namespace dbms {

static ServerStats g_stats;

ServerStats& getServerStats() {
    return g_stats;
}

static void sendLine(int fd, const std::string& msg) {
    std::string line = msg + "\n";
    ssize_t sent = 0;
    while (sent < static_cast<ssize_t>(line.size())) {
        ssize_t n = ::write(fd, line.data() + sent, line.size() - sent);
        if (n <= 0) break;
        sent += n;
    }
}

static std::string recvLine(int fd) {
    std::string line;
    char ch;
    while (true) {
        ssize_t n = ::read(fd, &ch, 1);
        if (n <= 0) return line;
        if (ch == '\n') break;
        if (ch != '\r') line.push_back(ch);
    }
    return line;
}

static void handleClient(int clientFd) {
    g_stats.activeConnections++;
    g_stats.totalConnections++;

    Session s;

    // Login phase
    sendLine(clientFd, "login");
    std::string creds = recvLine(clientFd);
    std::stringstream ss(creds);
    std::string username, password;
    ss >> username >> password;

    if (!login(username, password)) {
        sendLine(clientFd, "wrong username or password");
        ::close(clientFd);
        g_stats.activeConnections--;
        return;
    }

    s.username = username;
    s.permission = permissionQuery(username);
    s.currentDB = "info";
    sendLine(clientFd, "successfully login");

    // SQL execution loop
    while (true) {
        std::string sql = recvLine(clientFd);
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

        auto start = std::chrono::steady_clock::now();

        // Redirect cout to capture output
        auto* oldBuf = std::cout.rdbuf();
        std::stringstream output;
        std::cout.rdbuf(output.rdbuf());
        execute(sql, s);
        std::cout.rdbuf(oldBuf);

        auto end = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        if (ms > 100.0) logSlowQuery(sql, ms);

        std::string result = output.str();
        // Send result back to client
        size_t pos = 0;
        while (pos < result.size()) {
            size_t nl = result.find('\n', pos);
            if (nl == std::string::npos) {
                sendLine(clientFd, result.substr(pos));
                break;
            }
            sendLine(clientFd, result.substr(pos, nl - pos));
            pos = nl + 1;
        }
    }

    ::close(clientFd);
    g_stats.activeConnections--;
}

void startServer(int port) {
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

    std::cout << "DBMS server listening on port " << port << std::endl;

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t clientLen = sizeof(clientAddr);
        int clientFd = ::accept(serverFd, reinterpret_cast<sockaddr*>(&clientAddr), &clientLen);
        if (clientFd < 0) continue;

        if (g_stats.activeConnections >= g_stats.maxConnections.load()) {
            sendLine(clientFd, "too many connections");
            ::close(clientFd);
            g_stats.rejectedConnections++;
            continue;
        }

        std::thread([clientFd]() {
            handleClient(clientFd);
        }).detach();
    }

    ::close(serverFd);
}

} // namespace dbms
