#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace dbms {

struct ServerStats {
    std::atomic<int> activeConnections{0};
    std::atomic<int> totalConnections{0};
    std::atomic<int> maxConnections{64};
    std::atomic<int> rejectedConnections{0};
};

// Per-connection process info (for SHOW PROCESSLIST)
struct ProcessInfo {
    uint64_t id;
    std::string user;
    std::string host;
    std::string db;
    std::string command;
    double timeSec;
    std::string state;
    std::string info;
    std::chrono::steady_clock::time_point connectTime;
};

// Start a TCP server on the given port.
// Each client connection gets a dedicated thread.
// Protocol (text, newline-delimited):
//   Server -> Client: "login"
//   Client -> Server: "username password"
//   Server -> Client: "successfully login" or "wrong username or password"
//   Then SQL commands are exchanged until client sends "exit".
void startServer(int port);

// Get server connection statistics
ServerStats& getServerStats();

// Process list management (thread-safe)
uint64_t registerProcess(const std::string& user, const std::string& host, const std::string& db);
void updateProcessInfo(uint64_t pid, const std::string& command,
                       const std::string& state, const std::string& info);
void updateProcessDb(uint64_t pid, const std::string& db);
void unregisterProcess(uint64_t pid);
std::vector<ProcessInfo> getProcessList();

} // namespace dbms
