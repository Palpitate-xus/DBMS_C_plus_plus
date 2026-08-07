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
    bool cancelRequested = false;    // set by pg_cancel_backend
    bool terminateRequested = false; // set by pg_terminate_backend
};

// Start a TCP server on the given port. TLS is mandatory unless
// allowPlaintext is explicitly set by the caller for local development.
// Each client connection gets a dedicated thread.
// Protocol: PostgreSQL Frontend/Backend protocol 3.0. The server handles
// SSLRequest negotiation, startup/authentication, simple Query messages and
// the Parse/Bind/Execute/Sync extended-query flow.
void startServer(int port, bool allowPlaintext = false);

// Transport policy used by startup and unit tests. A server may listen only
// when TLS is ready or plaintext was explicitly enabled.
bool isServerTransportAllowed(bool tlsEnabled, bool allowPlaintext);

// Atomically reserve/release a connection slot so concurrent accepts cannot
// exceed maxConnections.
bool tryReserveConnectionSlot();
void releaseConnectionSlot();

// Get server connection statistics
ServerStats& getServerStats();

// Process list management (thread-safe)
uint64_t registerProcess(const std::string& user, const std::string& host, const std::string& db);
void updateProcessInfo(uint64_t pid, const std::string& command,
                       const std::string& state, const std::string& info);
void updateProcessDb(uint64_t pid, const std::string& db);
void unregisterProcess(uint64_t pid);
std::vector<ProcessInfo> getProcessList();

// Cancel / terminate a backend by pid (for pg_cancel_backend / pg_terminate_backend)
bool cancelBackend(uint64_t pid);
bool terminateBackend(uint64_t pid);

} // namespace dbms
