#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

struct SessionInterruptState;

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
    std::string lastQuery;
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
// Blocks until SIGINT/SIGTERM or requestServerShutdown() is received. Returns
// false when startup or the accept loop fails, so callers can fail closed.
bool startServer(int port, bool allowPlaintext = false);

// Request a graceful server shutdown. The listening socket and active client
// sockets are interrupted, then startServer() joins all connection workers.
void requestServerShutdown();
bool serverShutdownRequested();

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
struct BackendRegistration {
    uint64_t pid = 0;
    uint32_t secretKey = 0;
};
BackendRegistration registerProcess(
    const std::string& user, const std::string& host, const std::string& db,
    const std::shared_ptr<SessionInterruptState>& interruptState = nullptr);
// Reserve a database for removal only when no backend is connected to it.
// Registration and database switches check the same reservation under the
// process-list mutex, closing the startup/idle-session race with DROP.
bool reserveDatabaseDrop(const std::string& db);
void releaseDatabaseDrop(const std::string& db);
bool trySwitchProcessDb(uint64_t pid, const std::string& db);
void updateProcessInfo(uint64_t pid, const std::string& command,
                       const std::string& state, const std::string& info);
void updateProcessDb(uint64_t pid, const std::string& db);
void unregisterProcess(uint64_t pid);
std::vector<ProcessInfo> getProcessList();

// Cancel / terminate a backend by pid (for pg_cancel_backend / pg_terminate_backend)
bool cancelBackend(uint64_t pid);
bool terminateBackend(uint64_t pid);
bool cancelBackend(uint32_t pid, uint32_t secretKey);

} // namespace dbms
