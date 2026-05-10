#pragma once

#include <atomic>
#include <string>

namespace dbms {

struct ServerStats {
    std::atomic<int> activeConnections{0};
    std::atomic<int> totalConnections{0};
    std::atomic<int> maxConnections{64};
    std::atomic<int> rejectedConnections{0};
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

} // namespace dbms
