#pragma once

namespace dbms {

// Start a TCP server on the given port.
// Each client connection gets a dedicated thread.
// Protocol (text, newline-delimited):
//   Server -> Client: "login"
//   Client -> Server: "username password"
//   Server -> Client: "successfully login" or "wrong username or password"
//   Then SQL commands are exchanged until client sends "exit".
void startServer(int port);

} // namespace dbms
