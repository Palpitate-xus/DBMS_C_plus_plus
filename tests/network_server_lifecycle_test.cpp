#include "NetworkServer.h"

#include <arpa/inet.h>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {

int reservePort(int& fdOut) {
    fdOut = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(fdOut >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(0);
    assert(::bind(fdOut, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    assert(::listen(fdOut, 4) == 0);
    socklen_t length = sizeof(address);
    assert(::getsockname(fdOut, reinterpret_cast<sockaddr*>(&address), &length) == 0);
    return ntohs(address.sin_port);
}

bool waitForServer(int port, int& clientFd) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        clientFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (clientFd < 0) return false;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(static_cast<uint16_t>(port));
        if (::connect(clientFd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0) {
            return true;
        }
        ::close(clientFd);
        clientFd = -1;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

} // namespace

int main() {
    int occupiedFd = -1;
    const int occupiedPort = reservePort(occupiedFd);
    assert(!dbms::startServer(occupiedPort, true));
    ::close(occupiedFd);

    int lifecyclePortFd = -1;
    const int lifecyclePort = reservePort(lifecyclePortFd);
    ::close(lifecyclePortFd);

    bool serverResult = false;
    std::thread server([&] {
        serverResult = dbms::startServer(lifecyclePort, true);
    });

    int clientFd = -1;
    assert(waitForServer(lifecyclePort, clientFd));
    dbms::requestServerShutdown();
    ::shutdown(clientFd, SHUT_RDWR);
    ::close(clientFd);
    server.join();

    assert(serverResult);
    assert(dbms::serverShutdownRequested());
    assert(dbms::getServerStats().activeConnections.load() == 0);
    std::cout << "[NETWORK LIFECYCLE] occupied-port failure is reported\n";
    std::cout << "[NETWORK LIFECYCLE] graceful shutdown joins workers\n";
    return 0;
}
