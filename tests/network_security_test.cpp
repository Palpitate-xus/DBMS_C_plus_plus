#include "NetworkServer.h"
#include <cassert>
#include <iostream>

int main() {
    assert(!dbms::isServerTransportAllowed(false, false));
    assert(dbms::isServerTransportAllowed(true, false));
    assert(dbms::isServerTransportAllowed(false, true));

    auto& stats = dbms::getServerStats();
    const int oldActive = stats.activeConnections.load();
    const int oldMax = stats.maxConnections.load();
    stats.activeConnections.store(0);
    stats.maxConnections.store(1);
    assert(dbms::tryReserveConnectionSlot());
    assert(!dbms::tryReserveConnectionSlot());
    dbms::releaseConnectionSlot();
    assert(stats.activeConnections.load() == 0);
    stats.activeConnections.store(oldActive);
    stats.maxConnections.store(oldMax);

    std::cout << "[NETWORK SECURITY] TLS fail-closed policy OK\n";
    std::cout << "[NETWORK SECURITY] atomic connection capacity OK\n";
    return 0;
}
