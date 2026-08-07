#include "NetworkServer.h"
#include <cassert>
#include <iostream>

int main() {
    assert(!dbms::isServerTransportAllowed(false, false));
    assert(dbms::isServerTransportAllowed(true, false));
    assert(dbms::isServerTransportAllowed(false, true));

    std::cout << "[NETWORK SECURITY] TLS fail-closed policy OK\n";
    return 0;
}
