#include "NetworkServer.h"
#include "process/OutputCapture.h"
#include <cassert>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

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

    std::vector<std::string> captured(4);
    std::vector<std::thread> workers;
    for (size_t i = 0; i < captured.size(); ++i) {
        workers.emplace_back([i, &captured] {
            std::ostringstream output;
            {
                dbms::ScopedOutputCapture capture(output);
                for (int line = 0; line < 32; ++line) {
                    std::cout << "session-" << i << "-" << line << '\n';
                }
            }
            captured[i] = output.str();
        });
    }
    for (auto& worker : workers) worker.join();
    for (size_t i = 0; i < captured.size(); ++i) {
        assert(captured[i].find("session-" + std::to_string(i) + "-") !=
               std::string::npos);
        for (size_t other = 0; other < captured.size(); ++other) {
            if (other == i) continue;
            assert(captured[i].find("session-" + std::to_string(other) + "-") ==
                   std::string::npos);
        }
    }

    std::cout << "[NETWORK SECURITY] TLS fail-closed policy OK\n";
    std::cout << "[NETWORK SECURITY] atomic connection capacity OK\n";
    std::cout << "[NETWORK SECURITY] thread-local output capture OK\n";
    return 0;
}
