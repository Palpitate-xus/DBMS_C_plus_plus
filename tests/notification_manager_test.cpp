#include "common/NotificationManager.h"

#include <cassert>
#include <iostream>

int main() {
    auto& manager = dbms::notificationManager();
    constexpr uint64_t firstBackend = 1001;
    constexpr uint64_t secondBackend = 1002;
    constexpr uint64_t senderBackend = 2001;

    manager.disconnect(firstBackend);
    manager.disconnect(secondBackend);
    manager.disconnect(senderBackend);

    manager.listen(firstBackend, "same_role_channel");
    manager.listen(secondBackend, "same_role_channel");
    manager.publish(senderBackend, "same_role_channel", "payload");

    const auto first = manager.takePending(firstBackend);
    const auto second = manager.takePending(secondBackend);
    assert(first.size() == 1);
    assert(second.size() == 1);
    assert(first.front().senderPid == senderBackend);
    assert(first.front().channel == "same_role_channel");
    assert(first.front().payload == "payload");
    assert(second.front().senderPid == senderBackend);
    assert(second.front().channel == "same_role_channel");
    assert(second.front().payload == "payload");

    manager.listen(firstBackend, "disconnect_channel");
    manager.disconnect(firstBackend);
    manager.publish(senderBackend, "disconnect_channel", "orphan");
    assert(manager.takePending(firstBackend).empty());

    manager.disconnect(secondBackend);
    manager.disconnect(senderBackend);
    std::cout << "[NOTIFICATION MANAGER] backend identity and cleanup OK"
              << std::endl;
    return 0;
}
