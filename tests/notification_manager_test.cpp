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

    manager.listen(firstBackend, "db1", "same_role_channel");
    manager.listen(secondBackend, "db1", "same_role_channel");
    manager.publish(senderBackend, "db1", "same_role_channel", "payload");

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

    manager.listen(secondBackend, "db1", "database_channel");
    manager.publish(senderBackend, "db2", "database_channel", "wrong database");
    assert(manager.takePending(secondBackend).empty());
    manager.publish(senderBackend, "db1", "database_channel", "same database");
    assert(manager.takePending(secondBackend).size() == 1);

    manager.listen(firstBackend, "db1", "disconnect_channel");
    manager.disconnect(firstBackend);
    manager.publish(senderBackend, "db1", "disconnect_channel", "orphan");
    assert(manager.takePending(firstBackend).empty());

    constexpr uint64_t listenerBackend = 3001;
    constexpr uint64_t transactionalSender = 3002;
    manager.listen(listenerBackend, "db1", "transaction_channel");

    manager.beginTransaction(transactionalSender);
    manager.publish(transactionalSender, "db1", "transaction_channel", "rolled back");
    assert(manager.hasTransactionalActions(transactionalSender));
    assert(manager.takePending(listenerBackend).empty());
    manager.rollbackTransaction(transactionalSender);
    assert(manager.takePending(listenerBackend).empty());

    manager.beginTransaction(transactionalSender);
    manager.publish(transactionalSender, "db1", "transaction_channel", "committed");
    manager.publish(transactionalSender, "db1", "transaction_channel", "committed");
    assert(manager.takePending(listenerBackend).empty());
    assert(manager.commitTransaction(transactionalSender));
    const auto committed = manager.takePending(listenerBackend);
    assert(committed.size() == 1);
    assert(committed.front().payload == "committed");

    manager.beginTransaction(secondBackend);
    manager.listen(secondBackend, "db1", "transaction_channel");
    manager.publish(transactionalSender, "db1", "transaction_channel", "before listen commit");
    assert(manager.takePending(secondBackend).empty());
    assert(manager.takePending(listenerBackend).size() == 1);
    assert(manager.commitTransaction(secondBackend));
    manager.publish(transactionalSender, "db1", "transaction_channel", "after listen commit");
    assert(manager.takePending(secondBackend).size() == 1);
    assert(manager.takePending(listenerBackend).size() == 1);

    manager.beginTransaction(secondBackend);
    manager.unlisten(secondBackend, "db1", "transaction_channel");
    manager.rollbackTransaction(secondBackend);
    manager.publish(transactionalSender, "db1", "transaction_channel", "unlisten rolled back");
    assert(manager.takePending(secondBackend).size() == 1);
    assert(manager.takePending(listenerBackend).size() == 1);

    manager.beginTransaction(transactionalSender);
    manager.publish(transactionalSender, "db1", "transaction_channel", "before savepoint");
    assert(manager.savepoint(transactionalSender, "notification_sp"));
    manager.publish(transactionalSender, "db1", "transaction_channel", "after savepoint");
    assert(manager.rollbackToSavepoint(transactionalSender, "notification_sp"));
    assert(manager.commitTransaction(transactionalSender));
    const auto afterSavepoint = manager.takePending(listenerBackend);
    assert(afterSavepoint.size() == 1);
    assert(afterSavepoint.front().payload == "before savepoint");

    // PostgreSQL accepts payloads strictly shorter than 8000 bytes. Reject
    // invalid values before they can enter a transaction or delivery queue.
    assert(manager.publish(senderBackend, "db1", "bounds_channel",
                           std::string(7999, 'x')));
    assert(!manager.publish(senderBackend, "db1", "bounds_channel",
                            std::string(8000, 'x')));
    assert(!manager.publish(senderBackend, "db1", "bounds_channel",
                            std::string("embedded\0zero", 13)));

    manager.disconnect(secondBackend);
    manager.disconnect(senderBackend);
    manager.disconnect(listenerBackend);
    manager.disconnect(transactionalSender);
    std::cout << "[NOTIFICATION MANAGER] backend identity and cleanup OK"
              << std::endl;
    return 0;
}
