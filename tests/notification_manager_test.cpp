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

    // A receiving backend must not consume queued notifications while its
    // transaction is open.  They become visible after either transaction
    // boundary; the sender's already-committed notification is not rolled
    // back with the receiver.
    constexpr uint64_t longTransactionListener = 4001;
    manager.listen(longTransactionListener, "db1", "deferred_delivery");
    manager.beginTransaction(longTransactionListener);
    assert(manager.publish(senderBackend, "db1", "deferred_delivery",
                           "after boundary"));
    assert(manager.takePending(longTransactionListener).empty());
    manager.rollbackTransaction(longTransactionListener);
    const auto afterReceiverRollback =
        manager.takePending(longTransactionListener);
    assert(afterReceiverRollback.size() == 1);
    assert(afterReceiverRollback.front().payload == "after boundary");

    // PostgreSQL accepts payloads strictly shorter than 8000 bytes. Reject
    // invalid values before they can enter a transaction or delivery queue.
    assert(manager.publish(senderBackend, "db1", "bounds_channel",
                           std::string(7999, 'x')));
    assert(!manager.publish(senderBackend, "db1", "bounds_channel",
                            std::string(8000, 'x')));
    assert(!manager.publish(senderBackend, "db1", "bounds_channel",
                            std::string("embedded\0zero", 13)));

    // The shared logical queue is bounded independently of listener count.
    // A transaction reserves its queue bytes before commit, queue-full leaves
    // it rollbackable, and storage is released only after every recipient
    // has consumed (or disconnected from) that logical entry.
    dbms::NotificationManager bounded(8192);
    constexpr uint64_t boundedFirst = 5001;
    constexpr uint64_t boundedSecond = 5002;
    constexpr uint64_t boundedSender = 5003;
    bounded.listen(boundedFirst, "db1", "bounded_channel");
    bounded.listen(boundedSecond, "db1", "bounded_channel");
    bounded.beginTransaction(boundedSender);
    assert(bounded.publish(boundedSender, "db1", "bounded_channel",
                           std::string(7900, 'q')));
    assert(bounded.queueUsage() == 0.0);
    assert(bounded.prepareCommitTransaction(boundedSender));
    assert(bounded.commitTransaction(boundedSender));
    const double occupied = bounded.queueUsage();
    assert(occupied > 0.9 && occupied < 1.0);
    assert(!bounded.configureMaxQueueBytes(16384));

    constexpr uint64_t blockedSender = 5004;
    bounded.beginTransaction(blockedSender);
    assert(bounded.publish(blockedSender, "db1", "bounded_channel",
                           std::string(7900, 'r')));
    assert(!bounded.prepareCommitTransaction(blockedSender));
    assert(!bounded.commitTransaction(blockedSender));
    bounded.rollbackTransaction(blockedSender);
    assert(!bounded.publish(blockedSender, "db1", "bounded_channel",
                            std::string(7900, 's')));

    assert(bounded.takePending(boundedFirst).size() == 1);
    assert(bounded.queueUsage() == occupied);
    assert(bounded.takePending(boundedSecond).size() == 1);
    assert(bounded.queueUsage() == 0.0);
    assert(bounded.configureMaxQueueBytes(16384));

    manager.disconnect(secondBackend);
    manager.disconnect(senderBackend);
    manager.disconnect(listenerBackend);
    manager.disconnect(transactionalSender);
    manager.disconnect(longTransactionListener);
    std::cout << "[NOTIFICATION MANAGER] backend identity and cleanup OK"
              << std::endl;
    return 0;
}
