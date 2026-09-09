#include "process/AdvisoryLockManager.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <thread>

using namespace dbms;

namespace {

AdvisoryLockKey bigint(const char* database, int64_t value) {
    return {database, AdvisoryKeySpace::BigInt, value, 0};
}

AdvisoryLockKey pair(const char* database, int32_t first, int32_t second) {
    return {database, AdvisoryKeySpace::IntPair, first, second};
}

}  // namespace

int main() {
    AdvisoryLockManager manager;
    constexpr uint64_t first = 11;
    constexpr uint64_t second = 22;

    // Signed bigint keys, pair keys and databases are separate namespaces.
    assert(manager.acquire(bigint("a", -1), first,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));
    assert(manager.acquire(pair("a", -1, 0), second,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));
    assert(manager.acquire(bigint("b", -1), second,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));

    // Another owner cannot re-enter an exclusive lock. The owner can, and
    // each session acquisition requires a matching unlock.
    assert(!manager.acquire(bigint("a", -1), second,
                            AdvisoryLockScope::Session,
                            AdvisoryLockMode::Exclusive, false));
    assert(manager.acquire(bigint("a", -1), first,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));
    assert(manager.unlockSession(bigint("a", -1), first,
                                 AdvisoryLockMode::Exclusive));
    assert(!manager.acquire(bigint("a", -1), second,
                            AdvisoryLockScope::Session,
                            AdvisoryLockMode::Exclusive, false));
    assert(manager.unlockSession(bigint("a", -1), first,
                                 AdvisoryLockMode::Exclusive));
    assert(manager.acquire(bigint("a", -1), second,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));

    // Shared holders coexist, while an exclusive waiter sleeps until every
    // conflicting holder releases.
    const auto shared = bigint("a", 50);
    assert(manager.acquire(shared, first, AdvisoryLockScope::Session,
                           AdvisoryLockMode::Shared, false));
    assert(manager.acquire(shared, second, AdvisoryLockScope::Session,
                           AdvisoryLockMode::Shared, false));
    std::atomic<bool> entered{false};
    auto waiter = std::async(std::launch::async, [&] {
        entered.store(true, std::memory_order_release);
        return manager.acquire(shared, 33, AdvisoryLockScope::Session,
                               AdvisoryLockMode::Exclusive, true);
    });
    while (!entered.load(std::memory_order_acquire)) std::this_thread::yield();
    assert(waiter.wait_for(std::chrono::milliseconds(30)) ==
           std::future_status::timeout);
    assert(manager.unlockSession(shared, first, AdvisoryLockMode::Shared));
    assert(waiter.wait_for(std::chrono::milliseconds(30)) ==
           std::future_status::timeout);
    assert(manager.unlockSession(shared, second, AdvisoryLockMode::Shared));
    assert(waiter.wait_for(std::chrono::seconds(1)) ==
           std::future_status::ready);
    assert(waiter.get());

    // Transaction locks survive savepoint release, locks acquired after a
    // savepoint disappear on rollback-to, and commit releases the rest.
    manager.beginTransaction(first);
    assert(manager.acquire(bigint("a", 70), first,
                           AdvisoryLockScope::Transaction,
                           AdvisoryLockMode::Exclusive, false));
    manager.savepoint(first, "s");
    assert(manager.acquire(bigint("a", 71), first,
                           AdvisoryLockScope::Transaction,
                           AdvisoryLockMode::Exclusive, false));
    assert(!manager.acquire(bigint("a", 71), second,
                            AdvisoryLockScope::Session,
                            AdvisoryLockMode::Exclusive, false));
    manager.rollbackToSavepoint(first, "s");
    assert(manager.acquire(bigint("a", 71), second,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));
    assert(!manager.acquire(bigint("a", 70), second,
                            AdvisoryLockScope::Session,
                            AdvisoryLockMode::Exclusive, false));
    assert(manager.releaseTransaction(first) == 1);
    assert(manager.acquire(bigint("a", 70), second,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));

    // Prepared transactions retain xact locks until completion by any owner.
    manager.beginTransaction(first);
    assert(manager.acquire(bigint("a", 80), first,
                           AdvisoryLockScope::Transaction,
                           AdvisoryLockMode::Exclusive, false));
    assert(manager.prepareTransaction(first, "prepared-1"));
    assert(!manager.acquire(bigint("a", 80), second,
                            AdvisoryLockScope::Session,
                            AdvisoryLockMode::Exclusive, false));
    manager.finishPrepared("prepared-1");
    assert(manager.acquire(bigint("a", 80), second,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));

    // Disconnect cleanup releases both scopes owned by the backend.
    manager.beginTransaction(first);
    assert(manager.acquire(bigint("a", 90), first,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));
    assert(manager.acquire(bigint("a", 91), first,
                           AdvisoryLockScope::Transaction,
                           AdvisoryLockMode::Exclusive, false));
    assert(manager.releaseAll(first) == 2);
    assert(manager.acquire(bigint("a", 90), second,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));
    assert(manager.acquire(bigint("a", 91), second,
                           AdvisoryLockScope::Session,
                           AdvisoryLockMode::Exclusive, false));

    manager.releaseAll(first);
    manager.releaseAll(second);
    manager.releaseAll(33);
    return 0;
}
