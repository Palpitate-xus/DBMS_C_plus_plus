#include "network/ConnectionPool.h"

#include <cassert>
#include <chrono>
#include <future>
#include <iostream>
#include <thread>

using dbms::BackendContext;
using dbms::ConnectionPool;

static void waitForRenter(ConnectionPool& pool) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (pool.stats().waitingRenters != 1 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    assert(pool.stats().waitingRenters == 1);
}

static std::shared_ptr<BackendContext> finishRent(
    ConnectionPool& pool, std::thread& waiter,
    std::future<std::shared_ptr<BackendContext>>& result) {
    const bool ready = result.wait_for(std::chrono::seconds(3)) == std::future_status::ready;
    if (!ready) pool.shutdown();
    waiter.join();
    assert(ready);
    auto rented = result.get();
    assert(rented);
    return rented;
}

static void test_idle_foreign_database() {
    auto& pool = ConnectionPool::instance();
    pool.configure("statement", 1, 0);
    auto old = pool.acquire("cross_db", "db1");
    old->preparedStatements["old_query"] = {"SELECT 1", {}};
    pool.release(old);
    std::promise<std::shared_ptr<BackendContext>> rented;
    auto result = rented.get_future();
    std::thread waiter([&] { rented.set_value(pool.acquire("cross_db", "db2")); });
    auto fresh = finishRent(pool, waiter, result);
    assert(fresh != old && fresh->session.currentDB == "db2");
    assert(fresh->session.username == "cross_db");
    assert(fresh->preparedStatements.empty());
    assert(old->session.currentDB == "db1" && old->preparedStatements.size() == 1);
    assert(pool.stats().totalContexts == 1 && pool.stats().rentedContexts == 1);
    pool.discard(fresh);
    assert(pool.stats().totalContexts == 0);
    std::cout << "[POOL DB] idle foreign backend replaced at user capacity OK" << std::endl;
}

static void test_busy_foreign_database() {
    auto& pool = ConnectionPool::instance();
    pool.configure("transaction", 1, 0);
    auto held = pool.acquire("busy_cross_db", "db1");
    held->preparedStatements["old_query"] = {"SELECT 1", {}};
    const auto waitsBefore = pool.stats().totalWaits;
    std::promise<std::shared_ptr<BackendContext>> rented;
    auto result = rented.get_future();
    std::thread waiter([&] { rented.set_value(pool.acquire("busy_cross_db", "db2")); });
    waitForRenter(pool);
    assert(pool.stats().totalContexts == 1 && pool.stats().rentedContexts == 1);
    assert(held->session.currentDB == "db1");
    pool.release(held);
    auto fresh = finishRent(pool, waiter, result);
    assert(fresh != held && fresh->session.currentDB == "db2");
    assert(fresh->preparedStatements.empty() && held->preparedStatements.size() == 1);
    assert(pool.stats().totalContexts == 1 && pool.stats().waitingRenters == 0);
    assert(pool.stats().totalWaits == waitsBefore + 1);
    pool.discard(fresh);
    std::cout << "[POOL DB] busy backend retained until release OK" << std::endl;
}

static void test_capacity_growth_notification() {
    auto& pool = ConnectionPool::instance();
    pool.configure("statement", 1, 0);
    auto held = pool.acquire("grow_cross_db", "db1");
    std::promise<std::shared_ptr<BackendContext>> rented;
    auto result = rented.get_future();
    std::thread waiter([&] { rented.set_value(pool.acquire("grow_cross_db", "db2")); });
    waitForRenter(pool);
    pool.configure("statement", 2, 0);
    auto fresh = finishRent(pool, waiter, result);
    assert(fresh != held && held->session.currentDB == "db1");
    assert(fresh->session.currentDB == "db2" && pool.stats().totalContexts == 2);
    pool.discard(held);
    pool.discard(fresh);
    std::cout << "[POOL DB] configured growth wakes waiting renter OK" << std::endl;
}

static void test_per_user_capacity_and_same_database_reuse() {
    auto& pool = ConnectionPool::instance();
    pool.configure("statement", 1, 0);
    auto first = pool.acquire("capacity_a", "db1");
    auto other = pool.acquire("capacity_b", "db1");
    assert(first != other && pool.stats().totalContexts == 2);
    first->preparedStatements["same_db"] = {"SELECT 2", {}};
    pool.release(first);
    auto reused = pool.acquire("capacity_a", "db1");
    assert(reused == first && reused->preparedStatements.count("same_db") == 1);
    pool.discard(reused);
    pool.discard(other);
    assert(pool.stats().totalContexts == 0);
    std::cout << "[POOL DB] per-user limit and same-db state retained OK" << std::endl;
}

int main() {
    test_idle_foreign_database();
    test_busy_foreign_database();
    test_capacity_growth_notification();
    test_per_user_capacity_and_same_database_reuse();
    ConnectionPool::instance().shutdown();
    std::cout << "[POOL DB] all controls passed" << std::endl;
}
