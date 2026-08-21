// ============================================================================
// connection_pool_test — P2-4 PgBouncer-style connection pooling:
//   configure() validates modes and clamps sizes
//   session mode rents one backend per client, discard frees the slot
//   transaction/statement modes: release returns the backend, the next
//   acquire for the same (user, database) reuses the same context
//   acquire blocks at capacity and wakes on release (contention path)
//   per-user pools are isolated; database mismatch does not reuse
//   max_client_conn accounting refuses extra clients
//   resetForReuse keeps prepared statements (PgBouncer transaction mode)
//   stats() reports contexts/rentals/waits consistently
// ============================================================================

#include "network/ConnectionPool.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

using dbms::BackendContext;
using dbms::ConnectionPool;

// The singleton is process-wide; tests borrow it exclusively. Each test
// releases or discards every context it created before returning, so a
// plain configure() is a clean slate.
static void resetPoolSafe(ConnectionPool& pool, const char* mode,
                          int size, int maxClients) {
    pool.configure(mode, size, maxClients);
}

static void test_configure_modes() {
    auto& pool = ConnectionPool::instance();
    resetPoolSafe(pool, "transaction", 8, 100);
    assert(pool.mode() == ConnectionPool::Mode::Transaction);
    assert(pool.modeName() == "transaction");
    assert(pool.poolSize() == 8);
    assert(pool.maxClientConnections() == 100);

    pool.configure("statement", 2, 0);
    assert(pool.mode() == ConnectionPool::Mode::Statement);
    assert(pool.poolSize() == 2);

    pool.configure("bogus-mode", 0, -5);
    assert(pool.mode() == ConnectionPool::Mode::Session);
    assert(pool.modeName() == "session");
    assert(pool.poolSize() == 2);  // invalid size ignored

    pool.configure("session", 4, 0);
    std::cout << "[POOL] configure modes OK" << std::endl;
}

static void test_session_mode_lifecycle() {
    auto& pool = ConnectionPool::instance();
    resetPoolSafe(pool, "session", 4, 0);

    auto backend = pool.acquire("alice", "db1");
    assert(backend);
    assert(backend->session.username == "alice");
    assert(backend->session.currentDB == "db1");

    // A second renter with the same key gets a different backend.
    auto other = pool.acquire("alice", "db1");
    assert(other && other != backend);

    // Session mode: the backend stays rented until discard.
    auto st = pool.stats();
    assert(st.totalContexts == 2 && st.rentedContexts == 2 && st.idleContexts == 0);

    pool.discard(backend);
    pool.discard(other);
    st = pool.stats();
    assert(st.totalContexts == 0);
    std::cout << "[POOL] session mode lifecycle OK" << std::endl;
}

static void test_short_rent_reuse() {
    auto& pool = ConnectionPool::instance();
    resetPoolSafe(pool, "transaction", 2, 0);

    auto first = pool.acquire("bob", "db1");
    assert(first);
    first->preparedStatements["sel"] = {"SELECT 1", {}};
    ConnectionPool::resetForReuse(*first);
    pool.release(first);

    // The next rental reuses the same context (with its prepared
    // statements, PgBouncer transaction-mode semantics).
    auto second = pool.acquire("bob", "db1");
    assert(second == first);
    assert(second->preparedStatements.count("sel") == 1);

    // A different database must not reuse the same backend even though
    // it is idle for that user.
    ConnectionPool::resetForReuse(*second);
    pool.release(second);
    auto db2 = pool.acquire("bob", "db2");
    assert(db2 && db2 != second);
    assert(db2->session.currentDB == "db2");

    ConnectionPool::resetForReuse(*second);
    pool.release(second);
    ConnectionPool::resetForReuse(*db2);
    pool.release(db2);

    // Per-user isolation.
    auto alice = pool.acquire("alice", "db1");
    assert(alice && alice != second);
    pool.discard(alice);
    pool.discard(second);
    pool.discard(db2);
    std::cout << "[POOL] short-rent reuse OK" << std::endl;
}

static void test_capacity_blocking() {
    auto& pool = ConnectionPool::instance();
    resetPoolSafe(pool, "statement", 1, 0);

    auto held = pool.acquire("carol", "db1");
    assert(held);

    // Capacity 1: a concurrent renter must block until release. Run the
    // release from another thread.
    std::thread releaser([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        ConnectionPool::resetForReuse(*held);
        pool.release(held);
    });
    auto waited = pool.acquire("carol", "db1");
    assert(waited);
    assert(waited == held);  // reuse after release
    const uint64_t waits = pool.stats().totalWaits;
    assert(waits >= 1);      // this acquire had to wait
    releaser.join();
    pool.discard(waited);
    std::cout << "[POOL] capacity blocking/wakeup OK" << std::endl;
}

static void test_client_limit() {
    auto& pool = ConnectionPool::instance();
    resetPoolSafe(pool, "session", 4, 2);

    assert(pool.tryReserveClientSlot());
    assert(pool.tryReserveClientSlot());
    // Third client is refused at max_client_conn.
    assert(!pool.tryReserveClientSlot());
    assert(pool.stats().clientConnections == 2);
    pool.releaseClientSlot();
    assert(pool.tryReserveClientSlot());
    pool.releaseClientSlot();
    pool.releaseClientSlot();

    // 0 disables the limit entirely.
    pool.configure("session", 4, 0);
    for (int i = 0; i < 10; ++i) assert(pool.tryReserveClientSlot());
    for (int i = 0; i < 10; ++i) pool.releaseClientSlot();
    std::cout << "[POOL] client limit accounting OK" << std::endl;
}

static void test_stats() {
    auto& pool = ConnectionPool::instance();
    resetPoolSafe(pool, "session", 4, 0);

    const uint64_t rentsBefore = pool.stats().totalRents;
    auto ctx = pool.acquire("admin", "db9");
    assert(ctx);
    const auto st = pool.stats();
    assert(st.totalRents >= rentsBefore + 1);
    assert(st.mode == "session");
    assert(st.poolSize == 4);
    pool.discard(ctx);
    assert(pool.stats().totalContexts == 0);
    std::cout << "[POOL] stats OK" << std::endl;
}

int main() {
    test_configure_modes();
    test_session_mode_lifecycle();
    test_short_rent_reuse();
    test_capacity_blocking();
    test_client_limit();
    test_stats();
    ConnectionPool::instance().shutdown();
    std::cout << "[POOL] all tests passed" << std::endl;
    return 0;
}
