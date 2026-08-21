#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "utils/Session.h"

namespace dbms {

// PgBouncer-style backend context pool.
//
// A BackendContext bundles everything a pooled backend needs to serve
// statements: the Session (current DB, user, GUC overrides, temp tables)
// and the extended-query prepared statement map. Client connections no
// longer own a backend for their whole lifetime; they rent one while
// executing and return it to the pool between statements.
//
// Modes (mirroring PgBouncer):
//   session      the client keeps its backend until disconnect (PG default
//                behaviour; pooling only tracks accounting)
//   transaction  the backend is returned at transaction end (COMMIT /
//                ROLLBACK / implicit autocommit statement end)
//   statement    the backend is returned after every statement
//
// Pooling key is (user, database): sessions are not interchangeable across
// roles or databases.
// Pooled backend statements: a pool-owned prepared statement is the raw
// SQL plus declared parameter OIDs, mirroring what the protocol layer
// keeps per connection today. The protocol layer's richer portal state
// stays connection-local; only the statement registry pools.
struct PooledStatement {
    std::string sql;
    std::vector<uint32_t> parameterTypes;
};

class BackendContext {
public:
    Session session;
    // Prepared statements registered by Parse that survive backend
    // rentals (PgBouncer transaction-pooling semantics for named
    // statements).
    std::map<std::string, PooledStatement> preparedStatements;

    const std::string& user() const { return session.username; }
};

class ConnectionPool {
public:
    enum class Mode { Session, Transaction, Statement };

    static ConnectionPool& instance();

    // Configure from GUCs. poolMode: "session"|"transaction"|"statement".
    void configure(const std::string& poolMode, int poolSize, int maxClientConnections);

    Mode mode() const { return mode_; }
    int poolSize() const { return poolSize_; }
    int maxClientConnections() const { return maxClientConnections_; }
    const std::string& modeName() const { return modeName_; }

    // Reserve a slot for an incoming client connection (separate from the
    // engine's maxConnections backend slots). Returns false when the client
    // limit is reached.
    bool tryReserveClientSlot();
    void releaseClientSlot();

    // Rent a backend context for (user, database). Blocks while every
    // matching backend is busy and the pool is at capacity; returns nullptr
    // when shutdown is requested while waiting.
    std::shared_ptr<BackendContext> acquire(const std::string& user,
                                            const std::string& database);

    // Give a backend back. In session mode the owner keeps it until
    // disconnect, so release only happens through discard().
    void release(const std::shared_ptr<BackendContext>& ctx);

    // Permanently remove a context (session end with dirty state such as
    // temp tables or open cursors).
    void discard(const std::shared_ptr<BackendContext>& ctx);

    // Reset transaction-scoped state so the next renter sees a clean
    // backend (prepared statements survive, matching PgBouncer
    // transaction pooling semantics for named statements).
    static void resetForReuse(BackendContext& ctx);


    // Shutdown: wake all waiting renters.
    void shutdown();

    // Observability (thread-safe snapshots).
    struct Stats {
        int totalContexts = 0;
        int idleContexts = 0;
        int rentedContexts = 0;
        int waitingRenters = 0;
        int clientConnections = 0;
        int maxClientConnections = 0;
        int poolSize = 0;
        std::string mode = "session";
        uint64_t totalRents = 0;
        uint64_t totalWaits = 0;
    };
    Stats stats() const;

private:
    ConnectionPool() = default;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    Mode mode_ = Mode::Session;
    int poolSize_ = 16;
    int maxClientConnections_ = 0;  // 0 = unlimited
    std::string modeName_ = "session";

    struct Entry {
        std::shared_ptr<BackendContext> ctx;
        bool idle = true;
    };
    // Pool entries keyed by user; database lives in the context's session.
    std::map<std::string, std::vector<Entry>> pools_;
    std::atomic<int> clientConnections_{0};
    std::atomic<uint64_t> totalRents_{0};
    std::atomic<uint64_t> totalWaits_{0};
    int waitingRenters_ = 0;
    bool shutdown_ = false;
};

}  // namespace dbms
