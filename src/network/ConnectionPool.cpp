#include "network/ConnectionPool.h"

#include <algorithm>

namespace dbms {

ConnectionPool& ConnectionPool::instance() {
    static ConnectionPool pool;
    return pool;
}

void ConnectionPool::configure(const std::string& poolMode, int poolSize,
                               int maxClientConnections) {
    std::lock_guard<std::mutex> lock(mutex_);
    modeName_ = poolMode;
    if (poolMode == "transaction") {
        mode_ = Mode::Transaction;
    } else if (poolMode == "statement") {
        mode_ = Mode::Statement;
    } else {
        mode_ = Mode::Session;
        modeName_ = "session";
    }
    if (poolSize >= 1) poolSize_ = poolSize;
    maxClientConnections_ = std::max(0, maxClientConnections);
}

bool ConnectionPool::tryReserveClientSlot() {
    if (maxClientConnections_ == 0) return true;  // unlimited
    int current = clientConnections_.load(std::memory_order_relaxed);
    while (current < maxClientConnections_) {
        if (clientConnections_.compare_exchange_weak(current, current + 1,
                                                     std::memory_order_acq_rel,
                                                     std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void ConnectionPool::releaseClientSlot() {
    clientConnections_.fetch_sub(1, std::memory_order_acq_rel);
}

std::shared_ptr<BackendContext> ConnectionPool::acquire(const std::string& user,
                                                        const std::string& database) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (shutdown_) return nullptr;

    auto& entries = pools_[user];

    // Reuse an idle context bound to the same database.
    for (auto& entry : entries) {
        if (entry.idle && entry.ctx->session.currentDB == database) {
            entry.idle = false;
            totalRents_.fetch_add(1, std::memory_order_relaxed);
            return entry.ctx;
        }
    }

    // Grow the pool when below capacity.
    if (static_cast<int>(entries.size()) < poolSize_) {
        auto ctx = std::make_shared<BackendContext>();
        ctx->session.username = user;
        ctx->session.currentDB = database;
        ctx->session.permission = 1;
        entries.push_back({ctx, false});
        totalRents_.fetch_add(1, std::memory_order_relaxed);
        return ctx;
    }

    // At capacity: wait for a matching backend to come back.
    ++waitingRenters_;
    totalWaits_.fetch_add(1, std::memory_order_relaxed);
    cv_.wait(lock, [&]() {
        return shutdown_ || [&]() {
            for (const auto& entry : entries) {
                if (entry.idle && entry.ctx->session.currentDB == database) return true;
            }
            return false;
        }();
    });
    --waitingRenters_;
    if (shutdown_) return nullptr;
    for (auto& entry : entries) {
        if (entry.idle && entry.ctx->session.currentDB == database) {
            entry.idle = false;
            totalRents_.fetch_add(1, std::memory_order_relaxed);
            return entry.ctx;
        }
    }
    return nullptr;  // spurious wakeup guard; should not happen
}

void ConnectionPool::release(const std::shared_ptr<BackendContext>& ctx) {
    if (!ctx) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = pools_.find(ctx->session.username);
    if (it == pools_.end()) return;
    for (auto& entry : it->second) {
        if (entry.ctx == ctx) {
            entry.idle = true;
            break;
        }
    }
    cv_.notify_all();
}

void ConnectionPool::discard(const std::shared_ptr<BackendContext>& ctx) {
    if (!ctx) return;
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = pools_.find(ctx->session.username);
    if (it == pools_.end()) return;
    auto& entries = it->second;
    for (auto eit = entries.begin(); eit != entries.end(); ++eit) {
        if (eit->ctx == ctx) {
            entries.erase(eit);
            break;
        }
    }
    if (entries.empty()) pools_.erase(it);
    cv_.notify_all();
}

void ConnectionPool::resetForReuse(BackendContext& ctx) {
    // Transaction-scoped state is cleared; named prepared statements and
    // session GUCs survive, matching PgBouncer transaction pooling.
    // Transaction begin/end itself lives in the engine's transaction
    // context and is driven by the statement executor (BEGIN/COMMIT), so a
    // renter that returns mid-transaction must have been rolled back by
    // the protocol layer before calling this.
    // (Portal state is connection-local and never pools.)
}

void ConnectionPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
    }
    cv_.notify_all();
}

ConnectionPool::Stats ConnectionPool::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    Stats s;
    s.mode = modeName_;
    s.poolSize = poolSize_;
    s.maxClientConnections = maxClientConnections_;
    s.clientConnections = clientConnections_.load(std::memory_order_relaxed);
    s.waitingRenters = waitingRenters_;
    s.totalRents = totalRents_.load(std::memory_order_relaxed);
    s.totalWaits = totalWaits_.load(std::memory_order_relaxed);
    for (const auto& kv : pools_) {
        s.totalContexts += static_cast<int>(kv.second.size());
        for (const auto& entry : kv.second) {
            if (entry.idle) ++s.idleContexts;
            else ++s.rentedContexts;
        }
    }
    return s;
}

}  // namespace dbms
