#include "LockManager.h"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <sys/file.h>
#include <unistd.h>

namespace dbms {

LockManager& LockManager::global() {
    static LockManager manager;
    return manager;
}

LockManager::~LockManager() {
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        for (auto& entry : locks_) {
            entry.second.suspendedTransactions.clear();
            releaseProcessLock(entry.second);
        }
    }
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (auto& entry : rowLocks_) {
            entry.second.suspendedTransactions.clear();
            releaseProcessLock(entry.second);
        }
    }
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        for (auto& entry : pageLocks_) {
            entry.second.suspendedTransactions.clear();
            releaseProcessLock(entry.second);
        }
    }
    {
        std::lock_guard<std::mutex> guard(gapMutex_);
        preparedGapLocks_.clear();
        for (const auto& entry : gapProcessLockFds_) {
            (void)::flock(entry.second, LOCK_UN);
            ::close(entry.second);
        }
    }
}

bool LockManager::suspendCurrentLocksForPrepared(uint64_t txnId) {
    if (txnId == 0) return false;
    const std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> globalGuard(globalMutex_);
    std::lock_guard<std::mutex> rowGuard(rowMutex_);
    std::lock_guard<std::mutex> pageGuard(pageMutex_);
    std::lock_guard<std::mutex> gapGuard(gapMutex_);

    auto suspendTable = [&](LockState& state) {
        auto modeIt = state.holderModes.find(self);
        if (modeIt == state.holderModes.end()) return;
        const LockMode mode = modeIt->second;
        switch (mode) {
            case LockMode::Shared: state.mtx.unlock_shared(); --state.sharedCount; break;
            case LockMode::Exclusive: state.mtx.unlock(); state.exclusive = false; break;
            case LockMode::IntentShared: state.mtx.unlock_shared(); --state.intentSharedCount; break;
            case LockMode::IntentExclusive: state.mtx.unlock(); --state.intentExclusiveCount; break;
            case LockMode::Metadata: state.mtx.unlock(); state.metadata = false; break;
        }
        state.holders.erase(std::remove(state.holders.begin(), state.holders.end(), self),
                            state.holders.end());
        state.holderModes.erase(self);
        state.holderCounts.erase(self);
        state.suspendedTransactions[txnId] = mode;
    };
    for (auto& [key, state] : locks_) suspendTable(state);

    auto suspendSimple = [&](auto& registry) {
        for (auto& [key, state] : registry) {
            const auto holder = std::find(state.holders.begin(), state.holders.end(), self);
            if (holder == state.holders.end()) continue;
            const LockMode mode = state.exclusive ? LockMode::Exclusive : LockMode::Shared;
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            } else {
                state.mtx.unlock_shared();
                --state.sharedCount;
            }
            state.holders.erase(holder);
            state.suspendedTransactions[txnId] = mode;
        }
    };
    suspendSimple(rowLocks_);
    suspendSimple(pageLocks_);

    for (auto it = gapLocks_.begin(); it != gapLocks_.end(); ) {
        auto& gaps = it->second;
        const bool owned = std::any_of(gaps.begin(), gaps.end(),
                                       [&](const GapLock& gap) { return gap.holder == self; });
        if (owned) {
            preparedGapLocks_[it->first][txnId] = LockMode::Exclusive;
            gaps.erase(std::remove_if(gaps.begin(), gaps.end(),
                                      [&](const GapLock& gap) { return gap.holder == self; }),
                        gaps.end());
        }
        if (gaps.empty()) {
            // Keep the advisory fd open in gapProcessLockFds_; it is now owned
            // by preparedGapLocks_ and must survive this backend transition.
            it = gapLocks_.erase(it);
        } else {
            ++it;
        }
    }
    removeWaitEdges(self);
    return true;
}

void LockManager::releasePreparedLocks(uint64_t txnId) {
    if (txnId == 0) return;
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        for (auto it = locks_.begin(); it != locks_.end(); ) {
            it->second.suspendedTransactions.erase(txnId);
            if (it->second.suspendedTransactions.empty() && it->second.holders.empty()) {
                releaseProcessLock(it->second);
            }
            ++it;
        }
    }
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (auto it = rowLocks_.begin(); it != rowLocks_.end(); ) {
            it->second.suspendedTransactions.erase(txnId);
            if (it->second.suspendedTransactions.empty() && it->second.holders.empty()) {
                releaseProcessLock(it->second);
                if (it->second.waiters == 0) it = rowLocks_.erase(it);
                else ++it;
            } else {
                ++it;
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        for (auto it = pageLocks_.begin(); it != pageLocks_.end(); ) {
            it->second.suspendedTransactions.erase(txnId);
            if (it->second.suspendedTransactions.empty() && it->second.holders.empty()) {
                releaseProcessLock(it->second);
                if (it->second.waiters == 0) it = pageLocks_.erase(it);
                else ++it;
            } else {
                ++it;
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(gapMutex_);
        for (auto it = preparedGapLocks_.begin(); it != preparedGapLocks_.end(); ) {
            it->second.erase(txnId);
            if (!it->second.empty()) {
                ++it;
                continue;
            }
            auto fd = gapProcessLockFds_.find(it->first);
            if (fd != gapProcessLockFds_.end()) {
                (void)::flock(fd->second, LOCK_UN);
                ::close(fd->second);
                gapProcessLockFds_.erase(fd);
            }
            it = preparedGapLocks_.erase(it);
        }
    }
}

bool LockManager::restorePreparedLocks(
    uint64_t txnId, const std::string& dbname,
    const std::vector<PreparedLockInfo>& locks) {
    if (txnId == 0 || dbname.empty()) return false;

    auto modeIsShared = [](LockMode mode) {
        return mode == LockMode::Shared || mode == LockMode::IntentShared;
    };
    auto canAddPrepared = [&](const LockState& state, LockMode mode) {
        if (!state.holders.empty()) return false;
        for (const auto& [existingXid, existingMode] : state.suspendedTransactions) {
            if (existingXid == txnId) {
                if (existingMode != mode) return false;
                continue;
            }
            if (!modeIsShared(existingMode) || !modeIsShared(mode)) return false;
        }
        return true;
    };

    std::lock_guard<std::mutex> globalGuard(globalMutex_);
    std::lock_guard<std::mutex> rowGuard(rowMutex_);
    std::lock_guard<std::mutex> pageGuard(pageMutex_);
    std::lock_guard<std::mutex> gapGuard(gapMutex_);

    for (const auto& lock : locks) {
        if (lock.dbname != dbname || lock.table.empty()) {
            return false;
        }
        if (lock.kind == PreparedLockInfo::Kind::Table) {
            const std::string key = resourceKeyForNamespace(dbname, lock.table);
            auto& state = locks_[key];
            if (!canAddPrepared(state, lock.mode)) {
                return false;
            }
            if (state.suspendedTransactions.count(txnId)) continue;
            const auto result = tryAcquireProcessLock(
                state, dbname, "table", lock.table, lock.mode, true);
            if (result != ProcessLockResult::Acquired) {
                return false;
            }
            state.suspendedTransactions[txnId] = lock.mode;
        } else if (lock.kind == PreparedLockInfo::Kind::Row) {
            const std::string resource = lock.table + ":" + std::to_string(lock.rid);
            const std::string key = resourceKeyForNamespace(dbname, resource);
            auto& state = rowLocks_[key];
            if (!canAddPrepared(state, lock.mode)) {
                return false;
            }
            if (state.suspendedTransactions.count(txnId)) continue;
            const auto result = tryAcquireProcessLock(
                state, dbname, "row", key, lock.mode, true);
            if (result != ProcessLockResult::Acquired) {
                return false;
            }
            state.suspendedTransactions[txnId] = lock.mode;
        } else if (lock.kind == PreparedLockInfo::Kind::Page) {
            const std::string key = "page:" + dbname + ":" + lock.table + ":" +
                                    std::to_string(lock.pageId);
            auto& state = pageLocks_[key];
            if (!canAddPrepared(state, lock.mode)) {
                return false;
            }
            if (state.suspendedTransactions.count(txnId)) continue;
            const auto result = tryAcquireProcessLock(
                state, dbname, "page", key, lock.mode, true);
            if (result != ProcessLockResult::Acquired) {
                return false;
            }
            state.suspendedTransactions[txnId] = lock.mode;
        } else {
            if (lock.mode != LockMode::Exclusive) {
                return false;
            }
            const std::string key = resourceKeyForNamespace(dbname, lock.table);
            const auto existing = preparedGapLocks_.find(key);
            if (existing != preparedGapLocks_.end() && existing->second.count(txnId)) continue;
            if (!tryAcquireGapProcessLock(dbname, lock.table)) {
                return false;
            }
            preparedGapLocks_[key][txnId] = LockMode::Exclusive;
        }
    }
    return true;
}

LockManager::ThreadSettings& LockManager::threadSettings() const {
    return threadSettings_[instanceId_];
}

void LockManager::setResourceNamespace(const std::string& dbname) const {
    threadSettings().resourceNamespace = dbname;
}

std::string LockManager::resourceKey(const std::string& resource) const {
    return resourceKeyForNamespace(threadSettings().resourceNamespace, resource);
}

std::string LockManager::resourceKeyForNamespace(const std::string& resourceNamespace,
                                                 const std::string& resource) {
    if (resourceNamespace.empty()) return resource;
    return resourceNamespace + '\x1f' + resource;
}

std::string LockManager::rowResourceKey(const std::string& table, int64_t rid) const {
    return resourceKey(table + ":" + std::to_string(rid));
}

std::string LockManager::processLockPath(const std::string& resourceNamespace,
                                         const std::string& kind,
                                         const std::string& resource) const {
    if (resourceNamespace.empty()) return {};
    std::filesystem::path path(resourceNamespace);
    path /= ".lockmgr";
    // Resource names can originate from SQL or from the low-level API. Encode
    // every byte so a name can never escape the lock directory or collide with
    // a different path spelling (for example, "a/b" versus "a_b").
    static constexpr char hex[] = "0123456789abcdef";
    std::string encoded = kind + "-";
    encoded.reserve(kind.size() + resource.size() * 2 + 1);
    for (const unsigned char byte : resource) {
        encoded.push_back(hex[byte >> 4]);
        encoded.push_back(hex[byte & 0x0f]);
    }
    path /= encoded + ".lock";
    return path.string();
}

LockManager::ProcessLockResult LockManager::tryAcquireProcessLock(
    LockState& state, const std::string& resourceNamespace,
    const std::string& kind, const std::string& resource, LockMode mode,
    bool restoringPrepared) {
    const bool exclusive = mode != LockMode::Shared && mode != LockMode::IntentShared;
    if (!state.suspendedTransactions.empty() && !restoringPrepared) {
        // Prepared shared locks may coexist. Any prepared exclusive-like lock
        // blocks every new local/process owner until its second phase ends.
        for (const auto& [preparedXid, preparedMode] : state.suspendedTransactions) {
            (void)preparedXid;
            const bool preparedExclusive = preparedMode != LockMode::Shared &&
                                           preparedMode != LockMode::IntentShared;
            if (preparedExclusive || exclusive) return ProcessLockResult::Busy;
        }
        if (state.processLockFd < 0) return ProcessLockResult::Error;
        return ProcessLockResult::Acquired;
    }
    if (state.processLockFd >= 0) {
        if (state.processLockMode == LockMode::Shared && exclusive) {
            if (!state.holders.empty()) return ProcessLockResult::Busy;
            if (::flock(state.processLockFd, LOCK_EX | LOCK_NB) != 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) return ProcessLockResult::Busy;
                return ProcessLockResult::Error;
            }
            state.processLockMode = LockMode::Exclusive;
        } else if (state.processLockMode == LockMode::Exclusive && !exclusive) {
            if (::flock(state.processLockFd, LOCK_SH | LOCK_NB) != 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) return ProcessLockResult::Busy;
                return ProcessLockResult::Error;
            }
            state.processLockMode = LockMode::Shared;
        }
        return ProcessLockResult::Acquired;
    }

    const std::string path = processLockPath(resourceNamespace, kind, resource);
    if (path.empty()) return ProcessLockResult::Acquired;
    std::error_code ec;
    const auto databasePath = std::filesystem::path(path).parent_path().parent_path();
    if (!std::filesystem::is_directory(databasePath, ec) || ec) {
        return ProcessLockResult::Error;
    }
    ec.clear();
    std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
    if (ec) return ProcessLockResult::Error;
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return ProcessLockResult::Error;
    const int operation = exclusive ? LOCK_EX : LOCK_SH;
    if (::flock(fd, operation | LOCK_NB) != 0) {
        const bool busy = errno == EWOULDBLOCK || errno == EAGAIN;
        ::close(fd);
        return busy ? ProcessLockResult::Busy : ProcessLockResult::Error;
    }
    state.processLockFd = fd;
    state.processLockMode = exclusive ? LockMode::Exclusive : LockMode::Shared;
    state.processLockNamespace = resourceNamespace;
    state.processLockKind = kind;
    state.processLockResource = resource;
    return ProcessLockResult::Acquired;
}

LockManager::ProcessLockResult LockManager::tryAcquireProcessLock(
    LockState& state, const std::string& table, LockMode mode) {
    return tryAcquireProcessLock(state, threadSettings().resourceNamespace,
                                 "table", table, mode);
}

void LockManager::releaseProcessLock(LockState& state) {
    // A prepared transaction owns the advisory lock even after its backend
    // thread has released the local mutex token. Only COMMIT/ROLLBACK
    // PREPARED may release it.
    if (!state.suspendedTransactions.empty()) return;
    if (state.processLockFd < 0) return;
    (void)::flock(state.processLockFd, LOCK_UN);
    ::close(state.processLockFd);
    state.processLockFd = -1;
    state.processLockNamespace.clear();
    state.processLockKind.clear();
    state.processLockResource.clear();
}

bool LockManager::tryAcquireGapProcessLock(const std::string& table) {
    return tryAcquireGapProcessLock(threadSettings().resourceNamespace, table);
}

bool LockManager::tryAcquireGapProcessLock(const std::string& resourceNamespace,
                                           const std::string& table) {
    const std::string path = processLockPath(resourceNamespace, "gap", table);
    if (path.empty()) return true;
    const std::string key = resourceKeyForNamespace(resourceNamespace, table);
    if (preparedGapLocks_.find(key) != preparedGapLocks_.end()) return false;
    if (gapProcessLockFds_.find(key) != gapProcessLockFds_.end()) return true;
    std::error_code ec;
    const auto databasePath = std::filesystem::path(path).parent_path().parent_path();
    if (!std::filesystem::is_directory(databasePath, ec) || ec) return false;
    ec.clear();
    std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
    if (ec) return false;
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return false;
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        ::close(fd);
        return false;
    }
    gapProcessLockFds_[key] = fd;
    return true;
}

void LockManager::releaseGapProcessLock(const std::string& key) {
    if (preparedGapLocks_.find(key) != preparedGapLocks_.end()) return;
    auto it = gapProcessLockFds_.find(key);
    if (it == gapProcessLockFds_.end()) return;
    (void)::flock(it->second, LOCK_UN);
    ::close(it->second);
    gapProcessLockFds_.erase(it);
}

bool LockManager::isGapProcessLocked(const std::string& table) const {
    const std::string key = resourceKey(table);
    if (preparedGapLocks_.find(key) != preparedGapLocks_.end()) return true;
    if (gapProcessLockFds_.find(key) != gapProcessLockFds_.end()) return false;
    const std::string path = processLockPath(threadSettings().resourceNamespace,
                                             "gap", table);
    if (path.empty()) return false;
    const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) return errno != ENOENT;
    const bool busy = ::flock(fd, LOCK_SH | LOCK_NB) != 0;
    if (!busy) (void)::flock(fd, LOCK_UN);
    ::close(fd);
    return busy;
}

// ========================================================================
// Wait-for graph helpers
// ========================================================================

void LockManager::addWaitEdge(std::thread::id waiter, std::thread::id holder) {
    std::lock_guard<std::mutex> guard(waitMutex_);
    waitFor_[waiter].insert(holder);
}

void LockManager::removeWaitEdges(std::thread::id waiter) {
    std::lock_guard<std::mutex> guard(waitMutex_);
    waitFor_.erase(waiter);
}

void LockManager::setLockTimeout(int ms) {
    threadSettings().lockTimeoutMs = ms > 0 ? ms : 0;
}

void LockManager::setDeadlockTimeout(int ms) {
    threadSettings().deadlockTimeoutMs = ms > 0 ? ms : 0;
}

static std::string tidToString(std::thread::id tid) {
    std::ostringstream oss;
    oss << tid;
    return oss.str();
}

static std::string nowIso8601() {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&time_t_now);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    return std::string(buf);
}

bool LockManager::hasCycle(std::thread::id start) {
    std::lock_guard<std::mutex> guard(waitMutex_);
    std::set<std::thread::id> visited, recStack;
    std::vector<std::thread::id> stack;
    stack.push_back(start);
    while (!stack.empty()) {
        std::thread::id node = stack.back();
        if (recStack.find(node) == recStack.end()) {
            recStack.insert(node);
            auto it = waitFor_.find(node);
            if (it != waitFor_.end()) {
                for (const auto& neighbor : it->second) {
                    if (recStack.find(neighbor) != recStack.end()) {
                        // Deadlock detected — record it before returning
                        std::string desc = "cycle: " + tidToString(node) + " -> " + tidToString(neighbor);
                        for (const auto& e : waitFor_) {
                            desc += "; " + tidToString(e.first) + " waits for ";
                            bool first = true;
                            for (const auto& h : e.second) {
                                if (!first) desc += ",";
                                desc += tidToString(h);
                                first = false;
                            }
                        }
                        {
                            std::lock_guard<std::mutex> dlGuard(deadlockMutex_);
                            deadlockLog_.push_back({nowIso8601(), desc});
                            if (deadlockLog_.size() > 100) deadlockLog_.erase(deadlockLog_.begin());
                        }
                        return true;
                    }
                    if (visited.find(neighbor) == visited.end()) {
                        stack.push_back(neighbor);
                    }
                }
            }
        } else {
            recStack.erase(node);
            visited.insert(node);
            stack.pop_back();
        }
        if (!stack.empty() && visited.find(stack.back()) != visited.end()) {
            stack.pop_back();
        }
    }
    return false;
}

// ========================================================================
// Lock acquisition with deadlock detection
// ========================================================================

// ========================================================================
// Lock compatibility
// ========================================================================

static bool isCompatible(LockManager::LockMode requested, LockManager::LockMode held, bool sameThread) {
    if (sameThread) return true; // Same thread can upgrade or re-acquire
    switch (requested) {
        case LockManager::LockMode::Shared:
            return held == LockManager::LockMode::Shared || held == LockManager::LockMode::IntentShared;
        case LockManager::LockMode::Exclusive:
            return false;
        case LockManager::LockMode::IntentShared:
            return held == LockManager::LockMode::Shared ||
                   held == LockManager::LockMode::IntentShared;
        case LockManager::LockMode::IntentExclusive:
            // IntentExclusive uses the underlying exclusive mutex in this
            // manager, so claiming compatibility with IS/IX would allow the
            // wait graph to say "compatible" while shared_mutex blocks.
            return false;
        case LockManager::LockMode::Metadata:
            return false;
    }
    return false;
}

static const char* modeToStr(LockManager::LockMode mode) {
    switch (mode) {
        case LockManager::LockMode::Shared: return "shared";
        case LockManager::LockMode::Exclusive: return "exclusive";
        case LockManager::LockMode::IntentShared: return "IS";
        case LockManager::LockMode::IntentExclusive: return "IX";
        case LockManager::LockMode::Metadata: return "MDL";
    }
    return "";
}

bool LockManager::acquireLock(const std::string& table, LockMode mode) {
    const std::thread::id self = std::this_thread::get_id();

    auto isSharedMode = [](LockMode value) {
        return value == LockMode::Shared || value == LockMode::IntentShared;
    };
    auto isReusable = [&](LockMode held, LockMode requested) {
        if (held == LockMode::Exclusive || held == LockMode::Metadata) return true;
        if (held == LockMode::IntentExclusive) {
            return requested == LockMode::Shared ||
                   requested == LockMode::IntentShared ||
                   requested == LockMode::IntentExclusive;
        }
        return isSharedMode(held) && isSharedMode(requested);
    };

    auto releasePhysical = [&](LockState& state) {
        auto modeIt = state.holderModes.find(self);
        if (modeIt == state.holderModes.end()) return;
        switch (modeIt->second) {
            case LockMode::Shared:
                state.mtx.unlock_shared();
                --state.sharedCount;
                break;
            case LockMode::Exclusive:
                state.mtx.unlock();
                state.exclusive = false;
                break;
            case LockMode::IntentShared:
                state.mtx.unlock_shared();
                --state.intentSharedCount;
                break;
            case LockMode::IntentExclusive:
                state.mtx.unlock();
                --state.intentExclusiveCount;
                break;
            case LockMode::Metadata:
                state.mtx.unlock();
                state.metadata = false;
                break;
        }
        state.holders.erase(std::remove(state.holders.begin(), state.holders.end(), self),
                            state.holders.end());
        state.holderModes.erase(modeIt);
        state.holderCounts.erase(self);
    };

    auto acquirePhysical = [&](LockState& state) {
        switch (mode) {
            case LockMode::Shared:
                state.mtx.lock_shared();
                ++state.sharedCount;
                break;
            case LockMode::Exclusive:
                state.mtx.lock();
                state.exclusive = true;
                break;
            case LockMode::IntentShared:
                state.mtx.lock_shared();
                ++state.intentSharedCount;
                break;
            case LockMode::IntentExclusive:
                state.mtx.lock();
                ++state.intentExclusiveCount;
                break;
            case LockMode::Metadata:
                state.mtx.lock();
                state.metadata = true;
                break;
        }
        state.holders.push_back(self);
        state.holderModes[self] = mode;
        state.holderCounts[self] = 1;
        removeWaitEdges(self);
    };

    auto inspect = [&](LockState& state) {
        // Replace, rather than accumulate, this waiter's outgoing edges. This
        // matters when a lock is released and a different transaction takes
        // its place before the next polling pass.
        removeWaitEdges(self);
        bool compatible = true;
        for (const auto& holder : state.holders) {
            if (holder == self) continue;
            const auto it = state.holderModes.find(holder);
            const LockMode held = it == state.holderModes.end()
                ? LockMode::Shared : it->second;
            if (!isCompatible(mode, held, false)) {
                compatible = false;
                addWaitEdge(self, holder);
            }
        }
        return compatible;
    };

    const auto waitStart = std::chrono::steady_clock::now();
    const auto deadlockCheckAt = waitStart +
        std::chrono::milliseconds(std::max(0, threadSettings().deadlockTimeoutMs));
    while (true) {
        bool cycle = false;
        {
            std::lock_guard<std::mutex> guard(globalMutex_);
            auto& state = locks_[resourceKey(table)];

            auto selfMode = state.holderModes.find(self);
            if (selfMode != state.holderModes.end()) {
                if (isReusable(selfMode->second, mode)) {
                    ++state.holderCounts[self];
                    return true;
                }
                // Upgrade from a shared/intent lock. Drop this thread's
                // physical token before waiting for the stronger mode so it
                // cannot wait on itself.
                releasePhysical(state);
                if (state.holders.empty()) releaseProcessLock(state);
            }

            if (inspect(state)) {
                const auto processResult = tryAcquireProcessLock(state, table, mode);
                if (processResult == ProcessLockResult::Acquired) {
                    acquirePhysical(state);
                    return true;
                }
                if (processResult == ProcessLockResult::Error) {
                    removeWaitEdges(self);
                    if (state.holders.empty()) releaseProcessLock(state);
                    return false;
                }
            }
            cycle = std::chrono::steady_clock::now() >= deadlockCheckAt &&
                    hasCycle(self);
        }
        if (cycle) {
            removeWaitEdges(self);
            std::lock_guard<std::mutex> guard(globalMutex_);
            auto it = locks_.find(resourceKey(table));
            if (it != locks_.end() && it->second.holders.empty()) {
                releaseProcessLock(it->second);
            }
            return false;
        }

        if (threadSettings().lockTimeoutMs > 0) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - waitStart).count();
            if (elapsed >= threadSettings().lockTimeoutMs) {
                removeWaitEdges(self);
                std::lock_guard<std::mutex> guard(globalMutex_);
                auto it = locks_.find(resourceKey(table));
                if (it != locks_.end() && it->second.holders.empty()) {
                    releaseProcessLock(it->second);
                }
                return false;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

bool LockManager::lockShared(const std::string& table) {
    return acquireLock(table, LockMode::Shared);
}

bool LockManager::lockExclusive(const std::string& table) {
    return acquireLock(table, LockMode::Exclusive);
}

bool LockManager::lockIntentShared(const std::string& table) {
    return acquireLock(table, LockMode::IntentShared);
}

bool LockManager::lockIntentExclusive(const std::string& table) {
    return acquireLock(table, LockMode::IntentExclusive);
}

bool LockManager::lockMetadata(const std::string& table) {
    return acquireLock(table, LockMode::Metadata);
}

// ========================================================================
// Unlock
// ========================================================================

void LockManager::unlock(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(globalMutex_);
    removeWaitEdges(self);
    auto it = locks_.find(resourceKey(table));
    if (it == locks_.end()) return;
    auto& state = it->second;
    auto countIt = state.holderCounts.find(self);
    auto modeIt = state.holderModes.find(self);
    if (countIt == state.holderCounts.end() || modeIt == state.holderModes.end()) return;
    if (countIt->second > 1) {
        --countIt->second;
        return;
    }
    const LockMode mode = modeIt->second;
    switch (mode) {
        case LockMode::Shared:
            state.mtx.unlock_shared();
            --state.sharedCount;
            break;
        case LockMode::Exclusive:
            state.mtx.unlock();
            state.exclusive = false;
            break;
        case LockMode::IntentShared:
            state.mtx.unlock_shared();
            state.intentSharedCount--;
            break;
        case LockMode::IntentExclusive:
            state.mtx.unlock();
            state.intentExclusiveCount--;
            break;
        case LockMode::Metadata:
            state.mtx.unlock();
            state.metadata = false;
            break;
    }
    state.holders.erase(std::remove(state.holders.begin(), state.holders.end(), self),
                        state.holders.end());
    state.holderModes.erase(modeIt);
    state.holderCounts.erase(countIt);
    if (state.holders.empty()) releaseProcessLock(state);
}

void LockManager::unlockAll() {
    std::thread::id self = std::this_thread::get_id();
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        removeWaitEdges(self);
        for (auto& kv : locks_) {
            auto& state = kv.second;
            auto modeIt = state.holderModes.find(self);
            if (modeIt != state.holderModes.end()) {
                switch (modeIt->second) {
                    case LockMode::Shared:
                        state.mtx.unlock_shared();
                        state.sharedCount--;
                        break;
                    case LockMode::Exclusive:
                        state.mtx.unlock();
                        state.exclusive = false;
                        break;
                    case LockMode::IntentShared:
                        state.mtx.unlock_shared();
                        state.intentSharedCount--;
                        break;
                    case LockMode::IntentExclusive:
                        state.mtx.unlock();
                        state.intentExclusiveCount--;
                        break;
                    case LockMode::Metadata:
                        state.mtx.unlock();
                        state.metadata = false;
                        break;
                }
                state.holderModes.erase(modeIt);
                state.holderCounts.erase(self);
                state.holders.erase(std::remove(state.holders.begin(), state.holders.end(), self),
                                    state.holders.end());
                if (state.holders.empty()) releaseProcessLock(state);
            }
        }
    }
    // Also release all row locks held by this thread
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (auto it = rowLocks_.begin(); it != rowLocks_.end(); ) {
            auto& state = it->second;
            auto hit = std::find(state.holders.begin(), state.holders.end(), self);
            if (hit == state.holders.end()) {
                ++it;
                continue;
            }
            state.holders.erase(hit);
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            } else {
                state.mtx.unlock_shared();
                --state.sharedCount;
            }
            if (state.holders.empty()) releaseProcessLock(state);
            if (state.sharedCount == 0 && !state.exclusive &&
                state.holders.empty() && state.waiters == 0) {
                it = rowLocks_.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Also release all page locks held by this thread
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        for (auto it = pageLocks_.begin(); it != pageLocks_.end(); ) {
            auto& state = it->second;
            auto hit = std::find(state.holders.begin(), state.holders.end(), self);
            if (hit == state.holders.end()) {
                ++it;
                continue;
            }
            state.holders.erase(hit);
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            } else {
                state.mtx.unlock_shared();
                --state.sharedCount;
            }
            if (state.holders.empty()) releaseProcessLock(state);
            if (state.sharedCount == 0 && !state.exclusive &&
                state.holders.empty() && state.waiters == 0) {
                it = pageLocks_.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Gap locks are part of the transaction's lock set as well. Keep this
    // idempotent for callers that explicitly invoke unlockAllGaps afterward.
    unlockAllGaps();
}

// ========================================================================
// Helpers
// ========================================================================

std::vector<std::string> LockManager::lockedTables() const {
    std::vector<std::string> result;
    std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(globalMutex_));
    for (const auto& kv : locks_) {
        const auto& state = kv.second;
        if (state.exclusive || state.sharedCount > 0 ||
            state.intentSharedCount > 0 || state.intentExclusiveCount > 0 ||
            state.metadata) {
            result.push_back(kv.first);
        }
    }
    return result;
}

// ========================================================================
// Row-level locking
// ========================================================================

bool LockManager::rowLockShared(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = rowResourceKey(table, rid);
    const auto started = std::chrono::steady_clock::now();
    while (true) {
        LockState* state = nullptr;
        {
            std::lock_guard<std::mutex> guard(rowMutex_);
            auto& current = rowLocks_[key];
            // Already holding shared or exclusive lock on this row.
            if (std::find(current.holders.begin(), current.holders.end(), self) !=
                current.holders.end()) return true;
            if (!current.exclusive) {
                const auto processResult = tryAcquireProcessLock(
                    current, threadSettings().resourceNamespace, "row", key, LockMode::Shared);
                if (processResult == ProcessLockResult::Error) return false;
                if (processResult == ProcessLockResult::Acquired) {
                    current.mtx.lock_shared();
                    ++current.sharedCount;
                    current.holders.push_back(self);
                    return true;
                }
            }
            if (current.exclusive) {
                for (const auto& holder : current.holders) {
                    if (holder != self) addWaitEdge(self, holder);
                }
                if (hasCycle(self)) {
                    removeWaitEdges(self);
                    return false;
                }
            }
            ++current.waiters;
            state = &current;
        }
        state->mtx.lock_shared();
        {
            std::lock_guard<std::mutex> guard(rowMutex_);
            --state->waiters;
            const auto processResult = tryAcquireProcessLock(
                *state, threadSettings().resourceNamespace, "row", key, LockMode::Shared);
            if (processResult == ProcessLockResult::Acquired) {
                ++state->sharedCount;
                state->holders.push_back(self);
                removeWaitEdges(self);
                return true;
            }
            state->mtx.unlock_shared();
            if (state->holders.empty()) releaseProcessLock(*state);
            removeWaitEdges(self);
            if (processResult == ProcessLockResult::Error) return false;
        }
        if (threadSettings().lockTimeoutMs > 0 &&
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - started).count() >=
                threadSettings().lockTimeoutMs) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

bool LockManager::rowLockExclusive(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = rowResourceKey(table, rid);
    const auto started = std::chrono::steady_clock::now();
    while (true) {
        LockState* state = nullptr;
        {
            std::lock_guard<std::mutex> guard(rowMutex_);
            auto& current = rowLocks_[key];
            if (current.exclusive && current.holders.size() == 1 && current.holders[0] == self) {
                return true;
            }
            // An upgrade drops the local shared token before waiting. This
            // avoids self-deadlock and also releases the external token.
            auto selfHolder = std::find(current.holders.begin(), current.holders.end(), self);
            if (!current.exclusive && selfHolder != current.holders.end()) {
                current.mtx.unlock_shared();
                --current.sharedCount;
                current.holders.erase(selfHolder);
                if (current.holders.empty()) releaseProcessLock(current);
            }
            if (!current.exclusive && current.sharedCount == 0) {
                const auto processResult = tryAcquireProcessLock(
                    current, threadSettings().resourceNamespace, "row", key, LockMode::Exclusive);
                if (processResult == ProcessLockResult::Error) return false;
                if (processResult == ProcessLockResult::Acquired) {
                    current.mtx.lock();
                    current.exclusive = true;
                    current.holders.push_back(self);
                    removeWaitEdges(self);
                    return true;
                }
            }
            for (const auto& holder : current.holders) {
                if (holder != self) addWaitEdge(self, holder);
            }
            if (hasCycle(self)) {
                removeWaitEdges(self);
                return false;
            }
            ++current.waiters;
            state = &current;
        }
        state->mtx.lock();
        {
            std::lock_guard<std::mutex> guard(rowMutex_);
            --state->waiters;
            const auto processResult = tryAcquireProcessLock(
                *state, threadSettings().resourceNamespace, "row", key, LockMode::Exclusive);
            if (processResult == ProcessLockResult::Acquired) {
                state->exclusive = true;
                state->holders.push_back(self);
                removeWaitEdges(self);
                return true;
            }
            state->mtx.unlock();
            if (state->holders.empty()) releaseProcessLock(*state);
            removeWaitEdges(self);
            if (processResult == ProcessLockResult::Error) return false;
        }
        if (threadSettings().lockTimeoutMs > 0 &&
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - started).count() >=
                threadSettings().lockTimeoutMs) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

bool LockManager::rowLockSharedNoWait(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = rowResourceKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    auto& state = rowLocks_[key];
    if (std::find(state.holders.begin(), state.holders.end(), self) != state.holders.end()) {
        return true;
    }
    if (!state.exclusive && state.holders.empty()) {
        const auto processResult = tryAcquireProcessLock(
            state, threadSettings().resourceNamespace, "row", key, LockMode::Shared);
        if (processResult != ProcessLockResult::Acquired) return false;
        state.mtx.lock_shared();
        state.sharedCount++;
        state.holders.push_back(self);
        return true;
    }
    return false;
}

bool LockManager::rowLockExclusiveNoWait(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = rowResourceKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    auto& state = rowLocks_[key];
    if (state.exclusive && state.holders.size() == 1 && state.holders[0] == self) {
        return true;
    }
    if (!state.exclusive && state.sharedCount > 0 &&
        state.holders.size() == 1 && state.holders[0] == self) {
        state.mtx.unlock_shared();
        state.sharedCount = 0;
        state.holders.clear();
        // The shared file lock must be replaced before publishing an
        // exclusive row token; otherwise another process could still acquire
        // a shared row lock during this in-place upgrade.
        releaseProcessLock(state);
        const auto processResult = tryAcquireProcessLock(
            state, threadSettings().resourceNamespace, "row", key, LockMode::Exclusive);
        if (processResult != ProcessLockResult::Acquired) return false;
        state.mtx.lock();
        state.exclusive = true;
        state.holders.push_back(self);
        return true;
    }
    if (state.sharedCount == 0 && !state.exclusive) {
        const auto processResult = tryAcquireProcessLock(
            state, threadSettings().resourceNamespace, "row", key, LockMode::Exclusive);
        if (processResult != ProcessLockResult::Acquired) return false;
        state.mtx.lock();
        state.exclusive = true;
        state.holders.push_back(self);
        return true;
    }
    return false;
}

void LockManager::rowUnlock(const std::string& table, int64_t rid) {
    std::thread::id self = std::this_thread::get_id();
    std::string key = rowResourceKey(table, rid);
    std::lock_guard<std::mutex> guard(rowMutex_);
    removeWaitEdges(self);
    auto it = rowLocks_.find(key);
    if (it == rowLocks_.end()) return;
    auto& state = it->second;
    auto hit = std::find(state.holders.begin(), state.holders.end(), self);
    if (hit == state.holders.end()) return;
    state.holders.erase(hit);
    if (state.exclusive) {
        state.mtx.unlock();
        state.exclusive = false;
    } else {
        state.mtx.unlock_shared();
        --state.sharedCount;
    }
    if (state.holders.empty()) releaseProcessLock(state);
    // Clean up empty lock state
    if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
        state.waiters == 0) {
        rowLocks_.erase(it);
    }
}

void LockManager::rowUnlockAll(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(rowMutex_);
    removeWaitEdges(self);
    std::string prefix = resourceKey(table + ":");
    for (auto it = rowLocks_.begin(); it != rowLocks_.end(); ) {
        if (it->first.find(prefix) != 0) { ++it; continue; }
        auto& state = it->second;
        auto hit = std::find(state.holders.begin(), state.holders.end(), self);
        if (hit == state.holders.end()) { ++it; continue; }
        state.holders.erase(hit);
        if (state.exclusive) {
            state.mtx.unlock();
            state.exclusive = false;
        } else {
            state.mtx.unlock_shared();
            --state.sharedCount;
        }
        if (state.holders.empty()) releaseProcessLock(state);
        if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
            state.waiters == 0) {
            it = rowLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

std::vector<int64_t> LockManager::lockedRows(const std::string& table) const {
    std::vector<int64_t> result;
    std::string prefix = resourceKey(table + ":");
    std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(rowMutex_));
    for (const auto& kv : rowLocks_) {
        if (kv.first.find(prefix) != 0) continue;
        if (kv.second.exclusive || kv.second.sharedCount > 0) {
            // Extract rid from key
            size_t pos = kv.first.find(':');
            if (pos != std::string::npos) {
                try { result.push_back(std::stoll(kv.first.substr(pos + 1))); } catch (...) {}
            }
        }
    }
    return result;
}

// ========================================================================
// Gap locking
// ========================================================================

bool LockManager::lockGap(const std::string& table, const std::string& leftKey, const std::string& rightKey) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(gapMutex_);
    const std::string key = resourceKey(table);
    if (!tryAcquireGapProcessLock(table)) return false;
    gapLocks_[key].push_back({leftKey, rightKey, self});
    return true;
}

bool LockManager::isGapLocked(const std::string& table, const std::string& key) const {
    std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(gapMutex_));
    if (isGapProcessLocked(table)) return true;
    auto it = gapLocks_.find(resourceKey(table));
    if (it == gapLocks_.end()) return false;
    std::thread::id self = std::this_thread::get_id();
    for (const auto& gl : it->second) {
        if (gl.holder == self) continue; // skip own gap locks
        if (key > gl.leftKey && key < gl.rightKey) return true;
    }
    return false;
}

void LockManager::unlockGaps(const std::string& table) {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(gapMutex_);
    auto it = gapLocks_.find(resourceKey(table));
    if (it == gapLocks_.end()) return;
    auto& vec = it->second;
    vec.erase(std::remove_if(vec.begin(), vec.end(),
        [&self](const GapLock& gl) { return gl.holder == self; }), vec.end());
    if (vec.empty()) {
        releaseGapProcessLock(it->first);
        gapLocks_.erase(it);
    }
}

void LockManager::unlockAllGaps() {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(gapMutex_);
    for (auto it = gapLocks_.begin(); it != gapLocks_.end(); ) {
        auto& vec = it->second;
        vec.erase(std::remove_if(vec.begin(), vec.end(),
            [&self](const GapLock& gl) { return gl.holder == self; }), vec.end());
        if (vec.empty()) {
            releaseGapProcessLock(it->first);
            it = gapLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

// ========================================================================
// Monitoring
// ========================================================================

void LockManager::logDeadlock(const std::string& description) {
    std::lock_guard<std::mutex> guard(deadlockMutex_);
    deadlockLog_.push_back({nowIso8601(), description});
    if (deadlockLog_.size() > 100) deadlockLog_.erase(deadlockLog_.begin());
}

std::vector<LockManager::DeadlockEntry> LockManager::getDeadlockLog() const {
    std::lock_guard<std::mutex> guard(deadlockMutex_);
    return deadlockLog_;
}

void LockManager::clearDeadlockLog() {
    std::lock_guard<std::mutex> guard(deadlockMutex_);
    deadlockLog_.clear();
}

std::vector<LockManager::LockWaitInfo> LockManager::getLockWaits() const {
    std::vector<LockWaitInfo> result;
    std::lock_guard<std::mutex> guard(waitMutex_);
    for (const auto& kv : waitFor_) {
        LockWaitInfo info;
        info.waiterTid = tidToString(kv.first);
        for (const auto& h : kv.second) {
            info.holderTids.push_back(tidToString(h));
        }
        result.push_back(info);
    }
    return result;
}

std::vector<LockManager::LockHoldInfo> LockManager::getLockHolds() const {
    std::vector<LockHoldInfo> result;
    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        for (const auto& kv : locks_) {
            const auto& state = kv.second;
            for (const auto& holder : state.holders) {
                auto modeIt = state.holderModes.find(holder);
                std::string modeStr = modeToStr(
                    (modeIt != state.holderModes.end()) ? modeIt->second : LockMode::Shared);
                result.push_back({kv.first, tidToString(holder), modeStr});
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (const auto& kv : rowLocks_) {
            const auto& state = kv.second;
            std::string modeStr = state.exclusive ? "exclusive" : "shared";
            for (const auto& holder : state.holders) {
                result.push_back({kv.first, tidToString(holder), modeStr});
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        for (const auto& kv : pageLocks_) {
            const auto& state = kv.second;
            std::string modeStr = state.exclusive ? "exclusive" : "shared";
            for (const auto& holder : state.holders) {
                result.push_back({kv.first, tidToString(holder), modeStr});
            }
        }
    }
    return result;
}

bool LockManager::getPreparedLockInfos(
    std::vector<PreparedLockInfo>& result) const {
    result.clear();
    const std::thread::id self = std::this_thread::get_id();
    const auto& settings = threadSettings();
    if (settings.resourceNamespace.empty()) return false;

    const auto stripNamespace = [&](const std::string& key, std::string& resource) {
        const std::string prefix = settings.resourceNamespace + '\x1f';
        if (key.rfind(prefix, 0) != 0) return false;
        resource = key.substr(prefix.size());
        return !resource.empty();
    };

    {
        std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(globalMutex_));
        for (const auto& [key, state] : locks_) {
            if (!state.holderModes.count(self)) continue;
            const std::string resource = state.processLockResource.empty()
                ? key : state.processLockResource;
            if (resource.empty()) return false;
            result.push_back({PreparedLockInfo::Kind::Table, settings.resourceNamespace,
                              resource, 0, 0, {}, {}, state.holderModes.at(self)});
        }
    }
    {
        std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(rowMutex_));
        for (const auto& [key, state] : rowLocks_) {
            if (std::find(state.holders.begin(), state.holders.end(), self) == state.holders.end()) {
                continue;
            }
            std::string resource;
            if (!stripNamespace(state.processLockResource.empty() ? key : state.processLockResource,
                                resource)) return false;
            const size_t separator = resource.rfind(':');
            if (separator == std::string::npos || separator == 0 || separator + 1 == resource.size()) {
                return false;
            }
            int64_t rid = 0;
            try {
                size_t consumed = 0;
                rid = std::stoll(resource.substr(separator + 1), &consumed);
                if (consumed != resource.size() - separator - 1) return false;
            } catch (...) { return false; }
            result.push_back({PreparedLockInfo::Kind::Row, settings.resourceNamespace,
                              resource.substr(0, separator), rid, 0, {}, {},
                              state.exclusive ? LockMode::Exclusive : LockMode::Shared});
        }
    }
    {
        std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(pageMutex_));
        const std::string prefix = "page:" + settings.resourceNamespace + ":";
        for (const auto& [key, state] : pageLocks_) {
            if (std::find(state.holders.begin(), state.holders.end(), self) == state.holders.end()) {
                continue;
            }
            const std::string resource = state.processLockResource.empty()
                ? key : state.processLockResource;
            if (resource.rfind(prefix, 0) != 0) return false;
            const size_t separator = resource.rfind(':');
            if (separator == std::string::npos || separator <= prefix.size() ||
                separator + 1 == resource.size()) return false;
            uint32_t pageId = 0;
            try {
                size_t consumed = 0;
                const auto parsed = std::stoull(resource.substr(separator + 1), &consumed);
                if (consumed != resource.size() - separator - 1 || parsed > UINT32_MAX) return false;
                pageId = static_cast<uint32_t>(parsed);
            } catch (...) { return false; }
            result.push_back({PreparedLockInfo::Kind::Page, settings.resourceNamespace,
                              resource.substr(prefix.size(), separator - prefix.size()), 0,
                              pageId, {}, {}, state.exclusive ? LockMode::Exclusive : LockMode::Shared});
        }
    }
    {
        std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(gapMutex_));
        const std::string prefix = settings.resourceNamespace + '\x1f';
        for (const auto& [key, gaps] : gapLocks_) {
            for (const auto& gap : gaps) {
                if (gap.holder != self) continue;
                if (key.rfind(prefix, 0) != 0) return false;
                result.push_back({PreparedLockInfo::Kind::Gap, settings.resourceNamespace,
                                  key.substr(prefix.size()), 0, 0, gap.leftKey, gap.rightKey,
                                  LockMode::Exclusive});
            }
        }
    }
    return true;
}

LockManager::LockCheckpoint LockManager::captureCheckpoint() const {
    const std::thread::id self = std::this_thread::get_id();
    LockCheckpoint checkpoint;
    {
        std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(globalMutex_));
        for (const auto& [key, state] : locks_) {
            const auto it = state.holderCounts.find(self);
            const auto mode = state.holderModes.find(self);
            if (it != state.holderCounts.end() && mode != state.holderModes.end()) {
                checkpoint.tableCounts[key] = it->second;
                checkpoint.tableModes[key] = mode->second;
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(rowMutex_));
        for (const auto& [key, state] : rowLocks_) {
            if (std::find(state.holders.begin(), state.holders.end(), self) != state.holders.end()) {
                checkpoint.rowLocks.insert(key);
                checkpoint.rowModes[key] = state.exclusive ? LockMode::Exclusive : LockMode::Shared;
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(pageMutex_));
        for (const auto& [key, state] : pageLocks_) {
            if (std::find(state.holders.begin(), state.holders.end(), self) != state.holders.end()) {
                checkpoint.pageLocks.insert(key);
                checkpoint.pageModes[key] = state.exclusive ? LockMode::Exclusive : LockMode::Shared;
            }
        }
    }
    {
        std::lock_guard<std::mutex> guard(const_cast<std::mutex&>(gapMutex_));
        for (const auto& [key, gaps] : gapLocks_) {
            size_t count = 0;
            for (const auto& gap : gaps) if (gap.holder == self) ++count;
            if (count > 0) checkpoint.gapCounts[key] = count;
        }
    }
    return checkpoint;
}

void LockManager::rollbackToCheckpoint(const LockCheckpoint& checkpoint) {
    const std::thread::id self = std::this_thread::get_id();
    removeWaitEdges(self);

    {
        std::lock_guard<std::mutex> guard(globalMutex_);
        for (auto& [key, state] : locks_) {
            const auto holder = state.holderCounts.find(self);
            if (holder == state.holderCounts.end()) continue;
            const auto saved = checkpoint.tableCounts.find(key);
            const size_t target = saved == checkpoint.tableCounts.end() ? 0 : saved->second;
            const auto modeIt = state.holderModes.find(self);
            if (modeIt == state.holderModes.end()) continue;
            const auto savedMode = checkpoint.tableModes.find(key);
            const LockMode targetMode = savedMode == checkpoint.tableModes.end()
                ? modeIt->second : savedMode->second;
            if (holder->second <= target && modeIt->second == targetMode) continue;
            if (target == 0) {
                switch (modeIt->second) {
                    case LockMode::Shared:
                        state.mtx.unlock_shared();
                        --state.sharedCount;
                        break;
                    case LockMode::Exclusive:
                        state.mtx.unlock();
                        state.exclusive = false;
                        break;
                    case LockMode::IntentShared:
                        state.mtx.unlock_shared();
                        --state.intentSharedCount;
                        break;
                    case LockMode::IntentExclusive:
                        state.mtx.unlock();
                        --state.intentExclusiveCount;
                        break;
                    case LockMode::Metadata:
                        state.mtx.unlock();
                        state.metadata = false;
                        break;
                }
                state.holders.erase(std::remove(state.holders.begin(), state.holders.end(), self),
                                    state.holders.end());
                state.holderModes.erase(self);
                state.holderCounts.erase(self);
                if (state.holders.empty()) releaseProcessLock(state);
            } else {
                // If a later operation upgraded the logical mode, restore the
                // savepoint's mode before retaining its original depth.
                if (modeIt->second != targetMode) {
                    const auto processResult = tryAcquireProcessLock(
                        state, state.processLockNamespace, state.processLockKind,
                        state.processLockResource, targetMode);
                    if (processResult != ProcessLockResult::Acquired) continue;
                    const LockMode currentMode = modeIt->second;
                    switch (currentMode) {
                        case LockMode::Shared: state.mtx.unlock_shared(); --state.sharedCount; break;
                        case LockMode::Exclusive: state.mtx.unlock(); state.exclusive = false; break;
                        case LockMode::IntentShared: state.mtx.unlock_shared(); --state.intentSharedCount; break;
                        case LockMode::IntentExclusive: state.mtx.unlock(); --state.intentExclusiveCount; break;
                        case LockMode::Metadata: state.mtx.unlock(); state.metadata = false; break;
                    }
                    switch (targetMode) {
                        case LockMode::Shared: state.mtx.lock_shared(); ++state.sharedCount; break;
                        case LockMode::Exclusive: state.mtx.lock(); state.exclusive = true; break;
                        case LockMode::IntentShared: state.mtx.lock_shared(); ++state.intentSharedCount; break;
                        case LockMode::IntentExclusive: state.mtx.lock(); ++state.intentExclusiveCount; break;
                        case LockMode::Metadata: state.mtx.lock(); state.metadata = true; break;
                    }
                    modeIt->second = targetMode;
                }
                holder->second = target;
            }
        }
    }

    {
        std::lock_guard<std::mutex> guard(rowMutex_);
        for (auto it = rowLocks_.begin(); it != rowLocks_.end(); ) {
            auto& state = it->second;
            const auto hit = std::find(state.holders.begin(), state.holders.end(), self);
            const auto saved = checkpoint.rowLocks.find(it->first);
            if (hit == state.holders.end()) {
                ++it;
                continue;
            }
            if (saved != checkpoint.rowLocks.end()) {
                const LockMode currentMode = state.exclusive ? LockMode::Exclusive : LockMode::Shared;
                const LockMode targetMode = checkpoint.rowModes.at(it->first);
                if (currentMode != targetMode) {
                    const auto processResult = tryAcquireProcessLock(
                        state, state.processLockNamespace, state.processLockKind,
                        state.processLockResource, targetMode);
                    if (processResult != ProcessLockResult::Acquired) {
                        ++it;
                        continue;
                    }
                    if (currentMode == LockMode::Exclusive) {
                        state.mtx.unlock(); state.exclusive = false;
                    } else {
                        state.mtx.unlock_shared(); --state.sharedCount;
                    }
                    if (targetMode == LockMode::Exclusive) {
                        state.mtx.lock(); state.exclusive = true;
                    } else {
                        state.mtx.lock_shared(); ++state.sharedCount;
                    }
                }
                ++it;
                continue;
            }
            state.holders.erase(hit);
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            } else {
                state.mtx.unlock_shared();
                --state.sharedCount;
            }
            if (state.holders.empty()) releaseProcessLock(state);
            if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
                state.waiters == 0) it = rowLocks_.erase(it);
            else ++it;
        }
    }

    {
        std::lock_guard<std::mutex> guard(pageMutex_);
        for (auto it = pageLocks_.begin(); it != pageLocks_.end(); ) {
            auto& state = it->second;
            const auto hit = std::find(state.holders.begin(), state.holders.end(), self);
            const auto saved = checkpoint.pageLocks.find(it->first);
            if (hit == state.holders.end()) {
                ++it;
                continue;
            }
            if (saved != checkpoint.pageLocks.end()) {
                const LockMode currentMode = state.exclusive ? LockMode::Exclusive : LockMode::Shared;
                const LockMode targetMode = checkpoint.pageModes.at(it->first);
                if (currentMode != targetMode) {
                    const auto processResult = tryAcquireProcessLock(
                        state, state.processLockNamespace, state.processLockKind,
                        state.processLockResource, targetMode);
                    if (processResult != ProcessLockResult::Acquired) {
                        ++it;
                        continue;
                    }
                    if (currentMode == LockMode::Exclusive) {
                        state.mtx.unlock(); state.exclusive = false;
                    } else {
                        state.mtx.unlock_shared(); --state.sharedCount;
                    }
                    if (targetMode == LockMode::Exclusive) {
                        state.mtx.lock(); state.exclusive = true;
                    } else {
                        state.mtx.lock_shared(); ++state.sharedCount;
                    }
                }
                ++it;
                continue;
            }
            state.holders.erase(hit);
            if (state.exclusive) {
                state.mtx.unlock();
                state.exclusive = false;
            } else {
                state.mtx.unlock_shared();
                --state.sharedCount;
            }
            if (state.holders.empty()) releaseProcessLock(state);
            if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
                state.waiters == 0) it = pageLocks_.erase(it);
            else ++it;
        }
    }

    {
        std::lock_guard<std::mutex> guard(gapMutex_);
        for (auto it = gapLocks_.begin(); it != gapLocks_.end(); ) {
            const size_t target = checkpoint.gapCounts.count(it->first)
                ? checkpoint.gapCounts.at(it->first) : 0;
            size_t kept = 0;
            auto& gaps = it->second;
            gaps.erase(std::remove_if(gaps.begin(), gaps.end(),
                [&self, target, &kept](const GapLock& gap) {
                    if (gap.holder != self) return false;
                    if (kept < target) {
                        ++kept;
                        return false;
                    }
                    return true;
                }), gaps.end());
            if (gaps.empty()) {
                releaseGapProcessLock(it->first);
                it = gapLocks_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ========================================================================
// Page-level locking
// ========================================================================

static std::string makePageKey(const std::string& dbname, const std::string& table, uint32_t pageId) {
    return "page:" + dbname + ":" + table + ":" + std::to_string(pageId);
}

bool LockManager::pageLockShared(const std::string& dbname, const std::string& table, uint32_t pageId) const {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makePageKey(dbname, table, pageId);
    const auto started = std::chrono::steady_clock::now();
    while (true) {
        LockState* state = nullptr;
        {
            std::lock_guard<std::mutex> guard(pageMutex_);
            auto& current = const_cast<LockManager*>(this)->pageLocks_[key];
            if (std::find(current.holders.begin(), current.holders.end(), self) !=
                current.holders.end()) return true;
            if (!current.exclusive) {
                const auto processResult = const_cast<LockManager*>(this)->tryAcquireProcessLock(
                    current, dbname, "page", key, LockMode::Shared);
                if (processResult == ProcessLockResult::Error) return false;
                if (processResult == ProcessLockResult::Acquired) {
                    current.mtx.lock_shared();
                    ++current.sharedCount;
                    current.holders.push_back(self);
                    return true;
                }
            }
            for (const auto& holder : current.holders) {
                if (holder != self) const_cast<LockManager*>(this)->addWaitEdge(self, holder);
            }
            if (const_cast<LockManager*>(this)->hasCycle(self)) {
                const_cast<LockManager*>(this)->removeWaitEdges(self);
                return false;
            }
            ++current.waiters;
            state = &current;
        }
        state->mtx.lock_shared();
        {
            std::lock_guard<std::mutex> guard(pageMutex_);
            --state->waiters;
            const auto processResult = const_cast<LockManager*>(this)->tryAcquireProcessLock(
                *state, dbname, "page", key, LockMode::Shared);
            if (processResult == ProcessLockResult::Acquired) {
                ++state->sharedCount;
                state->holders.push_back(self);
                const_cast<LockManager*>(this)->removeWaitEdges(self);
                return true;
            }
            state->mtx.unlock_shared();
            if (state->holders.empty()) const_cast<LockManager*>(this)->releaseProcessLock(*state);
            const_cast<LockManager*>(this)->removeWaitEdges(self);
            if (processResult == ProcessLockResult::Error) return false;
        }
        const auto timeout = const_cast<LockManager*>(this)->threadSettings().lockTimeoutMs;
        if (timeout > 0 && std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started).count() >= timeout) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

bool LockManager::pageLockExclusive(const std::string& dbname, const std::string& table, uint32_t pageId) const {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makePageKey(dbname, table, pageId);
    const auto started = std::chrono::steady_clock::now();
    while (true) {
        LockState* state = nullptr;
        {
            std::lock_guard<std::mutex> guard(pageMutex_);
            auto& current = const_cast<LockManager*>(this)->pageLocks_[key];
            if (current.exclusive && current.holders.size() == 1 && current.holders[0] == self) return true;
            auto selfHolder = std::find(current.holders.begin(), current.holders.end(), self);
            if (!current.exclusive && selfHolder != current.holders.end()) {
                current.mtx.unlock_shared();
                --current.sharedCount;
                current.holders.erase(selfHolder);
                if (current.holders.empty()) const_cast<LockManager*>(this)->releaseProcessLock(current);
            }
            if (current.sharedCount == 0 && !current.exclusive) {
                const auto processResult = const_cast<LockManager*>(this)->tryAcquireProcessLock(
                    current, dbname, "page", key, LockMode::Exclusive);
                if (processResult == ProcessLockResult::Error) return false;
                if (processResult == ProcessLockResult::Acquired) {
                    current.mtx.lock();
                    current.exclusive = true;
                    current.holders.push_back(self);
                    const_cast<LockManager*>(this)->removeWaitEdges(self);
                    return true;
                }
            }
            for (const auto& holder : current.holders) {
                if (holder != self) const_cast<LockManager*>(this)->addWaitEdge(self, holder);
            }
            if (const_cast<LockManager*>(this)->hasCycle(self)) {
                const_cast<LockManager*>(this)->removeWaitEdges(self);
                return false;
            }
            ++current.waiters;
            state = &current;
        }
        state->mtx.lock();
        {
            std::lock_guard<std::mutex> guard(pageMutex_);
            --state->waiters;
            const auto processResult = const_cast<LockManager*>(this)->tryAcquireProcessLock(
                *state, dbname, "page", key, LockMode::Exclusive);
            if (processResult == ProcessLockResult::Acquired) {
                state->exclusive = true;
                state->holders.push_back(self);
                const_cast<LockManager*>(this)->removeWaitEdges(self);
                return true;
            }
            state->mtx.unlock();
            if (state->holders.empty()) const_cast<LockManager*>(this)->releaseProcessLock(*state);
            const_cast<LockManager*>(this)->removeWaitEdges(self);
            if (processResult == ProcessLockResult::Error) return false;
        }
        const auto timeout = const_cast<LockManager*>(this)->threadSettings().lockTimeoutMs;
        if (timeout > 0 && std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - started).count() >= timeout) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void LockManager::pageUnlock(const std::string& dbname, const std::string& table, uint32_t pageId) const {
    std::thread::id self = std::this_thread::get_id();
    std::string key = makePageKey(dbname, table, pageId);
    std::lock_guard<std::mutex> guard(pageMutex_);
    const_cast<LockManager*>(this)->removeWaitEdges(self);
    auto it = pageLocks_.find(key);
    if (it == pageLocks_.end()) return;
    auto& state = const_cast<LockState&>(it->second);
    auto hit = std::find(state.holders.begin(), state.holders.end(), self);
    if (hit == state.holders.end()) return;
    state.holders.erase(hit);
    if (state.exclusive) {
        state.mtx.unlock();
        state.exclusive = false;
    } else {
        state.mtx.unlock_shared();
        --state.sharedCount;
    }
    if (state.holders.empty()) const_cast<LockManager*>(this)->releaseProcessLock(state);
    if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
        state.waiters == 0) {
        pageLocks_.erase(it);
    }
}

void LockManager::pageUnlockAll(const std::string& dbname, const std::string& table) const {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(pageMutex_);
    const_cast<LockManager*>(this)->removeWaitEdges(self);
    std::string prefix = "page:" + dbname + ":" + table + ":";
    for (auto it = pageLocks_.begin(); it != pageLocks_.end(); ) {
        if (it->first.find(prefix) != 0) { ++it; continue; }
        auto& state = const_cast<LockState&>(it->second);
        auto hit = std::find(state.holders.begin(), state.holders.end(), self);
        if (hit == state.holders.end()) { ++it; continue; }
        state.holders.erase(hit);
        if (state.exclusive) {
            state.mtx.unlock();
            state.exclusive = false;
        } else {
            state.mtx.unlock_shared();
            --state.sharedCount;
        }
        if (state.holders.empty()) const_cast<LockManager*>(this)->releaseProcessLock(state);
        if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
            state.waiters == 0) {
            it = pageLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

void LockManager::pageUnlockAll() const {
    std::thread::id self = std::this_thread::get_id();
    std::lock_guard<std::mutex> guard(pageMutex_);
    const_cast<LockManager*>(this)->removeWaitEdges(self);
    for (auto it = pageLocks_.begin(); it != pageLocks_.end(); ) {
        auto& state = const_cast<LockState&>(it->second);
        auto hit = std::find(state.holders.begin(), state.holders.end(), self);
        if (hit == state.holders.end()) { ++it; continue; }
        state.holders.erase(hit);
        if (state.exclusive) {
            state.mtx.unlock();
            state.exclusive = false;
        } else {
            state.mtx.unlock_shared();
            --state.sharedCount;
        }
        if (state.holders.empty()) const_cast<LockManager*>(this)->releaseProcessLock(state);
        if (state.sharedCount == 0 && !state.exclusive && state.holders.empty() &&
            state.waiters == 0) {
            it = pageLocks_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace dbms
