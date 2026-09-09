#pragma once

#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace dbms {

enum class AdvisoryKeySpace { BigInt, IntPair };
enum class AdvisoryLockScope { Session, Transaction, Prepared };
enum class AdvisoryLockMode { Shared, Exclusive };

struct AdvisoryLockKey {
    std::string database;
    AdvisoryKeySpace keySpace = AdvisoryKeySpace::BigInt;
    int64_t first = 0;
    int64_t second = 0;

    bool operator<(const AdvisoryLockKey& other) const;
};

class AdvisoryLockManager {
public:
    bool acquire(const AdvisoryLockKey& key, uint64_t owner,
                 AdvisoryLockScope scope, AdvisoryLockMode mode,
                 bool wait);
    bool unlockSession(const AdvisoryLockKey& key, uint64_t owner,
                       AdvisoryLockMode mode);
    size_t releaseSession(uint64_t owner);
    size_t releaseTransaction(uint64_t owner);
    size_t releaseAll(uint64_t owner);

    void beginTransaction(uint64_t owner);
    void savepoint(uint64_t owner, const std::string& name);
    void releaseSavepoint(uint64_t owner, const std::string& name);
    void rollbackToSavepoint(uint64_t owner, const std::string& name);

    bool prepareTransaction(uint64_t owner, const std::string& gid);
    void finishPrepared(const std::string& gid);

private:
    struct Holder {
        uint64_t owner = 0;
        AdvisoryLockScope scope = AdvisoryLockScope::Session;
        AdvisoryLockMode mode = AdvisoryLockMode::Exclusive;
        uint64_t sequence = 0;
        std::string preparedGid;
    };
    struct Savepoint {
        std::string name;
        uint64_t sequence = 0;
    };
    struct TransactionState {
        uint64_t nextSequence = 1;
        std::vector<Savepoint> savepoints;
    };

    bool compatibleLocked(const AdvisoryLockKey& key, uint64_t owner,
                          AdvisoryLockMode mode) const;
    size_t removeLocked(uint64_t owner, bool session, bool transaction);
    void pruneEmptyLocked();

    std::mutex mutex_;
    std::condition_variable changed_;
    std::map<AdvisoryLockKey, std::vector<Holder>> holders_;
    std::map<uint64_t, TransactionState> transactions_;
    std::map<std::string, uint64_t> preparedOwners_;
    uint64_t nextPreparedOwner_ = UINT64_MAX;
};

AdvisoryLockManager& advisoryLockManager();

}  // namespace dbms
