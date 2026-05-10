#pragma once

#include <cstdint>
#include <mutex>
#include <string>

namespace dbms {

// Persistent 64-bit transaction ID generator.
// Thread-safe singleton. Stores nextTxId and maxCommitted in a file.
class TxnIdGenerator {
public:
    static TxnIdGenerator& instance();

    // Allocate and return the next unique transaction ID.
    uint64_t nextTxId();

    // Get the highest committed transaction ID.
    uint64_t maxCommittedTxId() const;

    // Called on COMMIT to update the max committed txId.
    void notifyCommit(uint64_t txId);

private:
    TxnIdGenerator();
    ~TxnIdGenerator() = default;
    TxnIdGenerator(const TxnIdGenerator&) = delete;
    TxnIdGenerator& operator=(const TxnIdGenerator&) = delete;

    mutable std::mutex mtx_;
    uint64_t nextTxId_ = 1;
    uint64_t maxCommitted_ = 0;
    std::string persistPath_ = ".txnid";

    void load();
    void save();
};

} // namespace dbms
