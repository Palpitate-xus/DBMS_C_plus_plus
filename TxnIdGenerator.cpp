#include "TxnIdGenerator.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>

namespace dbms {

TxnIdGenerator& TxnIdGenerator::instance() {
    static TxnIdGenerator gen;
    return gen;
}

TxnIdGenerator::TxnIdGenerator() {
    load();
}

void TxnIdGenerator::load() {
    if (!std::filesystem::exists(persistPath_)) return;
    std::ifstream ifs(persistPath_, std::ios::binary);
    if (!ifs) return;
    ifs.read(reinterpret_cast<char*>(&nextTxId_), sizeof(nextTxId_));
    ifs.read(reinterpret_cast<char*>(&maxCommitted_), sizeof(maxCommitted_));
    if (nextTxId_ == 0) nextTxId_ = 1;
}

void TxnIdGenerator::save() {
    std::ofstream ofs(persistPath_, std::ios::binary);
    if (!ofs) return;
    ofs.write(reinterpret_cast<const char*>(&nextTxId_), sizeof(nextTxId_));
    ofs.write(reinterpret_cast<const char*>(&maxCommitted_), sizeof(maxCommitted_));
    ofs.close();
    int fd = ::open(persistPath_.c_str(), O_RDWR);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
}

uint64_t TxnIdGenerator::nextTxId() {
    std::lock_guard<std::mutex> lock(mtx_);
    uint64_t id = nextTxId_++;
    save();
    return id;
}

uint64_t TxnIdGenerator::maxCommittedTxId() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return maxCommitted_;
}

void TxnIdGenerator::notifyCommit(uint64_t txId) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (txId > maxCommitted_) {
        maxCommitted_ = txId;
        save();
    }
}

} // namespace dbms
