#include "TxnIdGenerator.h"
#include "access/IndexFileUtil.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>

namespace dbms {

namespace {

constexpr uint32_t kTxnIdMagic = 0x31444958u;  // 'XID1'
constexpr uint32_t kTxnIdVersion = 1;
constexpr size_t kLegacyStateSize = sizeof(uint64_t) * 2;
constexpr size_t kStatePayloadSize = sizeof(uint32_t) * 2 + sizeof(uint64_t) * 2;
constexpr size_t kStateSize = kStatePayloadSize + sizeof(uint64_t);
// HeapTupleFields currently stores xmin/xmax as 32-bit values and there is
// no epoch/freeze scheme to disambiguate wraparound.  Do not allocate a full
// transaction ID that the durable tuple header would silently truncate.
constexpr uint64_t kMaxHeapTupleXid =
    std::numeric_limits<uint32_t>::max();

uint64_t checksum(const char* data, size_t size) {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<unsigned char>(data[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}

template <typename T>
void append(std::string& bytes, const T& value) {
    bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

template <typename T>
bool extract(const std::string& bytes, size_t& offset, T& value) {
    if (offset > bytes.size() || bytes.size() - offset < sizeof(value)) return false;
    std::memcpy(&value, bytes.data() + offset, sizeof(value));
    offset += sizeof(value);
    return true;
}

}  // namespace

TxnIdGenerator& TxnIdGenerator::instance() {
    static TxnIdGenerator gen;
    return gen;
}

TxnIdGenerator::TxnIdGenerator() {
    healthy_ = load();
}

bool TxnIdGenerator::load() {
    std::error_code ec;
    if (!std::filesystem::exists(persistPath_, ec)) return !ec;
    const uintmax_t fileSize = std::filesystem::file_size(persistPath_, ec);
    if (ec || (fileSize != kLegacyStateSize && fileSize != kStateSize)) return false;

    std::ifstream ifs(persistPath_, std::ios::binary);
    if (!ifs) return false;
    std::string bytes(static_cast<size_t>(fileSize), '\0');
    if (!ifs.read(bytes.data(), static_cast<std::streamsize>(bytes.size())) ||
        ifs.peek() != std::char_traits<char>::eof()) {
        return false;
    }

    uint64_t parsedNext = 0;
    uint64_t parsedHighWater = 0;
    size_t offset = 0;
    if (bytes.size() == kLegacyStateSize) {
        if (!extract(bytes, offset, parsedNext) ||
            !extract(bytes, offset, parsedHighWater)) {
            return false;
        }
    } else {
        uint32_t magic = 0;
        uint32_t version = 0;
        uint64_t storedChecksum = 0;
        if (!extract(bytes, offset, magic) || !extract(bytes, offset, version) ||
            !extract(bytes, offset, parsedNext) ||
            !extract(bytes, offset, parsedHighWater) ||
            !extract(bytes, offset, storedChecksum) || magic != kTxnIdMagic ||
            version != kTxnIdVersion ||
            storedChecksum != checksum(bytes.data(), kStatePayloadSize)) {
            return false;
        }
    }
    if (offset != bytes.size() || parsedNext == 0 ||
        parsedHighWater >= parsedNext) {
        return false;
    }

    nextTxId_ = parsedNext;
    // Old files tracked only committed transactions.  Advancing to the last
    // allocated ID is conservative for snapshot xmax and prevents an aborted
    // or prepared gap from lowering the horizon after restart.
    maxCommitted_ = std::max(parsedHighWater, parsedNext - 1);
    return true;
}

bool TxnIdGenerator::save(uint64_t nextTxId,
                          uint64_t allocationHighWater) const {
    std::string bytes;
    bytes.reserve(kStateSize);
    append(bytes, kTxnIdMagic);
    append(bytes, kTxnIdVersion);
    append(bytes, nextTxId);
    append(bytes, allocationHighWater);
    const uint64_t stateChecksum = checksum(bytes.data(), bytes.size());
    append(bytes, stateChecksum);
    return index_file::writeAtomically(persistPath_, bytes);
}

uint64_t TxnIdGenerator::nextTxId() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!healthy_ || nextTxId_ > kMaxHeapTupleXid) return 0;

    const uint64_t id = nextTxId_;
    const uint64_t persistedNext = id + 1;
    const uint64_t persistedHighWater = std::max(maxCommitted_, id);
    if (!save(persistedNext, persistedHighWater)) return 0;

    nextTxId_ = persistedNext;
    maxCommitted_ = persistedHighWater;
    return id;
}

uint64_t TxnIdGenerator::maxCommittedTxId() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return maxCommitted_;
}

void TxnIdGenerator::notifyCommit(uint64_t txId) {
    std::lock_guard<std::mutex> lock(mtx_);
    // Allocation is the durability point for both nextTxId and the snapshot
    // horizon, so COMMIT does not need a second file rewrite.  Keep this
    // update for legacy/recovery callers that notify an older allocated ID.
    if (healthy_ && txId != 0 && txId < nextTxId_ && txId > maxCommitted_) {
        maxCommitted_ = txId;
    }
}

} // namespace dbms
