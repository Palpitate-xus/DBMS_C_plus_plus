#include "WAL.h"
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <iostream>

namespace dbms {

namespace {

// Simple CRC32C implementation (table-less, small)
uint32_t crc32c(const char* data, size_t len) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc ^= static_cast<uint8_t>(data[i]);
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0x82F63B78u & static_cast<uint32_t>(-(crc & 1u)));
        }
    }
    return crc ^ 0xFFFFFFFFu;
}

uint64_t alignLen(uint64_t len, uint64_t align) {
    return (len + align - 1) & ~(align - 1);
}

} // namespace

WALManager::WALManager(const std::filesystem::path& walDir)
    : walDir_(walDir), currentLsn_(0) {}

WALManager::~WALManager() = default;

bool WALManager::ensureOpen() {
    if (open_) return true;
    std::error_code ec;
    std::filesystem::create_directories(walDir_, ec);
    if (ec) return false;
    std::filesystem::create_directories(archiveStatusDir(), ec);

    // Load or infer timeline ID.
    loadTimeline();
    if (!std::filesystem::exists(timelinePath())) {
        persistTimeline();
    }

    // Scan existing segments to find current LSN (end of last segment)
    currentLsn_ = 0;
    uint32_t maxSeg = 0;
    bool found = false;
    for (const auto& entry : std::filesystem::directory_iterator(walDir_)) {
        std::string name = entry.path().filename().string();
        if (name.size() != 24) continue;
        try {
            // Segment filename: TLI(8) + log(8) + seg(8) hex
            uint32_t tli = static_cast<uint32_t>(
                std::stoull(name.substr(0, 8), nullptr, 16));
            uint32_t segNo = static_cast<uint32_t>(
                std::stoull(name.substr(16, 8), nullptr, 16));
            if (tli == timelineId_ && segNo >= maxSeg) {
                maxSeg = segNo;
                found = true;
            }
        } catch (...) {}
    }
    if (found) {
        auto path = segmentPath(maxSeg);
        auto size = std::filesystem::file_size(path);
        currentLsn_ = static_cast<Lsn>(maxSeg) * kSegmentSize + size;
    }
    open_ = true;
    return true;
}

std::filesystem::path WALManager::segmentPath(uint32_t segNo) const {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%08X%08X%08X", timelineId_, 0u, segNo);
    return walDir_ / buf;
}

bool WALManager::loadTimeline() {
    auto path = timelinePath();
    if (!std::filesystem::exists(path)) {
        timelineId_ = 1;
        return true;
    }
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) return false;
    uint32_t tli = 1;
    ifs.read(reinterpret_cast<char*>(&tli), sizeof(tli));
    if (ifs.gcount() == sizeof(tli) && tli != 0) {
        timelineId_ = tli;
        return true;
    }
    // Fallback: infer from segment filenames.
    for (const auto& entry : std::filesystem::directory_iterator(walDir_)) {
        std::string name = entry.path().filename().string();
        if (name.size() == 24) {
            try {
                uint32_t tli = static_cast<uint32_t>(
                    std::stoull(name.substr(0, 8), nullptr, 16));
                if (tli != 0) {
                    timelineId_ = tli;
                    return true;
                }
            } catch (...) {}
        }
    }
    timelineId_ = 1;
    return true;
}

bool WALManager::persistTimeline() {
    auto path = timelinePath();
    std::ofstream ofs(path, std::ios::binary);
    if (!ofs) return false;
    uint32_t tli = timelineId_;
    ofs.write(reinterpret_cast<const char*>(&tli), sizeof(tli));
    return ofs.good();
}

bool WALManager::setTimeline(uint32_t tli) {
    if (tli == 0) return false;
    if (!ensureOpen()) return false;
    timelineId_ = tli;
    return persistTimeline();
}

std::filesystem::path WALManager::readyPath(uint32_t segNo) const {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%08X%08X%08X.ready", timelineId_, 0u, segNo);
    return archiveStatusDir() / buf;
}

std::filesystem::path WALManager::donePath(uint32_t segNo) const {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%08X%08X%08X.done", timelineId_, 0u, segNo);
    return archiveStatusDir() / buf;
}

bool WALManager::markSegmentReadyForArchive(uint32_t segNo) {
    if (!ensureOpen()) return false;
    std::error_code ec;
    std::filesystem::create_directories(archiveStatusDir(), ec);
    std::ofstream ofs(readyPath(segNo), std::ios::binary);
    return ofs.good();
}

bool WALManager::markSegmentArchived(uint32_t segNo) {
    if (!ensureOpen()) return false;
    std::error_code ec;
    std::filesystem::remove(readyPath(segNo), ec);
    std::ofstream ofs(donePath(segNo), std::ios::binary);
    return ofs.good();
}

bool WALManager::isSegmentArchived(uint32_t segNo) const {
    return std::filesystem::exists(donePath(segNo));
}

std::vector<uint32_t> WALManager::pendingArchiveSegments() const {
    std::vector<uint32_t> result;
    if (!open_) return result;
    if (!std::filesystem::exists(archiveStatusDir())) return result;
    for (const auto& entry : std::filesystem::directory_iterator(archiveStatusDir())) {
        std::string name = entry.path().filename().string();
        // Segment filename is 24 hex chars; .ready suffix makes it 30.
        if (name.size() != 30 || name.substr(24, 6) != ".ready") continue;
        try {
            uint32_t tli = static_cast<uint32_t>(
                std::stoull(name.substr(0, 8), nullptr, 16));
            if (tli != timelineId_) continue;
            uint32_t segNo = static_cast<uint32_t>(
                std::stoull(name.substr(16, 8), nullptr, 16));
            if (!isSegmentArchived(segNo)) {
                result.push_back(segNo);
            }
        } catch (...) {}
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

bool WALManager::archiveSegment(uint32_t segNo, const std::filesystem::path& archiveDir) {
    if (!ensureOpen()) return false;
    auto src = segmentPath(segNo);
    if (!std::filesystem::exists(src)) return false;
    std::error_code ec;
    std::filesystem::create_directories(archiveDir, ec);
    if (ec) return false;
    auto dst = archiveDir / src.filename();
    try {
        std::filesystem::copy_file(src, dst,
            std::filesystem::copy_options::overwrite_existing);
    } catch (...) {
        return false;
    }
    return markSegmentArchived(segNo);
}

bool WALManager::archivePendingSegments(const std::filesystem::path& archiveDir) {
    auto pending = pendingArchiveSegments();
    bool ok = true;
    for (uint32_t segNo : pending) {
        if (!archiveSegment(segNo, archiveDir)) ok = false;
    }
    return ok;
}

bool WALManager::markSegmentsReadyBefore(Lsn lsn) {
    if (!ensureOpen() || lsn == INVALID_LSN) return false;
    uint32_t endSeg = segmentNumber(lsn);
    for (uint32_t seg = 0; seg < endSeg; ++seg) {
        if (!markSegmentReadyForArchive(seg)) return false;
    }
    return true;
}

void WALManager::advanceCurrentLsn(uint32_t len) {
    currentLsn_ += len;
}

uint32_t WALManager::computeCrc(const char* data, size_t len) const {
    return crc32c(data, len);
}

bool WALManager::verifyCrc(const XLogRecord& rec) const {
    uint32_t totLen = rec.header.xl_tot_len;
    std::string buf;
    buf.reserve(totLen);
    XLogRecHeader headerCopy = rec.header;
    headerCopy.xl_crc = 0;
    buf.append(reinterpret_cast<const char*>(&headerCopy), sizeof(XLogRecHeader));
    buf.append(rec.data.data(), rec.data.size());
    uint32_t expected = computeCrc(buf.data(), buf.size());
    return expected == rec.header.xl_crc;
}

bool WALManager::ensureSegment(uint32_t segNo) {
    auto path = segmentPath(segNo);
    if (std::filesystem::exists(path)) return true;
    std::ofstream ofs(path, std::ios::binary);
    return ofs.good();
}

bool WALManager::appendBytes(const char* buf, size_t len) {
    if (!ensureOpen()) return false;
    uint64_t offset = currentLsn_;
    uint64_t remaining = len;
    const char* ptr = buf;
    while (remaining > 0) {
        uint32_t segNo = segmentNumber(offset);
        uint32_t off = segmentOffset(offset);
        if (!ensureSegment(segNo)) return false;
        auto path = segmentPath(segNo);
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd < 0) return false;
        if (::lseek(fd, off, SEEK_SET) < 0) {
            ::close(fd);
            return false;
        }
        uint64_t chunk = std::min(remaining, kSegmentSize - off);
        ssize_t wrote = ::write(fd, ptr, static_cast<size_t>(chunk));
        ::close(fd);
        if (wrote < 0 || static_cast<uint64_t>(wrote) != chunk) return false;
        ptr += chunk;
        offset += chunk;
        remaining -= chunk;
    }
    return true;
}

bool WALManager::readBytes(uint64_t offset, char* buf, size_t len) const {
    uint64_t remaining = len;
    char* ptr = buf;
    while (remaining > 0) {
        uint32_t segNo = segmentNumber(offset);
        uint32_t off = segmentOffset(offset);
        auto path = segmentPath(segNo);
        if (!std::filesystem::exists(path)) return false;
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;
        if (::lseek(fd, off, SEEK_SET) < 0) {
            ::close(fd);
            return false;
        }
        uint64_t chunk = std::min(remaining, kSegmentSize - off);
        ssize_t n = ::read(fd, ptr, static_cast<size_t>(chunk));
        ::close(fd);
        if (n < 0 || static_cast<uint64_t>(n) != chunk) return false;
        ptr += chunk;
        offset += chunk;
        remaining -= chunk;
    }
    return true;
}

bool WALManager::syncSegment(uint32_t segNo) {
    auto path = segmentPath(segNo);
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) return false;
    int r = ::fsync(fd);
    ::close(fd);
    return r == 0;
}

Lsn WALManager::XLogInsert(uint8_t rmid, uint8_t info, uint64_t xid,
                           const std::vector<char>& data) {
    if (!ensureOpen()) return INVALID_LSN;

    uint32_t payloadLen = static_cast<uint32_t>(data.size());
    uint32_t totalLen = static_cast<uint32_t>(
        alignLen(sizeof(XLogRecHeader) + payloadLen, MAXALIGN));

    XLogRecHeader header;
    header.xl_prev = 0;
    header.xl_tot_len = totalLen;
    header.xl_info = (static_cast<uint32_t>(rmid) << 8) | info;
    header.xl_xid = xid;
    header.xl_crc = 0;

    std::string record;
    record.reserve(totalLen);
    record.append(reinterpret_cast<const char*>(&header), sizeof(header));
    record.append(data.data(), data.size());
    record.resize(totalLen, '\0');

    XLogRecHeader* h = reinterpret_cast<XLogRecHeader*>(record.data());
    h->xl_crc = computeCrc(record.data(), record.size());

    Lsn recLsn = currentLsn_;
    uint32_t prevSeg = segmentNumber(recLsn);
    if (!appendBytes(record.data(), record.size())) return INVALID_LSN;
    advanceCurrentLsn(totalLen);
    uint32_t newSeg = segmentNumber(currentLsn_);
    if (newSeg != prevSeg) {
        // The previous segment is now complete; mark it ready for archiving.
        markSegmentReadyForArchive(prevSeg);
    }
    return recLsn;
}

void WALManager::XLogFlush(Lsn targetLsn) {
    if (!ensureOpen() || targetLsn == INVALID_LSN) return;
    uint32_t startSeg = segmentNumber(0);
    uint32_t endSeg = segmentNumber(targetLsn);
    for (uint32_t seg = startSeg; seg <= endSeg; ++seg) {
        syncSegment(seg);
    }
}

std::optional<XLogRecord> WALManager::ReadRecord(Lsn lsn) const {
    if (!open_ || lsn >= currentLsn_) return std::nullopt;
    XLogRecHeader header;
    if (!readBytes(lsn, reinterpret_cast<char*>(&header), sizeof(header))) {
        return std::nullopt;
    }
    if (header.xl_tot_len < sizeof(header) || header.xl_tot_len > 1u << 30) {
        return std::nullopt;
    }
    XLogRecord rec;
    rec.header = header;
    rec.data.resize(header.xl_tot_len - sizeof(header));
    if (!readBytes(lsn + sizeof(header), rec.data.data(), rec.data.size())) {
        return std::nullopt;
    }
    if (!verifyCrc(rec)) return std::nullopt;
    return rec;
}

std::optional<XLogRecord> WALManager::ReadNextRecord(Lsn lsn) const {
    if (lsn == 0) {
        // First record is at LSN 0? Actually records start at 0, but header begins at 0.
        return ReadRecord(0);
    }
    auto rec = ReadRecord(lsn);
    if (!rec) return std::nullopt;
    return ReadRecord(lsn + rec->header.xl_tot_len);
}

std::optional<Lsn> WALManager::findLastCheckpointLsn() const {
    Lsn lsn = 0;
    std::optional<Lsn> lastCp;
    while (true) {
        auto rec = ReadRecord(lsn);
        if (!rec) break;
        if (rec->rmid() == RM_CHECKPOINT_ID) {
            lastCp = lsn;
        }
        lsn += rec->header.xl_tot_len;
    }
    return lastCp;
}

std::optional<XLogRecord> WALManager::readCheckpoint(Lsn lsn) const {
    return ReadRecord(lsn);
}

void WALManager::reset() {
    if (!ensureOpen()) return;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(walDir_)) {
        std::filesystem::remove_all(entry.path(), ec);
    }
    currentLsn_ = 0;
    timelineId_ = 1;
}

// ============================================================================
// Helpers for decoding common payload layouts
// ============================================================================

namespace {

// Payload layout for HEAP records:
//   uint32_t nameLen
//   nameLen bytes table name
//   uint32_t blockNum
//   uint32_t forkNum
//   uint16_t slotId
//   [HEAP_INSERT/UPDATE:] uint32_t rowLen + rowData
//   [HEAP_DELETE:] uint64_t xmax
//   [HEAP_UPDATE:] uint32_t newBlockNum, uint32_t newForkNum, uint16_t newSlotId

bool readU32(const char*& p, const char* end, uint32_t& out) {
    if (p + sizeof(uint32_t) > end) return false;
    std::memcpy(&out, p, sizeof(out));
    p += sizeof(out);
    return true;
}
bool readU16(const char*& p, const char* end, uint16_t& out) {
    if (p + sizeof(uint16_t) > end) return false;
    std::memcpy(&out, p, sizeof(out));
    p += sizeof(out);
    return true;
}
bool readU64(const char*& p, const char* end, uint64_t& out) {
    if (p + sizeof(uint64_t) > end) return false;
    std::memcpy(&out, p, sizeof(out));
    p += sizeof(out);
    return true;
}
bool readString(const char*& p, const char* end, std::string& out) {
    uint32_t len = 0;
    if (!readU32(p, end, len)) return false;
    if (p + len > end) return false;
    out.assign(p, len);
    p += len;
    return true;
}

} // namespace

bool RedoApplyRecord(const XLogRecord& rec,
                     const std::function<bool(const std::string& tableName,
                                              uint32_t blockNum, uint32_t forkNum,
                                              const char* pageData, size_t pageLen)>& redoPageImage,
                     const std::function<bool(const std::string& tableName,
                                              uint32_t blockNum, uint32_t forkNum,
                                              uint16_t slotId, const char* rowData, size_t rowLen)>& redoInsert,
                     const std::function<bool(const std::string& tableName,
                                              uint32_t blockNum, uint32_t forkNum,
                                              uint16_t slotId, uint64_t xmax)>& redoDelete,
                     const std::function<bool(const std::string& tableName,
                                              uint32_t oldBlock, uint32_t oldFork,
                                              uint16_t oldSlot,
                                              uint32_t newBlock, uint32_t newFork,
                                              uint16_t newSlot,
                                              const char* rowData, size_t rowLen)>& redoUpdate) {
    const char* p = rec.data.data();
    const char* end = p + rec.data.size();
    uint8_t rmid = rec.rmid();
    uint8_t info = rec.info();

    if (rmid == RM_HEAP_ID) {
        std::string tableName;
        if (!readString(p, end, tableName)) return false;
        uint32_t blockNum = 0, forkNum = 0;
        uint16_t slotId = 0;
        if (!readU32(p, end, blockNum)) return false;
        if (!readU32(p, end, forkNum)) return false;
        if (!readU16(p, end, slotId)) return false;

        if (info == XLOG_HEAP_PAGE_BEFORE || info == XLOG_HEAP_PAGE_AFTER) {
            uint32_t pageLen = 0;
            if (!readU32(p, end, pageLen)) return false;
            if (p + pageLen > end) return false;
            return redoPageImage(tableName, blockNum, forkNum, p, pageLen);
        } else if (info == XLOG_HEAP_INSERT) {
            uint32_t rowLen = 0;
            if (!readU32(p, end, rowLen)) return false;
            if (p + rowLen > end) return false;
            return redoInsert(tableName, blockNum, forkNum, slotId, p, rowLen);
        } else if (info == XLOG_HEAP_DELETE) {
            uint64_t xmax = 0;
            if (!readU64(p, end, xmax)) return false;
            return redoDelete(tableName, blockNum, forkNum, slotId, xmax);
        } else if (info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE) {
            uint32_t newBlock = 0, newFork = 0;
            uint16_t newSlot = 0;
            uint32_t rowLen = 0;
            if (!readU32(p, end, newBlock)) return false;
            if (!readU32(p, end, newFork)) return false;
            if (!readU16(p, end, newSlot)) return false;
            if (!readU32(p, end, rowLen)) return false;
            if (p + rowLen > end) return false;
            return redoUpdate(tableName, blockNum, forkNum, slotId, newBlock, newFork, newSlot, p, rowLen);
        }
        return false;
    } else if (rmid == RM_XACT_ID) {
        return true;
    } else if (rmid == RM_SMGR_ID) {
        return true;
    } else if (rmid == RM_CHECKPOINT_ID) {
        return true;
    }
    return false;
}

} // namespace dbms
