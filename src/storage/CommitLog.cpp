#include "CommitLog.h"

#include <filesystem>
#include <fstream>
#include <iostream>

namespace dbms {

CommitLog::CommitLog(const std::string& dataDir)
    : dataDir_(dataDir) {
    if (!dataDir_.empty() && dataDir_.back() == '/') {
        dataDir_.pop_back();
    }
    // 确保 pg_xact 目录存在
    std::filesystem::create_directories(dataDir_ + "/pg_xact");
}

CommitLog::~CommitLog() {
    flush();
}

std::string CommitLog::segmentPath(uint64_t segNo) const {
    std::ostringstream oss;
    oss << dataDir_ << "/pg_xact/" << std::hex << segNo;
    return oss.str();
}

uint64_t CommitLog::segmentNumber(TxnId xid) {
    return xid / kXidsPerSegment;
}

size_t CommitLog::byteOffset(TxnId xid) {
    return static_cast<size_t>((xid % kXidsPerSegment) / 4);
}

uint8_t CommitLog::shiftForXid(TxnId xid) {
    return static_cast<uint8_t>((xid % 4) * 2);
}

uint8_t CommitLog::statusBits(Status s) {
    return static_cast<uint8_t>(s) & 0x03;
}

CommitLog::Status CommitLog::bitsToStatus(uint8_t bits) {
    return static_cast<Status>(bits & 0x03);
}

void CommitLog::loadSegment(uint64_t segNo) const {
    auto it = segments_.find(segNo);
    if (it != segments_.end()) return;

    Segment seg;
    seg.data.assign(kSegmentFileSize, 0);

    std::string path = segmentPath(segNo);
    if (std::filesystem::exists(path)) {
        std::ifstream ifs(path, std::ios::binary);
        if (ifs) {
            ifs.read(reinterpret_cast<char*>(seg.data.data()),
                     static_cast<std::streamsize>(kSegmentFileSize));
            // 未读满则剩余保持为 0（InProgress）
        }
    }

    segments_[segNo] = std::move(seg);
}

void CommitLog::ensureSegment(uint64_t segNo) const {
    loadSegment(segNo);
}

void CommitLog::saveSegment(uint64_t segNo) {
    auto it = segments_.find(segNo);
    if (it == segments_.end() || !it->second.dirty) return;

    std::string path = segmentPath(segNo);
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs) {
        std::cerr << "[CLOG] Failed to write segment: " << path << std::endl;
        return;
    }
    ofs.write(reinterpret_cast<const char*>(it->second.data.data()),
              static_cast<std::streamsize>(kSegmentFileSize));
    ofs.flush();
    it->second.dirty = false;
}

CommitLog::Status CommitLog::getStatus(TxnId xid) const {
    if (xid == INVALID_TXN_ID) return Status::InProgress;

    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t segNo = segmentNumber(xid);
    ensureSegment(segNo);

    const Segment& seg = segments_[segNo];
    size_t off = byteOffset(xid);
    uint8_t shift = shiftForXid(xid);
    uint8_t bits = (seg.data[off] >> shift) & 0x03;
    return bitsToStatus(bits);
}

void CommitLog::setStatus(TxnId xid, Status status) {
    if (xid == INVALID_TXN_ID) return;

    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t segNo = segmentNumber(xid);
    ensureSegment(segNo);

    Segment& seg = segments_[segNo];
    size_t off = byteOffset(xid);
    uint8_t shift = shiftForXid(xid);
    uint8_t mask = static_cast<uint8_t>(0x03 << shift);
    seg.data[off] = static_cast<uint8_t>((seg.data[off] & ~mask) | (statusBits(status) << shift));
    seg.dirty = true;
}

void CommitLog::setStatuses(const std::vector<std::pair<TxnId, Status>>& entries) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [xid, status] : entries) {
            if (xid == INVALID_TXN_ID) continue;
            uint64_t segNo = segmentNumber(xid);
            ensureSegment(segNo);
            Segment& seg = segments_[segNo];
            size_t off = byteOffset(xid);
            uint8_t shift = shiftForXid(xid);
            uint8_t mask = static_cast<uint8_t>(0x03 << shift);
            seg.data[off] = static_cast<uint8_t>((seg.data[off] & ~mask) | (statusBits(status) << shift));
            seg.dirty = true;
        }
    }
    flush();
}

void CommitLog::truncate(TxnId oldestXid) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (oldestXid <= kXidsPerSegment) return; // 至少保留第一段

    uint64_t maxSeg = segmentNumber(oldestXid);
    std::vector<uint64_t> toRemove;
    for (const auto& [segNo, _] : segments_) {
        if (segNo < maxSeg) {
            toRemove.push_back(segNo);
        }
    }
    for (uint64_t segNo : toRemove) {
        saveSegment(segNo); // 刷盘后再删
        segments_.erase(segNo);
        std::string path = segmentPath(segNo);
        try {
            std::filesystem::remove(path);
        } catch (...) {}
    }
}

void CommitLog::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& [segNo, _] : segments_) {
        saveSegment(segNo);
    }
}

const char* CommitLog::statusName(Status s) {
    switch (s) {
        case Status::InProgress:   return "in_progress";
        case Status::Committed:    return "committed";
        case Status::Aborted:      return "aborted";
        case Status::SubCommitted: return "sub_committed";
    }
    return "unknown";
}

} // namespace dbms
