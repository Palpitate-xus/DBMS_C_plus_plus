#include "CommitLog.h"

#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/file.h>
#include <unistd.h>

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
    // A database may have been removed by an embedded caller while this
    // cache entry was still alive. Do not recreate the deleted directory
    // merely because destruction is flushing an obsolete commit log.
    std::error_code ec;
    if (!dataDir_.empty() && std::filesystem::exists(dataDir_, ec) && !ec) {
        (void)flush();
    }
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
        std::error_code ec;
        seg.fileTime = std::filesystem::last_write_time(path, ec);
        seg.fileTimeValid = !ec;
    }

    segments_[segNo] = std::move(seg);
}

void CommitLog::refreshSegmentIfChanged(uint64_t segNo) const {
    auto it = segments_.find(segNo);
    if (it == segments_.end() || it->second.dirty) return;

    const std::string path = segmentPath(segNo);
    std::error_code ec;
    if (!std::filesystem::exists(path, ec) || ec) {
        if (it->second.fileTimeValid) {
            it->second.data.assign(kSegmentFileSize, 0);
            it->second.fileTimeValid = false;
        }
        return;
    }

    const auto stamp = std::filesystem::last_write_time(path, ec);
    if (ec || (it->second.fileTimeValid && it->second.fileTime == stamp)) return;

    std::vector<uint8_t> data(kSegmentFileSize, 0);
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) return;
    ifs.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(kSegmentFileSize));
    it->second.data = std::move(data);
    it->second.fileTime = stamp;
    it->second.fileTimeValid = true;
}

void CommitLog::ensureSegment(uint64_t segNo) const {
    loadSegment(segNo);
}

bool CommitLog::saveSegment(uint64_t segNo, int heldLockFd) {
    auto it = segments_.find(segNo);
    if (it == segments_.end() || !it->second.dirty) return true;

    const std::string path = segmentPath(segNo);

    // A test or an administrative drop may remove the database directory
    // while an uncached owner still exists. Never recreate a dropped database
    // or write stale CLOG state into a later same-name database.
    const std::filesystem::path clogDir = std::filesystem::path(dataDir_) / "pg_xact";
    if (!std::filesystem::is_directory(dataDir_) || !std::filesystem::is_directory(clogDir)) {
        std::cerr << "[CLOG] Missing data directory while flushing segment: " << path << std::endl;
        return false;
    }

    const bool ownsLock = heldLockFd < 0;
    int lockFd = heldLockFd;
    if (ownsLock) {
        const std::string lockPath = (clogDir / ".clog.lock").string();
        lockFd = ::open(lockPath.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
        if (lockFd < 0 || ::flock(lockFd, LOCK_EX) != 0) {
            if (lockFd >= 0) ::close(lockFd);
            std::cerr << "[CLOG] Failed to lock segment: " << path << std::endl;
            return false;
        }
    }
    if (lockFd < 0) {
        std::cerr << "[CLOG] Failed to lock segment: " << path << std::endl;
        return false;
    }
    auto releaseLock = [&]() {
        if (ownsLock) {
            ::flock(lockFd, LOCK_UN);
            ::close(lockFd);
        }
    };

    // Merge only the bits changed by this backend with the latest complete
    // segment.  Replacing a whole stale in-memory segment could erase a
    // transaction status written by another backend between cache loads.
    std::vector<uint8_t> latest(kSegmentFileSize, 0);
    if (std::filesystem::exists(path)) {
        std::ifstream ifs(path, std::ios::binary);
        if (ifs) {
            ifs.read(reinterpret_cast<char*>(latest.data()),
                     static_cast<std::streamsize>(kSegmentFileSize));
        }
    }
    if (it->second.pendingBits.empty()) {
        latest = it->second.data;
    } else {
        for (const auto& [offset, update] : it->second.pendingBits) {
            if (offset >= latest.size()) continue;
            latest[offset] = static_cast<uint8_t>((latest[offset] & ~update.first) |
                                                   (update.second & update.first));
        }
    }

    const std::string tempPath = path + ".tmp." + std::to_string(static_cast<unsigned long long>(::getpid())) +
                                 "." + std::to_string(reinterpret_cast<uintptr_t>(this));
    const int fd = ::open(tempPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) {
        releaseLock();
        std::cerr << "[CLOG] Failed to write segment: " << path << std::endl;
        return false;
    }

    const uint8_t* data = latest.data();
    size_t remaining = kSegmentFileSize;
    bool ok = true;
    while (remaining > 0) {
        const ssize_t written = ::write(fd, data, remaining);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) {
            ok = false;
            break;
        }
        data += written;
        remaining -= static_cast<size_t>(written);
    }
    if (ok && ::fsync(fd) != 0) ok = false;
    if (::close(fd) != 0) ok = false;

    if (ok && ::rename(tempPath.c_str(), path.c_str()) != 0) ok = false;
    if (ok) {
        const int dirFd = ::open(clogDir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dirFd < 0 || ::fsync(dirFd) != 0) ok = false;
        if (dirFd >= 0) ::close(dirFd);
    }
    if (!ok) {
        std::error_code ec;
        std::filesystem::remove(tempPath, ec);
        releaseLock();
        std::cerr << "[CLOG] Failed to durably write segment: " << path << std::endl;
        return false;
    }
    releaseLock();
    it->second.data = std::move(latest);
    it->second.pendingBits.clear();
    std::error_code ec;
    it->second.fileTime = std::filesystem::last_write_time(path, ec);
    it->second.fileTimeValid = !ec;
    it->second.dirty = false;
    return true;
}

CommitLog::Status CommitLog::getStatus(TxnId xid) const {
    if (xid == INVALID_TXN_ID) return Status::InProgress;

    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t segNo = segmentNumber(xid);
    ensureSegment(segNo);
    refreshSegmentIfChanged(segNo);

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
    auto& pending = seg.pendingBits[off];
    pending.first = static_cast<uint8_t>(pending.first | mask);
    pending.second = static_cast<uint8_t>((pending.second & ~mask) |
                                           (statusBits(status) << shift));
    seg.dirty = true;
}

bool CommitLog::setStatuses(const std::vector<std::pair<TxnId, Status>>& entries) {
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
            auto& pending = seg.pendingBits[off];
            pending.first = static_cast<uint8_t>(pending.first | mask);
            pending.second = static_cast<uint8_t>((pending.second & ~mask) |
                                                   (statusBits(status) << shift));
            seg.dirty = true;
        }
    }
    return flush();
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
        const std::filesystem::path clogDir = std::filesystem::path(dataDir_) / "pg_xact";
        const std::string lockPath = (clogDir / ".clog.lock").string();
        const int lockFd = ::open(lockPath.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
        if (lockFd < 0 || ::flock(lockFd, LOCK_EX) != 0) {
            if (lockFd >= 0) ::close(lockFd);
            continue;
        }

        const bool saved = saveSegment(segNo, lockFd);
        bool deleted = false;
        bool removed = false;
        if (saved) {
            std::error_code ec;
            deleted = std::filesystem::remove(segmentPath(segNo), ec) && !ec;
            removed = deleted;
            if (!removed && !ec) {
                std::error_code existsEc;
                removed = !std::filesystem::exists(segmentPath(segNo), existsEc) && !existsEc;
            }
            if (removed) {
                const int dirFd = ::open(clogDir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
                if (dirFd < 0 || ::fsync(dirFd) != 0) removed = false;
                if (dirFd >= 0) ::close(dirFd);
            }
        }
        ::flock(lockFd, LOCK_UN);
        ::close(lockFd);
        if (saved && removed) {
            segments_.erase(segNo);
        } else if (saved && deleted) {
            // The unlink happened but directory durability is uncertain. Keep
            // the in-memory image authoritative for this backend.
            auto it = segments_.find(segNo);
            if (it != segments_.end()) it->second.fileTimeValid = false;
        }
    }
}

bool CommitLog::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    bool ok = true;
    for (const auto& [segNo, _] : segments_) {
        if (!saveSegment(segNo)) ok = false;
    }
    return ok;
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
