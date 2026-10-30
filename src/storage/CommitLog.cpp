#include "CommitLog.h"
#include "common/DbError.h"

#include <algorithm>
#include <charconv>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <set>
#include <string>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace dbms {

namespace {
enum class SegmentRead { Missing, Loaded, Failed };

struct SegmentReadDescriptor {
    int fd;
    ~SegmentReadDescriptor() { ::close(fd); }
};

template<class Segment>
void rememberGeneration(Segment& segment, const struct stat& identity) {
    segment.fileDevice = static_cast<uint64_t>(identity.st_dev);
    segment.fileInode = static_cast<uint64_t>(identity.st_ino);
    segment.fileSize = static_cast<uint64_t>(identity.st_size);
    segment.modifiedSeconds = identity.st_mtim.tv_sec;
    segment.modifiedNanoseconds = identity.st_mtim.tv_nsec;
    segment.changedSeconds = identity.st_ctim.tv_sec;
    segment.changedNanoseconds = identity.st_ctim.tv_nsec;
    segment.fileIdentityValid = true;
}

template<class Segment>
bool sameGeneration(const Segment& segment, const struct stat& identity) {
    return segment.fileIdentityValid &&
        segment.fileDevice == static_cast<uint64_t>(identity.st_dev) &&
        segment.fileInode == static_cast<uint64_t>(identity.st_ino) &&
        segment.fileSize == static_cast<uint64_t>(identity.st_size) &&
        segment.modifiedSeconds == identity.st_mtim.tv_sec &&
        segment.modifiedNanoseconds == identity.st_mtim.tv_nsec &&
        segment.changedSeconds == identity.st_ctim.tv_sec &&
        segment.changedNanoseconds == identity.st_ctim.tv_nsec;
}

// Callers provide a zero-filled private buffer. Missing files and real EOF
// retain the existing InProgress tail convention; IO errors never mean zero.
SegmentRead readSegmentFile(const std::string& path, std::vector<uint8_t>& data,
                            struct stat& identity) {
    int fd;
    do { fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC); }
    while (fd < 0 && errno == EINTR);
    if (fd < 0) return errno == ENOENT ? SegmentRead::Missing : SegmentRead::Failed;
    const SegmentReadDescriptor descriptor{fd};
    if (::fstat(fd, &identity) != 0) return SegmentRead::Failed;
    size_t done = 0;
    while (done < data.size()) {
        const auto count = ::read(fd, data.data() + done, data.size() - done);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) return SegmentRead::Failed;
        if (count == 0) break;
        done += static_cast<size_t>(count);
    }
    return SegmentRead::Loaded;
}

DbError segmentReadError(const std::string& path) {
    return DbError("58030", "could not read transaction status segment: " + path);
}
} // namespace

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
    // uint64_t needs at most 16 hex digits. Unlike an ostream insertion,
    // ordinary string construction cannot silently swallow bad_alloc and
    // return an incomplete pathname for an otherwise real segment.
    char digits[16];
    const auto converted = std::to_chars(digits, digits + sizeof(digits), segNo, 16);
    return dataDir_ + "/pg_xact/" + std::string(digits, converted.ptr);
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
    struct stat identity {};
    const auto result = readSegmentFile(path, seg.data, identity);
    if (result == SegmentRead::Failed) throw segmentReadError(path);
    if (result == SegmentRead::Loaded) {
        // Associate the cached bytes with the descriptor actually read, not
        // a possibly newer inode installed at the same pathname afterwards.
        rememberGeneration(seg, identity);
    }

    segments_[segNo] = std::move(seg);
}

void CommitLog::refreshSegmentIfChanged(uint64_t segNo) const {
    auto it = segments_.find(segNo);
    if (it == segments_.end() || it->second.dirty) return;

    const std::string path = segmentPath(segNo);
    struct stat status {};
    int metadataResult;
    do { metadataResult = ::stat(path.c_str(), &status); }
    while (metadataResult != 0 && errno == EINTR);
    if (metadataResult != 0) {
        if (errno != ENOENT) throw segmentReadError(path);
        if (it->second.fileIdentityValid) {
            it->second.data.assign(kSegmentFileSize, 0);
            it->second.fileIdentityValid = false;
        }
        return;
    }

    // One stat supplies identity, length, mtime and ctime. Checking ctime
    // also catches in-place edits whose mtime was restored by a peer.
    if (sameGeneration(it->second, status)) return;

    std::vector<uint8_t> data(kSegmentFileSize, 0);
    struct stat identity {};
    const auto result = readSegmentFile(path, data, identity);
    if (result == SegmentRead::Failed) throw segmentReadError(path);
    it->second.data = std::move(data);
    if (result == SegmentRead::Loaded) rememberGeneration(it->second, identity);
    else it->second.fileIdentityValid = false;
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
    struct stat identity {};
    if (readSegmentFile(path, latest, identity) == SegmentRead::Failed) {
        releaseLock();
        std::cerr << "[CLOG] Failed to read segment for merge: " << path << std::endl;
        return false;
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

    // Keep the complete, fixed-size two-bit image and atomic replacement,
    // but let zero-only blocks remain holes. A typical young segment has
    // only a few populated blocks; writing its zero tail on every commit
    // needlessly dirties another 256 KiB. Reads of holes still return the
    // exact InProgress bytes, including statuses explicitly cleared to zero.
    int sized;
    do { sized = ::ftruncate(fd, static_cast<off_t>(kSegmentFileSize)); }
    while (sized != 0 && errno == EINTR);
    bool ok = sized == 0;
    constexpr size_t blockSize = 4096;
    for (size_t offset = 0; ok && offset < latest.size();) {
        const size_t length = std::min(blockSize, latest.size() - offset);
        if (std::all_of(latest.begin() + offset, latest.begin() + offset + length,
                        [](uint8_t byte) { return byte == 0; })) {
            offset += length;
            continue;
        }
        // Mature segments usually have a dense prefix. Publish adjacent
        // populated blocks in one transfer instead of adding a syscall for
        // every 4 KiB block when there are no holes to skip.
        size_t end = offset + length;
        while (end < latest.size()) {
            const size_t next = std::min(blockSize, latest.size() - end);
            if (std::all_of(latest.begin() + end, latest.begin() + end + next,
                            [](uint8_t byte) { return byte == 0; })) break;
            end += next;
        }
        off_t position;
        do { position = ::lseek(fd, static_cast<off_t>(offset), SEEK_SET); }
        while (position < 0 && errno == EINTR);
        if (position != static_cast<off_t>(offset)) { ok = false; break; }
        size_t done = 0;
        while (done < end - offset) {
            const ssize_t written = ::write(fd, latest.data() + offset + done, end - offset - done);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) { ok = false; break; }
            done += static_cast<size_t>(written);
        }
        offset = end;
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
    it->second.data = std::move(latest);
    it->second.pendingBits.clear();
    struct stat status {};
    if (::stat(path.c_str(), &status) == 0) {
        rememberGeneration(it->second, status);
    } else {
        it->second.fileIdentityValid = false;
    }
    it->second.dirty = false;
    // Keep the publication lock until the cached bytes and their generation
    // agree. A peer may replace the inode immediately after LOCK_UN; taking
    // its metadata after unlocking would label our older bytes as that newer
    // image and suppress the next refresh.
    releaseLock();
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
    // Registration can allocate a node or rehash. Do not publish a cached
    // status until its pending update is guaranteed to remain flushable.
    auto& pending = seg.pendingBits[off];
    seg.data[off] = static_cast<uint8_t>((seg.data[off] & ~mask) | (statusBits(status) << shift));
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
            // Earlier entries may already be pending, but this entry must
            // not change the cache if its own registration cannot allocate.
            auto& pending = seg.pendingBits[off];
            seg.data[off] = static_cast<uint8_t>((seg.data[off] & ~mask) | (statusBits(status) << shift));
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
    std::set<uint64_t> toRemove;
    for (const auto& [segNo, _] : segments_) {
        if (segNo < maxSeg) {
            toRemove.insert(segNo);
        }
    }

    // A freshly started backend may not have read any old segment yet. Scan
    // pg_xact as well as the cache so truncation does not depend on earlier
    // visibility lookups in this process.
    const std::filesystem::path clogDir =
        std::filesystem::path(dataDir_) / "pg_xact";
    std::error_code scanError;
    std::filesystem::directory_iterator entry(clogDir, scanError);
    const std::filesystem::directory_iterator end;
    while (!scanError && entry != end) {
        std::error_code fileError;
        if (entry->is_regular_file(fileError) && !fileError) {
            const std::string filename = entry->path().filename().string();
            uint64_t segNo = 0;
            const auto parsed = std::from_chars(
                filename.data(), filename.data() + filename.size(),
                segNo, 16);
            if (parsed.ec == std::errc{} &&
                parsed.ptr == filename.data() + filename.size() &&
                segNo < maxSeg) {
                toRemove.insert(segNo);
            }
        }
        entry.increment(scanError);
    }
    for (uint64_t segNo : toRemove) {
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
            if (it != segments_.end()) {
                it->second.fileIdentityValid = false;
            }
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
