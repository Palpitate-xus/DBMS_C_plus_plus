#include "WAL.h"
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <system_error>
#include <vector>

namespace dbms {

namespace {

// Each WAL directory gets its own in-process mutex instead of one global for
// the whole process: different databases no longer serialize their WAL
// appends. The file lock below still protects separate processes; this map
// also prevents two in-process WALManager instances observing the same
// end-of-WAL and appending at the same offset.
//
// The registry is allocated on the heap and intentionally leaked: a
// StorageEngine destructor may construct a WALManager (shutdown WAL flush)
// during static destruction, long after function-local statics in other TUs
// have been destroyed. A plain namespace-scope map would already be freed by
// then; the leak keeps every mutex valid for the remainder of the process.
std::map<std::string, std::shared_ptr<std::mutex>>& walDirMutexRegistry() {
    static auto* registry = new std::map<std::string, std::shared_ptr<std::mutex>>();
    return *registry;
}

std::mutex& walDirMutexRegistryGuard() {
    static auto* guard = new std::mutex();
    return *guard;
}

std::shared_ptr<std::mutex> walDirMutex(const std::filesystem::path& walDir) {
    std::lock_guard<std::mutex> guard(walDirMutexRegistryGuard());
    auto& entry = walDirMutexRegistry()[walDir.string()];
    if (!entry) entry = std::make_shared<std::mutex>();
    return entry;
}

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

bool syncFileDescriptor(int fd) {
    return fd >= 0 && ::fsync(fd) == 0;
}

bool syncDirectory(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return false;
    const bool ok = syncFileDescriptor(fd);
    ::close(fd);
    return ok;
}

std::filesystem::path temporaryPath(const std::filesystem::path& target) {
    return target.string() + ".tmp." +
        std::to_string(static_cast<unsigned long long>(::getpid()));
}

bool writeEmptyFileAtomically(const std::filesystem::path& target) {
    const auto temp = temporaryPath(target);
    const int fd = ::open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return false;
    const bool synced = syncFileDescriptor(fd);
    ::close(fd);
    if (!synced) {
        std::error_code ec;
        std::filesystem::remove(temp, ec);
        return false;
    }
    std::error_code ec;
    std::filesystem::rename(temp, target, ec);
    if (ec) {
        std::filesystem::remove(temp, ec);
        return false;
    }
    return syncDirectory(target.parent_path());
}

bool sameRegularFileContents(const std::filesystem::path& first,
                             const std::filesystem::path& second) {
    const int firstFd = ::open(first.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (firstFd < 0) return false;
    const int secondFd = ::open(second.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (secondFd < 0) {
        ::close(firstFd);
        return false;
    }
    struct stat firstStat{};
    struct stat secondStat{};
    bool same = ::fstat(firstFd, &firstStat) == 0 &&
                ::fstat(secondFd, &secondStat) == 0 &&
                S_ISREG(firstStat.st_mode) && S_ISREG(secondStat.st_mode) &&
                firstStat.st_size == secondStat.st_size &&
                firstStat.st_size >= 0;
    std::array<char, 64 * 1024> firstBytes{};
    std::array<char, 64 * 1024> secondBytes{};
    const auto readExactly = [](int fd, char* bytes, size_t size, off_t offset) {
        size_t readTotal = 0;
        while (readTotal < size) {
            const ssize_t count = ::pread(fd, bytes + readTotal,
                                          size - readTotal,
                                          offset + static_cast<off_t>(readTotal));
            if (count < 0 && errno == EINTR) continue;
            if (count <= 0) return false;
            readTotal += static_cast<size_t>(count);
        }
        return true;
    };
    for (off_t offset = 0; same && offset < firstStat.st_size;) {
        const auto count = static_cast<size_t>(std::min<off_t>(
            static_cast<off_t>(firstBytes.size()), firstStat.st_size - offset));
        same = readExactly(firstFd, firstBytes.data(), count, offset) &&
               readExactly(secondFd, secondBytes.data(), count, offset) &&
               std::memcmp(firstBytes.data(), secondBytes.data(), count) == 0;
        offset += static_cast<off_t>(count);
    }
    if (same) same = syncFileDescriptor(secondFd);
    ::close(secondFd);
    ::close(firstFd);
    return same;
}

bool copyFileAtomically(const std::filesystem::path& source,
                        const std::filesystem::path& target) {
    const int sourceFd = ::open(source.c_str(), O_RDONLY | O_CLOEXEC);
    if (sourceFd < 0) return false;
    // A previous process can die between link(temp, target) and unlink(temp).
    // Never truncate a predictable stale temporary name: it may be another
    // hard link to an already-published archive segment.
    const std::string tempPattern = target.string() + ".tmp.XXXXXX";
    std::vector<char> tempName(tempPattern.begin(), tempPattern.end());
    tempName.push_back('\0');
    const int targetFd = ::mkstemp(tempName.data());
    if (targetFd < 0) {
        ::close(sourceFd);
        return false;
    }
    const std::filesystem::path temp(tempName.data());
    if (::fcntl(targetFd, F_SETFD, FD_CLOEXEC) != 0 ||
        ::fchmod(targetFd, 0644) != 0) {
        ::close(targetFd);
        ::close(sourceFd);
        std::error_code cleanupEc;
        std::filesystem::remove(temp, cleanupEc);
        return false;
    }

    bool ok = true;
    char buffer[1024 * 1024];
    while (ok) {
        const ssize_t readCount = ::read(sourceFd, buffer, sizeof(buffer));
        if (readCount == 0) break;
        if (readCount < 0) {
            ok = false;
            break;
        }
        ssize_t written = 0;
        while (written < readCount) {
            const ssize_t n = ::write(targetFd, buffer + written,
                                      static_cast<size_t>(readCount - written));
            if (n <= 0) {
                ok = false;
                break;
            }
            written += n;
        }
    }
    if (ok) ok = syncFileDescriptor(targetFd);
    ::close(targetFd);
    ::close(sourceFd);
    if (!ok) {
        std::error_code ec;
        std::filesystem::remove(temp, ec);
        return false;
    }

    // Publishing with rename would replace an existing WAL segment from a
    // different database generation. A hard link is atomic and refuses to
    // overwrite; an identical prior copy is safe to retry after a marker
    // publication failure.
    if (::link(temp.c_str(), target.c_str()) != 0 &&
        (errno != EEXIST || !sameRegularFileContents(temp, target))) {
        std::error_code cleanupEc;
        std::filesystem::remove(temp, cleanupEc);
        return false;
    }
    std::error_code ec;
    if (!std::filesystem::remove(temp, ec) || ec) return false;
    return syncDirectory(target.parent_path());
}

bool parseSegmentName(const std::string& name, uint32_t expectedTli,
                     uint32_t& segNo) {
    if (name.size() != 24) return false;
    try {
        const uint32_t tli = static_cast<uint32_t>(
            std::stoull(name.substr(0, 8), nullptr, 16));
        const uint32_t log = static_cast<uint32_t>(
            std::stoull(name.substr(8, 8), nullptr, 16));
        segNo = static_cast<uint32_t>(
            std::stoull(name.substr(16, 8), nullptr, 16));
        return tli == expectedTli && log == 0;
    } catch (...) {
        return false;
    }
}

} // namespace

WALManager::WALManager(const std::filesystem::path& walDir)
    : walDir_(walDir), currentLsn_(0),
      dirMutex_(walDirMutex(walDir)) {}

WALManager::~WALManager() {
    closeWriteFd();
    if (lockFd_ >= 0) {
        ::flock(lockFd_, LOCK_UN);
        ::close(lockFd_);
        lockFd_ = -1;
    }
    if (directoryFd_ >= 0) ::close(directoryFd_);
}

bool WALManager::refersToCurrentDirectory() const {
    struct stat opened {}, current {};
    return directoryFd_ >= 0 && ::fstat(directoryFd_, &opened) == 0 &&
           ::stat(walDir_.c_str(), &current) == 0 &&
           S_ISDIR(current.st_mode) && opened.st_dev == current.st_dev &&
           opened.st_ino == current.st_ino;
}

bool WALManager::ensureOpen(bool createDirectory) {
    if (directoryFd_ >= 0 && !refersToCurrentDirectory()) return false;
    if (open_) return true;
    std::error_code ec;
    if (directoryFd_ < 0) {
        if (createDirectory) {
            std::filesystem::create_directories(walDir_, ec);
            if (ec) return false;
        }
        directoryFd_ = ::open(walDir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directoryFd_ < 0) return false;
    }
    if (!refersToCurrentDirectory()) return false;
    // Never recreate a missing parent through a retained stale manager.
    std::filesystem::create_directory(archiveStatusDir(), ec);
    if (ec) return false;

    // Load or infer timeline ID.
    if (!loadTimeline()) return false;
    if (!std::filesystem::exists(timelinePath())) {
        if (!persistTimeline()) return false;
    }

    if (!refreshCurrentLsnFromDisk()) return false;
    open_ = true;
    if (!validateRecordsOnDisk()) {
        open_ = false;
        return false;
    }
    // Seed the incremental tail state (chain walk happens once here).
    if (!scanWalTail()) {
        open_ = false;
        return false;
    }
    // Everything on disk is already durable by definition.
    lastSyncedLsn_ = currentLsn_.load(std::memory_order_relaxed);
    earliestSegValid_ = false;
    return true;
}

bool WALManager::refreshCurrentLsnFromDisk() {
    // Scan existing segments to find current LSN (end of last segment).
    currentLsn_.store(0, std::memory_order_relaxed);
    uint32_t maxSeg = 0;
    bool found = false;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(walDir_, ec)) {
        if (ec) return false;
        std::string name = entry.path().filename().string();
        uint32_t segNo = 0;
        if (!parseSegmentName(name, timelineId_, segNo)) continue;
        if (segNo >= maxSeg) {
            maxSeg = segNo;
            found = true;
        }
    }
    if (found) {
        auto path = segmentPath(maxSeg);
        auto size = std::filesystem::file_size(path, ec);
        if (ec) return false;
        if (size > kSegmentSize) return false;
        currentLsn_.store(static_cast<Lsn>(maxSeg) * kSegmentSize + size, std::memory_order_relaxed);
    }
    return true;
}

// Walk the retained record chain once to determine the last record's LSN and
// length. This is the only place the chain is walked from the tail side; the
// hot XLogInsert path keeps the result incrementally in lastRecordLsn_.
bool WALManager::scanWalTail() {
    lastRecordLsn_ = 0;
    hasLastRecord_ = false;
    lastRecordTotLen_ = 0;
    Lsn lsn = earliestAvailableLsn();
    std::optional<Lsn> previous;
    while (lsn < currentLsn_) {
        auto rec = ReadRecord(lsn);
        if (!rec) {
            // Zero padding from switchWal() legitimately terminates the
            // record chain before the end of a padded segment: the tail
            // state is the last real record, and currentLsn_ (the append
            // position) stands.
            XLogRecHeader header;
            if (readBytes(lsn, reinterpret_cast<char*>(&header), sizeof(header)) &&
                header.xl_prev == 0 && header.xl_tot_len == 0 &&
                header.xl_info == 0 && header.xl_xid == 0) {
                break;
            }
            return false;
        }
        previous = lsn;
        lastRecordLsn_ = lsn;
        hasLastRecord_ = true;
        lastRecordTotLen_ = rec->header.xl_tot_len;
        const Lsn next = lsn + rec->header.xl_tot_len;
        if (next <= lsn || next > currentLsn_) return false;
        lsn = next;
    }
    // A padding break means the walk stopped before currentLsn_ by design.
    return true;
}

bool WALManager::validateRecordsOnDisk() const {
    Lsn lsn = earliestAvailableLsn();
    std::optional<Lsn> previous;
    while (lsn < currentLsn_) {
        auto rec = ReadRecord(lsn);
        if (!rec) {
            // Zero padding from switchWal() ends the record chain before
            // the end of a padded segment.
            XLogRecHeader header;
            if (readBytes(lsn, reinterpret_cast<char*>(&header), sizeof(header)) &&
                header.xl_prev == 0 && header.xl_tot_len == 0 &&
                header.xl_info == 0 && header.xl_xid == 0) {
                break;
            }
            return false;
        }
        if (previous) {
            if (rec->header.xl_prev != *previous) return false;
        } else if ((lsn == 0 && rec->header.xl_prev != 0) ||
                   (lsn != 0 && rec->header.xl_prev >= lsn)) {
            // The first retained record may point to a segment already
            // truncated from this WAL directory, but never forward into the
            // retained stream.
            return false;
        }
        previous = lsn;
        const Lsn next = lsn + rec->header.xl_tot_len;
        if (next <= lsn || next > currentLsn_) return false;
        lsn = next;
    }
    // Padding break: the walk may stop before currentLsn_ by design.
    return true;
}

std::optional<Lsn> WALManager::lastRecordLsn() const {
    if (currentLsn_ == 0) return std::nullopt;
    if (!validateRecordsOnDisk()) return std::nullopt;
    Lsn lsn = earliestAvailableLsn();
    std::optional<Lsn> previous;
    while (lsn < currentLsn_) {
        auto rec = ReadRecord(lsn);
        if (!rec) return std::nullopt;
        previous = lsn;
        const Lsn next = lsn + rec->header.xl_tot_len;
        if (next <= lsn || next > currentLsn_) return std::nullopt;
        lsn = next;
    }
    return lsn == currentLsn_ ? previous : std::nullopt;
}

int WALManager::acquireWalFileLock() const {
    if (!refersToCurrentDirectory()) return -1;
    // Reuse the long-lived lock descriptor across operations. Only the flock
    // itself is taken/dropped per operation, preserving cross-process
    // semantics while removing the per-record open/close syscalls.
    if (lockFd_ < 0) {
        int fd = ::openat(directoryFd_, "wal.lock", O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (fd < 0) return -1;
        lockFd_ = fd;
    }
    if (::flock(lockFd_, LOCK_EX) != 0) return -1;
    if (!refersToCurrentDirectory()) {
        ::flock(lockFd_, LOCK_UN);
        return -1;
    }
    return lockFd_;
}

void WALManager::releaseWalFileLock(int fd) {
    if (fd < 0) return;
    ::flock(fd, LOCK_UN);
}

// Metadata check that the on-disk tail and its adjacent segment match
// currentLsn_. Must run under the WAL file lock. Returns false when another
// process appended (or the stream is damaged) and the caller must rebuild
// state from disk.
bool WALManager::validateAppendOffset() {
    // A cached empty stream has no retained segment against which to prove
    // freshness. Rebuild it under the WAL lock, including retained prefixes.
    if (currentLsn_ == 0) return false;
    const uint32_t seg = segmentNumber(currentLsn_ - 1);
    std::error_code ec;
    const auto path = segmentPath(seg);
    if (!std::filesystem::exists(path, ec) || ec) return false;
    const auto size = std::filesystem::file_size(path, ec);
    if (ec || size > kSegmentSize) return false;
    const Lsn onDisk = static_cast<Lsn>(seg) * kSegmentSize + size;
    if (onDisk != currentLsn_) return false;
    if (seg == std::numeric_limits<uint32_t>::max()) return true;

    // At a boundary, a peer can append into the next segment without changing
    // the full previous file at all. A later segment beside a partial tail
    // also invalidates this cache, even when a failed cleanup left it empty.
    const auto next = segmentPath(seg + 1);
    const bool exists = std::filesystem::exists(next, ec);
    if (ec) return false;
    if (!exists) return true;
    const auto nextSize = std::filesystem::file_size(next, ec);
    return !ec && segmentOffset(currentLsn_) == 0 && nextSize == 0;
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
    const auto temp = temporaryPath(path);
    const int fd = ::open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) return false;
    const uint32_t tli = timelineId_;
    const ssize_t written = ::write(fd, &tli, sizeof(tli));
    const bool ok = written == static_cast<ssize_t>(sizeof(tli)) && syncFileDescriptor(fd);
    ::close(fd);
    if (!ok) {
        std::error_code ec;
        std::filesystem::remove(temp, ec);
        return false;
    }
    std::error_code ec;
    std::filesystem::rename(temp, path, ec);
    if (ec) {
        std::filesystem::remove(temp, ec);
        return false;
    }
    return syncDirectory(path.parent_path());
}

bool WALManager::setTimeline(uint32_t tli) {
    if (tli == 0) return false;
    if (!ensureOpen()) return false;
    if (tli == timelineId_) return true;

    // Timeline changes are metadata commits.  Serialize the persisted
    // timeline switch with appenders before changing any in-memory path
    // state; otherwise another manager can append to the old timeline while
    // this instance already reports the new one.
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return false;

    const uint32_t previousTli = timelineId_;
    timelineId_ = tli;
    // Segment filenames embed the timeline; cached fd/path state must be
    // rebuilt and the tail state re-derived after the switch.
    closeWriteFd();
    earliestSegValid_ = false;

    if (persistTimeline() && refreshCurrentLsnFromDisk() && scanWalTail()) {
        lastSyncedLsn_ = currentLsn_;
        releaseWalFileLock(walLock);
        return true;
    }

    // A failed switch must not leave the durable timeline file pointing at a
    // timeline that this manager rejected.  Best-effort restoration keeps a
    // retry on the old timeline safe; if restoration itself fails, mark this
    // manager unusable so no caller can append through inconsistent state.
    timelineId_ = previousTli;
    earliestSegValid_ = false;
    const bool restored = persistTimeline() &&
                          refreshCurrentLsnFromDisk() &&
                          scanWalTail();
    if (restored) {
        lastSyncedLsn_ = currentLsn_;
    } else {
        open_ = false;
    }
    releaseWalFileLock(walLock);
    return false;
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
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return false;
    const bool ok = markSegmentReadyForArchiveLocked(segNo);
    releaseWalFileLock(walLock);
    return ok;
}

bool WALManager::markSegmentReadyForArchiveLocked(uint32_t segNo) {
    std::error_code ec;
    std::filesystem::create_directory(archiveStatusDir(), ec);
    if (ec) return false;
    return writeEmptyFileAtomically(readyPath(segNo));
}

bool WALManager::markSegmentArchived(uint32_t segNo) {
    if (!ensureOpen()) return false;
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return false;
    const bool ok = markSegmentArchivedLocked(segNo);
    releaseWalFileLock(walLock);
    return ok;
}

bool WALManager::markSegmentArchivedLocked(uint32_t segNo) {
    std::error_code ec;
    if (!writeEmptyFileAtomically(donePath(segNo))) return false;
    std::filesystem::remove(readyPath(segNo), ec);
    if (ec) return false;
    if (syncDirectory(archiveStatusDir())) return true;

    // Keep the operation retryable if the final marker-directory sync fails.
    // A later archiver can verify/copy the segment again and complete cleanup.
    (void)writeEmptyFileAtomically(readyPath(segNo));
    return false;
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
        uint32_t segNo = 0;
        if (!parseSegmentName(name.substr(0, 24), timelineId_, segNo)) continue;
        // A ready marker remains retryable even if a previous process managed
        // to publish .done but failed while durably removing .ready.
        result.push_back(segNo);
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

bool WALManager::archiveSegment(uint32_t segNo, const std::filesystem::path& archiveDir) {
    if (!ensureOpen()) return false;
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return false;
    const bool ok = archiveSegmentLocked(segNo, archiveDir);
    releaseWalFileLock(walLock);
    return ok;
}

bool WALManager::archiveSegmentLocked(uint32_t segNo,
                                      const std::filesystem::path& archiveDir) {
    auto src = segmentPath(segNo);
    std::error_code sourceEc;
    if (!std::filesystem::is_regular_file(src, sourceEc) || sourceEc) return false;
    std::error_code ec;
    std::filesystem::create_directories(archiveDir, ec);
    if (ec) return false;
    auto dst = archiveDir / src.filename();
    if (!syncDirectory(archiveDir)) return false;
    if (!syncSegment(segNo)) return false;
    if (!copyFileAtomically(src, dst)) return false;
    return markSegmentArchivedLocked(segNo);
}

bool WALManager::archivePendingSegments(const std::filesystem::path& archiveDir) {
    if (!ensureOpen()) return false;
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return false;
    auto pending = pendingArchiveSegments();
    bool ok = true;
    for (uint32_t segNo : pending) {
        if (!archiveSegmentLocked(segNo, archiveDir)) ok = false;
    }
    releaseWalFileLock(walLock);
    return ok;
}

Lsn WALManager::switchWal() {
    if (!ensureOpen()) return INVALID_LSN;
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return INVALID_LSN;

    const uint64_t cur = currentLsn_.load(std::memory_order_relaxed);
    const uint32_t curSeg = segmentNumber(cur);
    const uint64_t off = segmentOffset(cur);
    if (off > 0) {
        // Zero-fill the rest of the current segment so its file exists in
        // full and a later reader sees end-of-records at the padding.
        int fd = segmentWriteFd(curSeg);
        if (fd < 0) {
            releaseWalFileLock(walLock);
            return INVALID_LSN;
        }
        const uint64_t padLen = kSegmentSize - off;
        std::vector<char> zeros(4096, 0);
        uint64_t remaining = padLen;
        if (::lseek(fd, static_cast<off_t>(off), SEEK_SET) < 0) {
            releaseWalFileLock(walLock);
            return INVALID_LSN;
        }
        while (remaining > 0) {
            const uint64_t chunk = std::min<uint64_t>(remaining, zeros.size());
            const ssize_t wrote = ::write(fd, zeros.data(), static_cast<size_t>(chunk));
            if (wrote < 0 || static_cast<uint64_t>(wrote) != chunk) {
                releaseWalFileLock(walLock);
                return INVALID_LSN;
            }
            remaining -= chunk;
        }
        if (::fsync(fd) < 0) {
            releaseWalFileLock(walLock);
            return INVALID_LSN;
        }
    }

    const Lsn next = static_cast<Lsn>(curSeg + 1) * kSegmentSize;
    currentLsn_.store(next, std::memory_order_relaxed);
    closeWriteFd();
    releaseWalFileLock(walLock);
    return next;
}

bool WALManager::markSegmentsReadyBefore(Lsn lsn) {
    if (!ensureOpen() || lsn == INVALID_LSN) return false;
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return false;
    uint32_t endSeg = segmentNumber(lsn);
    bool ok = true;
    for (uint32_t seg = 0; seg < endSeg; ++seg) {
        std::error_code ec;
        if (!std::filesystem::exists(segmentPath(seg), ec)) {
            if (ec) ok = false;
            continue;
        }
        if (!markSegmentReadyForArchiveLocked(seg)) ok = false;
    }
    releaseWalFileLock(walLock);
    return ok;
}

Lsn WALManager::earliestAvailableLsn() const {
    // Cached answer: the first retained segment only changes through
    // truncateBefore()/reset(), both of which invalidate the cache. The
    // directory walk used to run on every XLogFlush.
    if (earliestSegValid_) {
        return static_cast<Lsn>(earliestSeg_) * kSegmentSize;
    }
    if (!std::filesystem::is_directory(walDir_)) return 0;

    std::error_code ec;
    uint32_t firstSeg = std::numeric_limits<uint32_t>::max();
    for (const auto& entry : std::filesystem::directory_iterator(
             walDir_, std::filesystem::directory_options::skip_permission_denied, ec)) {
        if (ec) return 0;
        std::error_code entryEc;
        if (!entry.is_regular_file(entryEc) || entryEc) continue;
        const std::string name = entry.path().filename().string();
        uint32_t seg = 0;
        if (!parseSegmentName(name, timelineId_, seg)) continue;
        firstSeg = std::min(firstSeg, seg);
    }
    if (firstSeg == std::numeric_limits<uint32_t>::max()) {
        earliestSeg_ = 0;
        earliestSegValid_ = true;
        return 0;
    }
    earliestSeg_ = firstSeg;
    earliestSegValid_ = true;
    return static_cast<Lsn>(firstSeg) * kSegmentSize;
}

bool WALManager::truncateBefore(Lsn lsn) {
    if (!ensureOpen() || lsn == INVALID_LSN || lsn > currentLsn_) return false;

    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return false;

    const uint32_t keepSeg = segmentNumber(lsn);
    const uint32_t currentSeg = currentLsn_ == 0
        ? 0
        : segmentNumber(currentLsn_ - 1);
    std::vector<uint32_t> candidates;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(
             walDir_, std::filesystem::directory_options::skip_permission_denied, ec)) {
        if (ec) {
            releaseWalFileLock(walLock);
            return false;
        }
        std::error_code entryEc;
        if (!entry.is_regular_file(entryEc) || entryEc) continue;
        const std::string name = entry.path().filename().string();
        uint32_t seg = 0;
        if (!parseSegmentName(name, timelineId_, seg)) continue;
        if (seg < keepSeg && seg < currentSeg) candidates.push_back(seg);
    }
    std::sort(candidates.begin(), candidates.end());

    // Preflight the complete removal set before unlinking anything. A
    // partially archived range must not produce a partially truncated WAL.
    for (uint32_t segNo : candidates) {
        std::error_code statusEc;
        if (!std::filesystem::exists(donePath(segNo), statusEc) || statusEc) {
            releaseWalFileLock(walLock);
            return false;
        }
    }

    bool ok = true;
    bool changed = false;
    for (uint32_t segNo : candidates) {
        std::error_code removeEc;
        if (!std::filesystem::remove(segmentPath(segNo), removeEc) && removeEc) {
            ok = false;
            continue;
        }
        changed = true;

        std::error_code markerEc;
        std::filesystem::remove(readyPath(segNo), markerEc);
        if (markerEc) ok = false;
        markerEc.clear();
        std::filesystem::remove(donePath(segNo), markerEc);
        if (markerEc) ok = false;
    }

    if (changed) {
        const int dirFd = ::open(walDir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dirFd < 0 || ::fsync(dirFd) != 0) ok = false;
        if (dirFd >= 0) ::close(dirFd);
        // Removal changes the retained prefix and may have unlinked the
        // segment the cached write fd points at.
        earliestSegValid_ = false;
        closeWriteFd();
    }
    releaseWalFileLock(walLock);
    return ok;
}

void WALManager::advanceCurrentLsn(uint32_t len) {
    currentLsn_.store(currentLsn_.load(std::memory_order_relaxed) + len,
                      std::memory_order_relaxed);
}

uint32_t WALManager::computeCrc(const char* data, size_t len) const {
    return crc32c(data, len);
}

bool WALManager::verifyCrc(const XLogRecord& rec) const {
    uint32_t totLen = rec.header.xl_tot_len;
    if (totLen < sizeof(XLogRecHeader) ||
        totLen != sizeof(XLogRecHeader) + rec.data.size()) return false;
    std::string buf;
    buf.reserve(totLen);
    XLogRecHeader headerCopy = rec.header;
    headerCopy.xl_crc = 0;
    buf.append(reinterpret_cast<const char*>(&headerCopy), sizeof(XLogRecHeader));
    if (!rec.data.empty()) buf.append(rec.data.data(), rec.data.size());
    uint32_t expected = computeCrc(buf.data(), buf.size());
    return expected == rec.header.xl_crc;
}

bool WALManager::ensureSegment(uint32_t segNo) {
    auto path = segmentPath(segNo);
    if (std::filesystem::exists(path)) return true;
    std::ofstream ofs(path, std::ios::binary);
    return ofs.good();
}

// Ensure writeFd_ targets the given segment, opening/switching as needed.
// Returns the raw fd or -1. The fd stays open across appends; it is reopened
// only when the stream rolls into a new segment (or after truncation).
int WALManager::segmentWriteFd(uint32_t segNo) {
    if (writeFdValid_ && writeFdSeg_ == segNo && writeFd_ >= 0) return writeFd_;
    if (!closeWriteFd()) return -1;
    if (!ensureSegment(segNo)) return -1;
    auto path = segmentPath(segNo);
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd < 0) return -1;
    writeFd_ = fd;
    writeFdSeg_ = segNo;
    writeFdValid_ = true;
    return fd;
}

bool WALManager::closeWriteFd() {
    if (writeFd_ >= 0) {
        ::close(writeFd_);
        writeFd_ = -1;
    }
    writeFdValid_ = false;
    return true;
}

bool WALManager::appendBytes(const char* buf, size_t len) {
    if (!ensureOpen()) return false;
    if (len == 0) return true;
    const Lsn start = currentLsn_;
    if (len > std::numeric_limits<Lsn>::max() - start) return false;
    const uint64_t first = start / kSegmentSize;
    const uint64_t last = (start + len - 1) / kSegmentSize;
    constexpr size_t capacity = kMaxRecordPayload / kSegmentSize + 2;
    if (last > std::numeric_limits<uint32_t>::max() ||
        last - first + 1 > capacity) return false;

    struct AppendSegment {
        std::filesystem::path path;
        int fd = -1;
        bool existed = false;
        bool modified = false;
        off_t size = 0;
        dev_t device = 0;
        ino_t inode = 0;
        ~AppendSegment() { if (fd >= 0) ::close(fd); }
    };
    std::array<AppendSegment, capacity> segments;
    const size_t count = static_cast<size_t>(last - first + 1);
    // Prepare all allocating paths and establish the rollback boundary before
    // any byte is changed. Future segments may be absent or empty, never data
    // belonging to a writer whose tail this manager has not refreshed.
    for (size_t i = 0; i < count; ++i) {
        auto& segment = segments[i];
        segment.path = segmentPath(static_cast<uint32_t>(first + i));
        segment.size = i == 0 ? static_cast<off_t>(start % kSegmentSize) : 0;
        struct stat status{};
        if (::stat(segment.path.c_str(), &status) == 0) {
            if (!S_ISREG(status.st_mode) || status.st_size != segment.size)
                return false;
            segment.existed = true;
            segment.device = status.st_dev;
            segment.inode = status.st_ino;
        } else if (errno != ENOENT) {
            return false;
        } else if (segment.size != 0) {
            return false;
        }
    }
    const auto sync = [](int fd) {
        int result;
        do { result = ::fsync(fd); } while (result != 0 && errno == EINTR);
        return result == 0;
    };
    const auto rollback = [&]() {
        bool ok = true;
        bool removed = false;
        for (size_t i = 0; i < count; ++i) {
            auto& segment = segments[i];
            if (segment.fd < 0) continue;
            if (segment.existed && segment.modified) {
                int result;
                do { result = ::ftruncate(segment.fd, segment.size); }
                while (result != 0 && errno == EINTR);
                if (result != 0 || !sync(segment.fd)) ok = false;
            }
        }
        // Remove new names from the end. If unlink itself fails, keep the
        // preceding new names as well: no hole may hide the damaged suffix
        // from another manager's adjacent-segment refresh check.
        for (size_t i = count; i-- > 0;) {
            auto& segment = segments[i];
            if (segment.fd < 0 || segment.existed) continue;
            int result;
            do { result = ::unlink(segment.path.c_str()); }
            while (result != 0 && errno == EINTR);
            if (result == 0) removed = true;
            else if (errno != ENOENT) { ok = false; break; }
        }
        if (removed && !sync(directoryFd_)) ok = false;
        // A failed cleanup cannot be trusted by the hot append fast path.
        // Reopening must validate the actual on-disk stream before reuse.
        if (!ok) open_ = false;
        return false;
    };

    size_t written = 0;
    for (size_t i = 0; i < count; ++i) {
        auto& segment = segments[i];
        const uint32_t number = static_cast<uint32_t>(first + i);
        struct stat cached{};
        if (writeFdValid_ && writeFd_ >= 0 && writeFdSeg_ == number &&
            segment.existed && ::fstat(writeFd_, &cached) == 0 &&
            cached.st_dev == segment.device && cached.st_ino == segment.inode &&
            cached.st_size == segment.size) {
            segment.fd = writeFd_;
            writeFd_ = -1;
            writeFdValid_ = false;
        } else {
            closeWriteFd();
            const int flags = O_WRONLY | (segment.existed ? 0 : O_CREAT | O_EXCL);
            do { segment.fd = ::open(segment.path.c_str(), flags, 0644); }
            while (segment.fd < 0 && errno == EINTR);
            if (segment.fd < 0) return rollback();
            struct stat opened{};
            if (::fstat(segment.fd, &opened) != 0 ||
                !S_ISREG(opened.st_mode) || opened.st_size != segment.size ||
                (segment.existed && (opened.st_dev != segment.device ||
                                     opened.st_ino != segment.inode))) {
                return rollback();
            }
        }
        if (::lseek(segment.fd, segment.size, SEEK_SET) < 0) return rollback();
        const size_t chunk = static_cast<size_t>(std::min<uint64_t>(
            len - written, kSegmentSize - static_cast<uint64_t>(segment.size)));
        size_t complete = 0;
        while (complete < chunk) {
            const ssize_t n = ::write(segment.fd, buf + written + complete,
                                      chunk - complete);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return rollback();
            segment.modified = true;
            complete += static_cast<size_t>(n);
        }
        written += chunk;
    }
    // Retain only the last descriptor as the normal cached append fd. The
    // local owners close earlier segments on success and all on failure.
    writeFd_ = segments[count - 1].fd;
    segments[count - 1].fd = -1;
    writeFdSeg_ = static_cast<uint32_t>(last);
    writeFdValid_ = true;
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
        int fd;
        do {
            fd = ::open(path.c_str(), O_RDONLY);
        } while (fd < 0 && errno == EINTR);
        if (fd < 0) return false;
        if (::lseek(fd, off, SEEK_SET) < 0) {
            ::close(fd);
            return false;
        }
        uint64_t chunk = std::min(remaining, kSegmentSize - off);
        size_t received = 0;
        while (received < chunk) {
            const ssize_t n = ::read(fd, ptr + received,
                                     static_cast<size_t>(chunk) - received);
            if (n < 0 && errno == EINTR) continue;
            // WAL needs the entire requested range: only a positive
            // transfer advances it; real EOF and hard errors still fail.
            if (n <= 0) {
                ::close(fd);
                return false;
            }
            received += static_cast<size_t>(n);
        }
        ::close(fd);
        ptr += chunk;
        offset += chunk;
        remaining -= chunk;
    }
    return true;
}

bool WALManager::syncSegment(uint32_t segNo) {
    // Reuse the long-lived append descriptor when it targets this segment;
    // otherwise open briefly read-only (fsync does not require write access).
    if (writeFdValid_ && writeFdSeg_ == segNo && writeFd_ >= 0) {
        return ::fsync(writeFd_) == 0;
    }
    auto path = segmentPath(segNo);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    int r = ::fsync(fd);
    ::close(fd);
    return r == 0;
}

Lsn WALManager::XLogInsert(uint8_t rmid, uint8_t info, uint64_t xid,
                           const std::vector<char>& data) {
    if (!ensureOpen()) return INVALID_LSN;

    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return INVALID_LSN;
    // Tail/adjacent-segment guard: if another process appended behind our back,
    // rebuild the tail state from disk instead of silently overwriting.
    if (!validateAppendOffset()) {
        if (!refreshCurrentLsnFromDisk() || !scanWalTail()) {
            releaseWalFileLock(walLock);
            return INVALID_LSN;
        }
        // The stream moved; the cached write fd may point at a removed or
        // rewritten segment. Force a reopen on next append.
        closeWriteFd();
    }

    if (data.size() > kMaxRecordPayload ||
        data.size() > std::numeric_limits<uint32_t>::max() - sizeof(XLogRecHeader)) {
        releaseWalFileLock(walLock);
        return INVALID_LSN;
    }
    uint32_t payloadLen = static_cast<uint32_t>(data.size());
    const uint64_t alignedLen = alignLen(sizeof(XLogRecHeader) + payloadLen, MAXALIGN);
    if (alignedLen > std::numeric_limits<uint32_t>::max()) {
        releaseWalFileLock(walLock);
        return INVALID_LSN;
    }
    uint32_t totalLen = static_cast<uint32_t>(alignedLen);

    // The tail record is maintained incrementally; no chain re-walk here.
    // LSN 0 is a valid record position, so presence is tracked separately.
    if (currentLsn_ != 0 && !hasLastRecord_) {
        // e.g. after switchWal() in another process left the tail cache
        // empty; rebuild it from the record chain instead of failing.
        if (!scanWalTail()) {
            releaseWalFileLock(walLock);
            return INVALID_LSN;
        }
    }
    const Lsn previousLsn = lastRecordLsn_;

    XLogRecHeader header;
    header.xl_prev = previousLsn;
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
    if (!appendBytes(record.data(), record.size())) {
        releaseWalFileLock(walLock);
        return INVALID_LSN;
    }
    // Advance the incremental tail state together with the LSN.
    lastRecordLsn_ = recLsn;
    hasLastRecord_ = true;
    lastRecordTotLen_ = totalLen;
    advanceCurrentLsn(totalLen);
    uint32_t newSeg = segmentNumber(currentLsn_);
    if (newSeg != prevSeg) {
        // The previous segment is now complete; mark it ready for archiving.
        markSegmentReadyForArchiveLocked(prevSeg);
    }
    releaseWalFileLock(walLock);
    return recLsn;
}

bool WALManager::XLogFlush(Lsn targetLsn) {
    if (!ensureOpen() || targetLsn == INVALID_LSN) return false;
    // ---- Group commit gate ----------------------------------------------
    // Serialize flushes on a dedicated mutex BEFORE dirMutex_.  While one
    // thread fsyncs inside dirMutex_, queued committers block here rather
    // than on dirMutex_, so concurrent XLogInserts keep appending; when
    // the in-flight fsync lands, its synced prefix almost always already
    // covers the waiters' targets and they return from the durable-prefix
    // check without any fsync of their own.  A slow jbd2 commit costs a
    // waiter one queue delay instead of serializing N full fsyncs under
    // the insert lock (the observed world-stop pattern: every connection
    // stalls ~30s while commits queue behind each other's fsync).
    std::lock_guard<std::mutex> flushQueue(flushMutex_);
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return false;
    if (targetLsn > currentLsn_) {
        releaseWalFileLock(walLock);
        return false;
    }
    // Already-durable prefix: nothing to do. This makes the common repeated
    // flush of the same LSN a pure memory check.
    if (targetLsn != 0 && targetLsn <= lastSyncedLsn_) {
        releaseWalFileLock(walLock);
        return true;
    }
    Lsn endLsn = targetLsn;
    if (targetLsn < currentLsn_) {
        if (hasLastRecord_ && targetLsn == lastRecordLsn_) {
            endLsn = targetLsn + lastRecordTotLen_;
        } else {
            const auto record = ReadRecord(targetLsn);
            if (!record) {
                releaseWalFileLock(walLock);
                return false;
            }
            endLsn = targetLsn + record->header.xl_tot_len;
        }
        if (endLsn <= targetLsn || endLsn > currentLsn_) {
            releaseWalFileLock(walLock);
            return false;
        }
    }
    if (endLsn == 0) {
        releaseWalFileLock(walLock);
        return true;
    }
    uint32_t startSeg = (lastSyncedLsn_ == 0)
        ? segmentNumber(earliestAvailableLsn())
        : segmentNumber(lastSyncedLsn_);
    uint32_t endSeg = segmentNumber(endLsn - 1);
    for (uint32_t seg = startSeg; seg <= endSeg; ++seg) {
        if (!syncSegment(seg)) {
            releaseWalFileLock(walLock);
            return false;
        }
    }
    if (endLsn > lastSyncedLsn_) lastSyncedLsn_ = endLsn;
    releaseWalFileLock(walLock);
    return true;
}

std::optional<XLogRecord> WALManager::ReadRecord(Lsn lsn) const {
    if (!open_ || lsn >= currentLsn_ || (lsn % MAXALIGN) != 0) return std::nullopt;
    XLogRecHeader header;
    if (!readBytes(lsn, reinterpret_cast<char*>(&header), sizeof(header))) {
        return std::nullopt;
    }
    if (header.xl_tot_len < sizeof(header) ||
        header.xl_tot_len > sizeof(header) + kMaxRecordPayload + MAXALIGN - 1 ||
        (header.xl_tot_len % MAXALIGN) != 0 ||
        static_cast<uint64_t>(header.xl_tot_len) > currentLsn_ - lsn) {
        return std::nullopt;
    }
    XLogRecord rec;
    rec.header = header;
    rec.data.resize(header.xl_tot_len - sizeof(header));
    if (!rec.data.empty() &&
        !readBytes(lsn + sizeof(header), rec.data.data(), rec.data.size())) {
        return std::nullopt;
    }
    if (!verifyCrc(rec)) return std::nullopt;
    return rec;
}

std::optional<XLogRecord> WALManager::ReadNextRecord(Lsn lsn) const {
    if (lsn == 0) {
        return ReadRecord(earliestAvailableLsn());
    }
    auto rec = ReadRecord(lsn);
    if (!rec) return std::nullopt;
    const Lsn next = lsn + rec->header.xl_tot_len;
    if (next < lsn) return std::nullopt;
    return ReadRecord(next);
}

std::optional<Lsn> WALManager::findLastCheckpointLsn() const {
    Lsn lsn = earliestAvailableLsn();
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
    std::lock_guard<std::mutex> processLock(*dirMutex_);
    const int walLock = acquireWalFileLock();
    if (walLock < 0) return;
    std::error_code ec;
    // Close the append descriptor before unlinking the segments it targets.
    closeWriteFd();
    for (const auto& entry : std::filesystem::directory_iterator(walDir_)) {
        std::filesystem::remove_all(entry.path(), ec);
    }
    currentLsn_ = 0;
    timelineId_ = 1;
    lastRecordLsn_ = 0;
    hasLastRecord_ = false;
    lastRecordTotLen_ = 0;
    lastSyncedLsn_ = 0;
    earliestSeg_ = 0;
    earliestSegValid_ = false;
    releaseWalFileLock(walLock);
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
    if (p > end || static_cast<size_t>(end - p) < sizeof(uint32_t)) return false;
    std::memcpy(&out, p, sizeof(out));
    p += sizeof(out);
    return true;
}
bool readU16(const char*& p, const char* end, uint16_t& out) {
    if (p > end || static_cast<size_t>(end - p) < sizeof(uint16_t)) return false;
    std::memcpy(&out, p, sizeof(out));
    p += sizeof(out);
    return true;
}
bool readU64(const char*& p, const char* end, uint64_t& out) {
    if (p > end || static_cast<size_t>(end - p) < sizeof(uint64_t)) return false;
    std::memcpy(&out, p, sizeof(out));
    p += sizeof(out);
    return true;
}
bool readString(const char*& p, const char* end, std::string& out) {
    uint32_t len = 0;
    if (!readU32(p, end, len)) return false;
    if (p > end || static_cast<size_t>(end - p) < len) return false;
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
            if (p > end || static_cast<size_t>(end - p) < pageLen) return false;
            return redoPageImage(tableName, blockNum, forkNum, p, pageLen);
        } else if (info == XLOG_HEAP_INSERT) {
            uint32_t rowLen = 0;
            if (!readU32(p, end, rowLen)) return false;
            if (p > end || static_cast<size_t>(end - p) < rowLen) return false;
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
            if (p > end || static_cast<size_t>(end - p) < rowLen) return false;
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
