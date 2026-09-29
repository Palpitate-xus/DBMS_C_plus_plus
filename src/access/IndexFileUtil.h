#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace dbms::index_file {

// Deterministic one-shot injection used by durability regression tests.  The
// state is process-local and remains dormant unless a test explicitly arms it.
inline std::atomic<unsigned> directorySyncFailureCountForTesting{0};

inline void failNextDirectorySyncForTesting() {
    directorySyncFailureCountForTesting.store(1, std::memory_order_release);
}

inline void failDirectorySyncAfterForTesting(unsigned successfulCalls) {
    directorySyncFailureCountForTesting.store(
        successfulCalls + 1, std::memory_order_release);
}

inline bool syncDirectory(const std::filesystem::path& directory) {
    unsigned remaining = directorySyncFailureCountForTesting.load(
        std::memory_order_acquire);
    while (remaining != 0) {
        if (directorySyncFailureCountForTesting.compare_exchange_weak(
                remaining, remaining - 1, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            if (remaining == 1) {
                errno = EIO;
                return false;
            }
            break;
        }
    }
    const int dirFd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY);
    if (dirFd < 0) return false;
    const bool ok = (::fsync(dirFd) == 0);
    const bool closeOk = (::close(dirFd) == 0);
    return ok && closeOk;
}

inline bool removeDurably(const std::filesystem::path& path) {
    std::error_code error;
    if (!std::filesystem::remove(path, error) || error) return false;
    const auto parent = path.parent_path().empty()
        ? std::filesystem::path(".") : path.parent_path();
    return syncDirectory(parent);
}

enum class WriteResult { OK, ALREADY_EXISTS, IO_ERROR };

// Publish a fully synced temporary file.  A no-replace publication uses a
// same-directory hard link so even a dangling symlink at the destination is
// treated as occupied, without a check-then-rename race.
inline WriteResult writeAtomicallyImpl(const std::filesystem::path& target,
                                       const std::string& bytes,
                                       bool noReplace) {
    static std::atomic<uint64_t> sequence{0};
    const auto parent = target.parent_path().empty()
        ? std::filesystem::path(".") : target.parent_path();
    // Schema markers use this no-replace path.  An old PID/sequence-based
    // temporary file must not prevent a later CREATE, and a live temporary
    // name must not look like a ".schema_" marker to namespace enumeration.
    std::string tempName;
    int fd = -1;
    if (noReplace) {
        tempName = (parent / (".dbms_atomic_" +
            target.filename().string() + ".XXXXXX")).string();
        fd = ::mkstemp(tempName.data());
    } else {
        tempName = target.string() + ".tmp." + std::to_string(::getpid()) +
                   "." + std::to_string(sequence.fetch_add(
                       1, std::memory_order_relaxed));
        fd = ::open(tempName.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    }
    if (fd < 0) return WriteResult::IO_ERROR;
    const std::filesystem::path temp(tempName);

    bool ok = true;
    size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::write(fd, bytes.data() + written, bytes.size() - written);
        if (n <= 0) { ok = false; break; }
        written += static_cast<size_t>(n);
    }
    if (ok && ::fsync(fd) != 0) ok = false;
    if (::close(fd) != 0) ok = false;

    if (!ok) {
        std::error_code ignored;
        std::filesystem::remove(temp, ignored);
        return WriteResult::IO_ERROR;
    }
    if (noReplace) {
        if (::link(temp.c_str(), target.c_str()) != 0) {
            const int publishError = errno;
            std::error_code ignored;
            std::filesystem::remove(temp, ignored);
            return publishError == EEXIST ? WriteResult::ALREADY_EXISTS
                                          : WriteResult::IO_ERROR;
        }
        if (::unlink(temp.c_str()) != 0) return WriteResult::IO_ERROR;
    } else if (::rename(temp.c_str(), target.c_str()) != 0) {
        std::error_code ignored;
        std::filesystem::remove(temp, ignored);
        return WriteResult::IO_ERROR;
    }

    return syncDirectory(parent) ? WriteResult::OK : WriteResult::IO_ERROR;
}

// Replace an index file atomically and make both the file and its directory
// durable.  Indexes are rebuildable, but a partially written file must never
// be mistaken for a valid index after a crash.
inline bool writeAtomically(const std::filesystem::path& target,
                            const std::string& bytes) {
    return writeAtomicallyImpl(target, bytes, false) == WriteResult::OK;
}

inline WriteResult writeAtomicallyNoReplace(
    const std::filesystem::path& target, const std::string& bytes) {
    return writeAtomicallyImpl(target, bytes, true);
}

} // namespace dbms::index_file
