#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace dbms::index_file {

// Replace an index file atomically and make both the file and its directory
// durable.  Indexes are rebuildable, but a partially written file must never
// be mistaken for a valid index after a crash.
inline bool writeAtomically(const std::filesystem::path& target,
                            const std::string& bytes) {
    static std::atomic<uint64_t> sequence{0};
    const auto parent = target.parent_path().empty()
        ? std::filesystem::path(".") : target.parent_path();
    const auto temp = target.string() + ".tmp." + std::to_string(::getpid()) +
                      "." + std::to_string(sequence.fetch_add(1, std::memory_order_relaxed));

    const int fd = ::open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) return false;

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
        return false;
    }
    if (::rename(temp.c_str(), target.c_str()) != 0) {
        std::error_code ignored;
        std::filesystem::remove(temp, ignored);
        return false;
    }

    const int dirFd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
    if (dirFd < 0) return false;
    const bool dirOk = (::fsync(dirFd) == 0);
    ::close(dirFd);
    return dirOk;
}

} // namespace dbms::index_file
