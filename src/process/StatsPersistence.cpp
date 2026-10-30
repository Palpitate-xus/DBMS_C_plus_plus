#include "StatsPersistence.h"

#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <string>
#include <unistd.h>

namespace dbms {

StatsFileLock::StatsFileLock(const std::filesystem::path& snapshotPath) {
    const auto lockPath = snapshotPath.string() + ".lock";
    fd_ = ::open(lockPath.c_str(), O_CREAT | O_RDWR, 0600);
    if (fd_ < 0) return;

    // A process-associated lock does not exclude sibling threads, and an
    // unrelated close on this inode would drop every such process lock.
    // Own this lock through this descriptor instead. OFD locks still
    // conflict with the legacy POSIX locks used by other server processes.
    struct flock lock{};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    int result = 0;
    do {
        result = ::fcntl(fd_, F_OFD_SETLKW, &lock);
    } while (result != 0 && errno == EINTR);
    if (result != 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

StatsFileLock::~StatsFileLock() {
    if (fd_ < 0) return;
    struct flock lock{};
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    ::fcntl(fd_, F_OFD_SETLK, &lock);
    ::close(fd_);
}

bool StatsFileLock::ok() const {
    return fd_ >= 0;
}

bool syncStatsFile(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

bool syncStatsDirectory(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}

StatsSnapshotPublication publishStatsSnapshotWithStatus(
    const std::filesystem::path& destination,
    const std::function<bool(const std::filesystem::path&)>& writer) {
    // Complete every path allocation before a file can be created or renamed.
    const auto parent = destination.parent_path();
    const std::filesystem::path directory = parent.empty()
        ? std::filesystem::path(".") : parent;
    const std::filesystem::path temporary = destination.string() + ".tmp." +
        std::to_string(static_cast<long long>(::getpid()));
    struct Cleanup {
        const std::filesystem::path& path;
        bool renamed = false;
        ~Cleanup() noexcept {
            if (!renamed) (void)::unlink(path.c_str());
        }
    } cleanup{temporary};

    if (!writer(temporary) || !syncStatsFile(temporary))
        return StatsSnapshotPublication::NotPublished;
    if (::rename(temporary.c_str(), destination.c_str()) != 0)
        return StatsSnapshotPublication::NotPublished;
    cleanup.renamed = true;
    // No path construction or other allocating work may follow publication.
    return syncStatsDirectory(directory)
        ? StatsSnapshotPublication::Durable
        : StatsSnapshotPublication::PublishedNotDurable;
}

bool publishStatsSnapshot(
    const std::filesystem::path& destination,
    const std::function<bool(const std::filesystem::path&)>& writer) {
    return publishStatsSnapshotWithStatus(destination, writer) ==
        StatsSnapshotPublication::Durable;
}

} // namespace dbms
