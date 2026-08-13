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

    struct flock lock{};
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    int result = 0;
    do {
        result = ::fcntl(fd_, F_SETLKW, &lock);
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
    ::fcntl(fd_, F_SETLK, &lock);
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

bool publishStatsSnapshot(
    const std::filesystem::path& destination,
    const std::function<bool(const std::filesystem::path&)>& writer) {
    const auto parent = destination.parent_path();
    const auto temporary = destination.string() + ".tmp." +
        std::to_string(static_cast<long long>(::getpid()));
    std::error_code ec;
    if (!writer(temporary) || !syncStatsFile(temporary)) {
        std::filesystem::remove(temporary, ec);
        return false;
    }
    if (::rename(temporary.c_str(), destination.c_str()) != 0 ||
        !syncStatsDirectory(parent.empty() ? "." : parent)) {
        std::filesystem::remove(temporary, ec);
        return false;
    }
    return true;
}

} // namespace dbms
