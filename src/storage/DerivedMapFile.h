#pragma once

#include <cerrno>
#include <climits>
#include <cstdint>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace dbms::derived_map_file {

// Map state may only be read/published through its actual opened inode.
// A table restore/rename can republish the path while raw callers retain a map.
inline bool owned(int fd, const std::string& filename, struct stat* result = nullptr) {
    struct stat opened{}, current{};
    if (fd < 0 || ::fstat(fd, &opened) != 0 || ::lstat(filename.c_str(), &current) != 0 ||
        !S_ISREG(opened.st_mode) || !S_ISREG(current.st_mode) ||
        opened.st_dev != current.st_dev || opened.st_ino != current.st_ino) return false;
    if (result) *result = opened;
    return true;
}

inline bool read(int fd, const std::string& filename, std::vector<uint8_t>& data) {
    struct stat before{};
    if (!owned(fd, filename, &before) || before.st_size < 0 ||
        static_cast<uint64_t>(before.st_size) > UINT32_MAX) return false;
    std::vector<uint8_t> bytes;
    try { bytes.resize(static_cast<size_t>(before.st_size)); }
    catch (...) { return false; }
    size_t offset = 0;
    while (offset < bytes.size()) {
        const auto count = ::pread(fd, bytes.data() + offset, bytes.size() - offset,
                                   static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        offset += static_cast<size_t>(count);
    }
    struct stat after{};
    if (!owned(fd, filename, &after) || before.st_size != after.st_size ||
        before.st_mtim.tv_sec != after.st_mtim.tv_sec || before.st_mtim.tv_nsec != after.st_mtim.tv_nsec ||
        before.st_ctim.tv_sec != after.st_ctim.tv_sec || before.st_ctim.tv_nsec != after.st_ctim.tv_nsec)
        return false;
    data.swap(bytes);
    return true;
}

inline int open(const std::string& filename, std::vector<uint8_t>& data) {
    const int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0644);
    if (fd < 0) return -1;
    if (!read(fd, filename, data)) { ::close(fd); return -1; }
    return fd;
}

inline bool write(int fd, const std::string& filename, const std::vector<uint8_t>& data) {
    if (!owned(fd, filename)) return false;
    size_t offset = 0;
    while (offset < data.size()) {
        const auto count = ::pwrite(fd, data.data() + offset, data.size() - offset,
                                    static_cast<off_t>(offset));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) return false;
        offset += static_cast<size_t>(count);
    }
    if (::ftruncate(fd, static_cast<off_t>(data.size())) != 0) return false;
    int synced;
    do { synced = ::fsync(fd); } while (synced != 0 && errno == EINTR);
    return synced == 0 && owned(fd, filename);
}

inline bool matches(int fd, const std::string& filename, const std::vector<uint8_t>& data) {
    std::vector<uint8_t> actual;
    return read(fd, filename, actual) && actual == data;
}

} // namespace dbms::derived_map_file
