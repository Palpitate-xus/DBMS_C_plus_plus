#pragma once

#include "common/sha256.h"
#include <atomic>
#include <cerrno>
#include <dirent.h>
#include <filesystem>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <vector>

namespace dbms::derived_map_publication {

inline constexpr char attribute[] = "user.dbms.derived_map_publication";
inline constexpr char suffix[] = ".map_publication";

inline bool sync(int fd) {
    int status;
    do { status = ::fsync(fd); } while (status != 0 && errno == EINTR);
    return status == 0;
}

inline std::string digest(const std::vector<uint8_t>& bytes) {
    SHA256 digest;
    if (!bytes.empty()) digest.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    return digest.finishHex();
}

inline std::string record(uint8_t kind, const struct stat& owner,
                          const std::vector<uint8_t>& bytes) {
    return "DMP1 " + std::to_string(kind) + " " +
        std::to_string(static_cast<uint64_t>(owner.st_dev)) + " " +
        std::to_string(static_cast<uint64_t>(owner.st_ino)) + " " +
        std::to_string(bytes.size()) + " " + std::to_string(owner.st_mtim.tv_sec) + " " +
        std::to_string(owner.st_mtim.tv_nsec) + " " + digest(bytes) + "\n";
}

struct Fd {
    int value = -1;
    explicit Fd(int value) : value(value) {}
    ~Fd() { if (value >= 0) ::close(value); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
};

inline bool sameGeneration(const struct stat& left, const struct stat& right) {
    return left.st_dev == right.st_dev && left.st_ino == right.st_ino &&
        left.st_size == right.st_size && left.st_mtim.tv_sec == right.st_mtim.tv_sec &&
        left.st_mtim.tv_nsec == right.st_mtim.tv_nsec &&
        left.st_ctim.tv_sec == right.st_ctim.tv_sec && left.st_ctim.tv_nsec == right.st_ctim.tv_nsec;
}

inline bool directoryOwned(int fd, const std::filesystem::path& path) {
    struct stat opened{}, current{};
    return ::fstat(fd, &opened) == 0 && ::lstat(path.c_str(), &current) == 0 &&
        S_ISDIR(opened.st_mode) && S_ISDIR(current.st_mode) &&
        opened.st_dev == current.st_dev && opened.st_ino == current.st_ino;
}

inline bool cleanupTemporaryReceipts(int directory, const std::string& leaf) {
    const auto prefix = leaf + ".tmp.";
    const int duplicate = ::dup(directory);
    DIR* entries = duplicate < 0 ? nullptr : ::fdopendir(duplicate);
    if (!entries) { if (duplicate >= 0) (void)::close(duplicate); return false; }
    bool ok = true;
    while (const auto* entry = ::readdir(entries)) {
        const std::string name(entry->d_name);
        if (name.compare(0, prefix.size(), prefix) != 0) continue;
        const auto remainder = name.substr(prefix.size());
        const auto dot = remainder.find('.');
        if (dot == std::string::npos || dot == 0 || dot + 1 == remainder.size() ||
            remainder.find_first_not_of("0123456789.") != std::string::npos ||
            remainder.find('.', dot + 1) != std::string::npos) continue;
        struct stat owned{};
        if (::fstatat(directory, name.c_str(), &owned, AT_SYMLINK_NOFOLLOW) != 0 ||
            !S_ISREG(owned.st_mode) || ::unlinkat(directory, name.c_str(), 0) != 0) ok = false;
    }
    (void)::closedir(entries);
    return ok;
}

inline bool writeSidecar(const std::string& filename, const std::string& receipt) {
    const std::filesystem::path path(filename + suffix);
    const auto parent = path.parent_path().empty() ? std::filesystem::path(".") : path.parent_path();
    Fd directory(::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (directory.value < 0 || !directoryOwned(directory.value, parent)) return false;
    const auto leaf = path.filename().string();
    // The caller holds this map inode's exclusive flock. Only our exact
    // filename's interrupted temporaries can be retired, never another map.
    if (!cleanupTemporaryReceipts(directory.value, leaf)) return false;
    struct stat current{};
    if (::fstatat(directory.value, leaf.c_str(), &current, AT_SYMLINK_NOFOLLOW) == 0) {
        if (!S_ISREG(current.st_mode)) return false;
    } else if (errno != ENOENT) return false;
    static std::atomic<uint64_t> ordinal{0};
    const auto temporary = leaf + ".tmp." + std::to_string(::getpid()) + "." +
                           std::to_string(++ordinal);
    Fd output(::openat(directory.value, temporary.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (output.value < 0) return false;
    bool ok = true;
    size_t offset = 0;
    while (offset < receipt.size()) {
        const auto count = ::write(output.value, receipt.data() + offset, receipt.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { ok = false; break; }
        offset += static_cast<size_t>(count);
    }
    if (ok) ok = sync(output.value);
    const int closed = ::close(output.value);
    output.value = -1;
    if (closed != 0) ok = false;
    if (ok) ok = ::renameat(directory.value, temporary.c_str(), directory.value, leaf.c_str()) == 0;
    if (!ok) (void)::unlinkat(directory.value, temporary.c_str(), 0);
    if (!sync(directory.value)) ok = false;
    return ok && directoryOwned(directory.value, parent);
}

// A receipt is a cooperative publication candidate, not evidence that its
// producer's later fsync succeeded. Consumers must perform their own sync.
inline bool stamp(int fd, const std::string& filename, uint8_t kind,
                  const std::vector<uint8_t>& bytes) {
    struct stat owner{};
    if (::fstat(fd, &owner) != 0 || !S_ISREG(owner.st_mode)) return false;
    const auto receipt = record(kind, owner, bytes);
    if (::fsetxattr(fd, attribute, receipt.data(), receipt.size(), 0) == 0) return true;
    const auto error = errno;
    if (error != ENOTSUP && error != EOPNOTSUPP && error != ENOSYS &&
        error != EPERM && error != EACCES) return false;
    return writeSidecar(filename, receipt);
}

inline bool matchesAttribute(int fd, const std::string& expected) {
    char receipt[512];
    const auto size = ::fgetxattr(fd, attribute, receipt, sizeof(receipt));
    return size == static_cast<ssize_t>(expected.size()) &&
        std::string(receipt, static_cast<size_t>(size)) == expected;
}

inline bool matchesSidecarAt(int directory, const std::string& leaf,
                             const std::string& expected, bool durable) {
    Fd receipt(::openat(directory, leaf.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    struct stat owned{}, current{};
    if (receipt.value < 0 || ::fstat(receipt.value, &owned) != 0 || !S_ISREG(owned.st_mode) ||
        owned.st_size != static_cast<off_t>(expected.size()) ||
        ::fstatat(directory, leaf.c_str(), &current, AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISREG(current.st_mode) || current.st_dev != owned.st_dev || current.st_ino != owned.st_ino)
        return false;
    char bytes[512];
    ssize_t count;
    do { count = ::pread(receipt.value, bytes, sizeof(bytes), 0); } while (count < 0 && errno == EINTR);
    if (count != static_cast<ssize_t>(expected.size()) ||
        std::string(bytes, static_cast<size_t>(count)) != expected) return false;
    if (durable && (!sync(receipt.value) || !sync(directory))) return false;
    struct stat after{};
    if (::fstat(receipt.value, &after) != 0 || !sameGeneration(owned, after) ||
        ::fstatat(directory, leaf.c_str(), &current, AT_SYMLINK_NOFOLLOW) != 0 ||
        !sameGeneration(after, current)) return false;
    do { count = ::pread(receipt.value, bytes, sizeof(bytes), 0); } while (count < 0 && errno == EINTR);
    return count == static_cast<ssize_t>(expected.size()) &&
        std::string(bytes, static_cast<size_t>(count)) == expected;
}

inline bool matchesSidecar(const std::string& filename, const std::string& expected,
                           bool durable) {
    const std::filesystem::path path(filename + suffix);
    const auto parent = path.parent_path().empty() ? std::filesystem::path(".") : path.parent_path();
    Fd directory(::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW));
    if (directory.value < 0 || !directoryOwned(directory.value, parent)) return false;
    bool matched = matchesSidecarAt(directory.value, path.filename().string(), expected, durable);
    // On filesystems without inode-attached xattrs, same-directory hardlinks
    // may publish under another name. A complete exact inode/content receipt
    // is required; neither an alias name nor its timestamp is authorization.
    if (!matched) {
        const int duplicate = ::dup(directory.value);
        DIR* entries = duplicate < 0 ? nullptr : ::fdopendir(duplicate);
        if (entries) {
            while (const auto* entry = ::readdir(entries)) {
                const std::string leaf(entry->d_name);
                const size_t ending = sizeof(suffix) - 1;
                if (leaf.size() >= ending && leaf.compare(leaf.size() - ending, ending, suffix) == 0 &&
                    matchesSidecarAt(directory.value, leaf, expected, durable)) {
                    matched = true;
                    break;
                }
            }
            (void)::closedir(entries);
        } else if (duplicate >= 0) (void)::close(duplicate);
    }
    return matched && directoryOwned(directory.value, parent);
}

inline bool matches(int fd, const std::string& filename, uint8_t kind,
                    const struct stat& owner, const std::vector<uint8_t>& bytes,
                    bool durable) {
    const auto expected = record(kind, owner, bytes);
    if (matchesAttribute(fd, expected)) return true;
    return matchesSidecar(filename, expected, durable);
}

} // namespace dbms::derived_map_publication
