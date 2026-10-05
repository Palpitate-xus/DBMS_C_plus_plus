#include "LargeObject.h"

#include "access/IndexFileUtil.h"

#include <algorithm>
#include <charconv>
#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <string_view>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

namespace dbms {

namespace {

std::filesystem::path parentDirectory(const std::filesystem::path& path) {
    return path.parent_path().empty()
        ? std::filesystem::path(".") : path.parent_path();
}

int openRegularObject(const std::filesystem::path& path, int flags) {
    const int fd = ::open(path.c_str(), flags | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return -1;
    struct stat status {};
    if (::fstat(fd, &status) != 0) {
        const int savedError = errno;
        (void)::close(fd);
        errno = savedError;
        return -1;
    }
    if (!S_ISREG(status.st_mode)) {
        (void)::close(fd);
        errno = EINVAL;
        return -1;
    }
    return fd;
}

bool fitsOffset(size_t value) {
    return static_cast<uintmax_t>(value) <=
        static_cast<uintmax_t>(std::numeric_limits<off_t>::max());
}

bool ensureDirectoryTreeDurable(const std::filesystem::path& directory) {
    std::vector<std::filesystem::path> missing;
    for (auto current = directory; !current.empty();
         current = current.parent_path()) {
        std::error_code ec;
        const auto status = std::filesystem::symlink_status(current, ec);
        if (ec == std::errc::no_such_file_or_directory ||
            (!ec && status.type() == std::filesystem::file_type::not_found)) {
            ec.clear();
            missing.push_back(current);
            continue;
        }
        if (ec || std::filesystem::is_symlink(status) ||
            !std::filesystem::is_directory(status)) {
            return false;
        }
        if (!missing.empty() || current == directory) {
            break;
        }
    }

    std::error_code ec;
    std::filesystem::create_directories(directory, ec);
    if (ec) return false;

    // Persist each newly-created directory entry from the deepest component
    // outward. A large-object file is not durable if its containing database
    // or .lobjects directory can disappear after a crash.
    for (const auto& created : missing) {
        if (!index_file::syncDirectory(parentDirectory(created))) return false;
    }
    return true;
}

}  // namespace

LargeObjectManager::LargeObjectManager(const std::string& dbPath) : dbPath_(dbPath) {
    const std::filesystem::path directory =
        std::filesystem::path(dbPath_) / ".lobjects";
    if (!ensureDirectoryTreeDurable(directory)) return;
    std::error_code ec;
    // Reconstruct the lightweight catalog from the durable object files.
    // Without this scan every new manager restarted allocation at OID 1 and
    // could overwrite a large object created by an earlier instance.
    std::filesystem::directory_iterator entry(directory, ec);
    const std::filesystem::directory_iterator end;
    while (!ec && entry != end) {
        std::error_code fileError;
        const auto fileStatus = entry->symlink_status(fileError);
        if (!fileError && std::filesystem::is_regular_file(fileStatus)) {
            const std::string filename = entry->path().filename().string();
            constexpr std::string_view prefix = "lo_";
            constexpr std::string_view suffix = ".dat";
            if (filename.size() > prefix.size() + suffix.size() &&
                filename.compare(0, prefix.size(), prefix) == 0 &&
                filename.compare(filename.size() - suffix.size(),
                                 suffix.size(), suffix) == 0) {
                const char* first = filename.data() + prefix.size();
                const char* last =
                    filename.data() + filename.size() - suffix.size();
                int id = 0;
                const auto parsed = std::from_chars(first, last, id);
                if (parsed.ec == std::errc{} && parsed.ptr == last && id > 0) {
                    const int fileFd = openRegularObject(
                        entry->path(), O_RDONLY);
                    struct stat objectStatus {};
                    if (fileFd >= 0 &&
                        ::fstat(fileFd, &objectStatus) == 0 &&
                        objectStatus.st_size >= 0 &&
                        static_cast<uintmax_t>(objectStatus.st_size) <=
                            std::numeric_limits<size_t>::max()) {
                        sizes_[id] =
                            static_cast<size_t>(objectStatus.st_size);
                        if (id == std::numeric_limits<int>::max()) {
                            nextId_ = 0;
                        } else if (nextId_ != 0 && id >= nextId_) {
                            nextId_ = id + 1;
                        }
                    }
                    if (fileFd >= 0) (void)::close(fileFd);
                }
            }
        }
        entry.increment(ec);
    }
    if (!ec) ready_ = true;
}

int LargeObjectManager::create() {
    if (!ready_) return 0;
    while (nextId_ > 0) {
        const int id = nextId_;
        nextId_ = id == std::numeric_limits<int>::max() ? 0 : id + 1;

        // Materialize the zero-length object and reserve its ID atomically.
        // This also protects separate manager instances from choosing the
        // same ID when they were constructed concurrently.
        const std::string path = loPath(id);
        const int fd = ::open(path.c_str(),
                              O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
                              0600);
        if (fd >= 0) {
            bool durable = (::fsync(fd) == 0);
            if (::close(fd) != 0) durable = false;
            const auto parent = parentDirectory(path);
            if (durable) durable = index_file::syncDirectory(parent);
            if (!durable) {
                (void)::unlink(path.c_str());
                (void)index_file::syncDirectory(parent);
                return 0;
            }
            sizes_[id] = 0;
            return id;
        }
        if (errno != EEXIST) return 0;

        const int fileFd = openRegularObject(path, O_RDONLY);
        struct stat objectStatus {};
        if (fileFd >= 0 && ::fstat(fileFd, &objectStatus) == 0 &&
            objectStatus.st_size >= 0 &&
            static_cast<uintmax_t>(objectStatus.st_size) <=
                std::numeric_limits<size_t>::max()) {
            sizes_[id] = static_cast<size_t>(objectStatus.st_size);
        }
        if (fileFd >= 0) (void)::close(fileFd);
    }
    return 0;
}

bool LargeObjectManager::write(int loId, size_t offset, const std::string& data) {
    if (!ready_ || loId <= 0) return false;
    auto path = loPath(loId);

    if (!fitsOffset(offset) ||
        data.size() > std::numeric_limits<size_t>::max() - offset ||
        !fitsOffset(offset + data.size())) {
        return false;
    }

    const int fd = openRegularObject(path, O_WRONLY);
    if (fd < 0) return false;
    size_t written = 0;
    bool ok = true;
    while (written < data.size()) {
        const size_t chunk = std::min(
            data.size() - written,
            static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t count = ::pwrite(
            fd, data.data() + written, chunk,
            static_cast<off_t>(offset + written));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            ok = false;
            break;
        }
        written += static_cast<size_t>(count);
    }
    if (ok && !data.empty() && ::fsync(fd) != 0) ok = false;
    if (::close(fd) != 0) ok = false;
    if (!ok || data.empty()) return ok;

    size_t end = offset + data.size();
    if (end > sizes_[loId]) sizes_[loId] = end;
    return true;
}

std::string LargeObjectManager::read(int loId, size_t offset, size_t length) const {
    if (!ready_ || loId <= 0) return "";
    auto path = loPath(loId);
    if (!fitsOffset(offset)) return "";
    const int fd = openRegularObject(path, O_RDONLY);
    if (fd < 0) return "";
    struct stat status {};
    if (::fstat(fd, &status) != 0 || status.st_size <= 0) {
        (void)::close(fd);
        return "";
    }
    if (static_cast<uintmax_t>(status.st_size) >
        std::numeric_limits<size_t>::max()) {
        (void)::close(fd);
        return "";
    }
    const size_t fileSize = static_cast<size_t>(status.st_size);
    if (offset >= fileSize) {
        (void)::close(fd);
        return "";
    }
    const size_t toRead = length == 0
        ? fileSize - offset : std::min(length, fileSize - offset);
    std::string data;
    if (toRead > data.max_size()) {
        (void)::close(fd);
        return "";
    }
    data.resize(toRead);
    size_t received = 0;
    while (received < toRead) {
        const size_t chunk = std::min(
            toRead - received,
            static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t count = ::pread(
            fd, data.data() + received, chunk,
            static_cast<off_t>(offset + received));
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) break;
        received += static_cast<size_t>(count);
    }
    (void)::close(fd);
    data.resize(received);
    return data;
}

bool LargeObjectManager::truncate(int loId, size_t newSize) {
    if (!ready_ || loId <= 0) return false;
    auto path = loPath(loId);
    if (!fitsOffset(newSize)) return false;
    const int fd = openRegularObject(path, O_WRONLY);
    if (fd < 0) return false;
    bool ok = (::ftruncate(fd, static_cast<off_t>(newSize)) == 0);
    if (ok && ::fsync(fd) != 0) ok = false;
    if (::close(fd) != 0) ok = false;
    if (!ok) return false;
    sizes_[loId] = newSize;
    return true;
}

bool LargeObjectManager::drop(int loId) {
    if (!ready_ || loId <= 0) return false;
    auto path = loPath(loId);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) return false;
    sizes_.erase(loId);
    return true;
}

size_t LargeObjectManager::size(int loId) const {
    if (!ready_ || loId <= 0) return 0;
    auto it = sizes_.find(loId);
    return (it != sizes_.end()) ? it->second : 0;
}

bool LargeObjectManager::importFile(int loId, const std::string& filePath) {
    if (!ready_ || loId <= 0) return false;
    const int objectFd = openRegularObject(loPath(loId), O_RDONLY);
    if (objectFd < 0) return false;
    (void)::close(objectFd);

    std::ifstream in(filePath, std::ios::binary | std::ios::ate);
    if (!in) return false;
    const std::streamoff end = in.tellg();
    if (end < 0 ||
        static_cast<uintmax_t>(end) >
            static_cast<uintmax_t>(std::numeric_limits<size_t>::max()) ||
        static_cast<uintmax_t>(end) >
            static_cast<uintmax_t>(std::numeric_limits<std::streamsize>::max())) {
        return false;
    }

    std::string data(static_cast<size_t>(end), '\0');
    in.seekg(0, std::ios::beg);
    if (!in) return false;
    if (!data.empty() &&
        !in.read(data.data(), static_cast<std::streamsize>(data.size()))) {
        return false;
    }

    // Import replaces the object. Writing at offset zero would leave bytes
    // from a previous, longer value at the end of the file.
    if (!index_file::writeAtomically(loPath(loId), data)) return false;
    sizes_[loId] = data.size();
    return true;
}

bool LargeObjectManager::exportFile(int loId, const std::string& filePath) const {
    if (!ready_ || loId <= 0) return false;
    // Open the source first. read() returns an empty string both for a valid
    // zero-length object and for a missing/unreadable object, so using it here
    // would report success and truncate the destination on source failure.
    const int sourceFd = openRegularObject(loPath(loId), O_RDONLY);
    if (sourceFd < 0) return false;
    struct stat sourceStatus {};
    if (::fstat(sourceFd, &sourceStatus) != 0) {
        (void)::close(sourceFd);
        return false;
    }

    // An export onto the same file is already complete. Compare filesystem
    // identity so hard links and symbolic links cannot truncate the source
    // before the streaming copy reads it.
    struct stat destinationStatus {};
    if (::stat(filePath.c_str(), &destinationStatus) == 0 &&
        sourceStatus.st_dev == destinationStatus.st_dev &&
        sourceStatus.st_ino == destinationStatus.st_ino) {
        (void)::close(sourceFd);
        return true;
    }

    std::ofstream out(filePath, std::ios::binary | std::ios::trunc);
    if (!out) {
        (void)::close(sourceFd);
        return false;
    }
    char buffer[64 * 1024];
    bool readOk = true;
    while (out) {
        const ssize_t bytes = ::read(sourceFd, buffer, sizeof(buffer));
        if (bytes < 0 && errno == EINTR) continue;
        if (bytes < 0) {
            readOk = false;
            break;
        }
        if (bytes == 0) break;
        out.write(buffer, bytes);
    }
    out.flush();
    if (::close(sourceFd) != 0) readOk = false;
    return readOk && out.good();
}

std::string LargeObjectManager::loPath(int loId) const {
    return dbPath_ + "/.lobjects/lo_" + std::to_string(loId) + ".dat";
}

} // namespace dbms
