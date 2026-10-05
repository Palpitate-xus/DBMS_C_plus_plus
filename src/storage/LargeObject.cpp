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
#include <system_error>
#include <unistd.h>

namespace dbms {

namespace {

std::filesystem::path parentDirectory(const std::filesystem::path& path) {
    return path.parent_path().empty()
        ? std::filesystem::path(".") : path.parent_path();
}

bool ensureDirectoryTreeDurable(const std::filesystem::path& directory) {
    std::vector<std::filesystem::path> missing;
    for (auto current = directory; !current.empty();
         current = current.parent_path()) {
        std::error_code ec;
        const bool exists = std::filesystem::exists(current, ec);
        if (ec) return false;
        if (exists) {
            if (!std::filesystem::is_directory(current, ec) || ec) return false;
            break;
        }
        missing.push_back(current);
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
        if (entry->is_regular_file(fileError) && !fileError) {
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
                    const uintmax_t bytes =
                        std::filesystem::file_size(entry->path(), fileError);
                    if (!fileError &&
                        bytes <= std::numeric_limits<size_t>::max()) {
                        sizes_[id] = static_cast<size_t>(bytes);
                        if (id == std::numeric_limits<int>::max()) {
                            nextId_ = 0;
                        } else if (nextId_ != 0 && id >= nextId_) {
                            nextId_ = id + 1;
                        }
                    }
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

        std::error_code ec;
        const uintmax_t bytes = std::filesystem::file_size(path, ec);
        if (!ec && bytes <= std::numeric_limits<size_t>::max()) {
            sizes_[id] = static_cast<size_t>(bytes);
        }
    }
    return 0;
}

bool LargeObjectManager::write(int loId, size_t offset, const std::string& data) {
    if (!ready_ || loId <= 0) return false;
    auto path = loPath(loId);

    if (offset > static_cast<size_t>(std::numeric_limits<std::streamoff>::max()) ||
        data.size() > static_cast<size_t>(std::numeric_limits<std::streamsize>::max()) ||
        data.size() > std::numeric_limits<size_t>::max() - offset) {
        return false;
    }

    std::fstream fs(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!fs) return false;

    fs.seekp(static_cast<std::streamoff>(offset));
    if (!fs) return false;
    fs.write(data.data(), static_cast<std::streamsize>(data.size()));
    fs.flush();
    if (!fs) return false;

    // Seeking beyond EOF does not extend a file when no bytes are written.
    if (data.empty()) return true;

    size_t end = offset + data.size();
    if (end > sizes_[loId]) sizes_[loId] = end;
    return true;
}

std::string LargeObjectManager::read(int loId, size_t offset, size_t length) const {
    if (!ready_ || loId <= 0) return "";
    auto path = loPath(loId);
    std::ifstream fs(path, std::ios::binary);
    if (!fs) return "";

    fs.seekg(0, std::ios::end);
    size_t fileSize = static_cast<size_t>(fs.tellg());
    if (offset >= fileSize) return "";

    fs.seekg(static_cast<std::streamoff>(offset));
    size_t toRead = length == 0 ? fileSize - offset : std::min(length, fileSize - offset);
    std::string data(toRead, '\0');
    fs.read(data.data(), static_cast<std::streamsize>(toRead));
    return data;
}

bool LargeObjectManager::truncate(int loId, size_t newSize) {
    if (!ready_ || loId <= 0) return false;
    auto path = loPath(loId);
    std::error_code ec;
    std::filesystem::resize_file(path, newSize, ec);
    if (ec) return false;
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
    std::error_code objectError;
    if (!std::filesystem::is_regular_file(loPath(loId), objectError) ||
        objectError) {
        return false;
    }

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
    std::ifstream in(loPath(loId), std::ios::binary);
    if (!in) return false;

    // An export onto the same file is already complete. Compare filesystem
    // identity so hard links and symbolic links cannot truncate the source
    // before the streaming copy reads it.
    std::error_code identityError;
    if (std::filesystem::equivalent(loPath(loId), filePath, identityError))
        return true;

    std::ofstream out(filePath, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    char buffer[64 * 1024];
    while (in && out) {
        in.read(buffer, sizeof(buffer));
        const std::streamsize bytes = in.gcount();
        if (bytes > 0) out.write(buffer, bytes);
    }
    out.flush();
    return !in.bad() && out.good();
}

std::string LargeObjectManager::loPath(int loId) const {
    return dbPath_ + "/.lobjects/lo_" + std::to_string(loId) + ".dat";
}

} // namespace dbms
