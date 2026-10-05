#include "DataDirectory.h"

#include "access/BPTreeFormat.h"
#include "access/BloomIndexFormat.h"
#include "access/GinIndexFormat.h"
#include "access/HashIndexFormat.h"
#include "access/IndexChecksum.h"
#include "access/IndexFileUtil.h"
#include "common/Config.h"
#include "storage/DataFileHeader.h"
#include "storage/PageCrypto.h"
#include "storage/PgPage.h"
#include "storage/WAL.h"
#include "interfaces/dbms_defs.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <sys/file.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace dbms {
namespace {

struct BootstrapState {
    std::filesystem::path root;
    std::string systemIdentifier;
    int lockFd = -1;
    bool ready = false;
    ~BootstrapState() {
        if (lockFd >= 0) (void)::close(lockFd);
    }
};

class DirectoryLock {
public:
    ~DirectoryLock() {
        if (fd_ >= 0) (void)::close(fd_);
    }
    DirectoryLock(const DirectoryLock&) = delete;
    DirectoryLock& operator=(const DirectoryLock&) = delete;
    DirectoryLock() = default;

    bool acquire(const std::filesystem::path& root, std::string& error) {
        fd_ = ::open(root.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (fd_ < 0) {
            error = "could not open data directory for instance lock: " +
                std::string(std::strerror(errno));
            return false;
        }
        if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
            error = (errno == EWOULDBLOCK || errno == EAGAIN)
                ? "data directory is already in use by another process"
                : "could not lock data directory: " +
                    std::string(std::strerror(errno));
            return false;
        }
        return true;
    }

    int release() {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

private:
    int fd_ = -1;
};

constexpr const char* kCurrentControlMagic =
    "DBMS_CPP_CLUSTER_CONTROL_V3";
constexpr const char* kVersion2ControlMagic =
    "DBMS_CPP_CLUSTER_CONTROL_V2";
constexpr const char* kLegacyControlMagic =
    "DBMS_CPP_CLUSTER_CONTROL_V1";
constexpr uint32_t kControlFormatVersion = 3;
constexpr uint32_t kCatalogFormatVersion = 1;
constexpr const char* kCurrentFeatureFlags = "00000000";
constexpr size_t kMaxControlFileBytes = 1024;

const char* nativeByteOrder() {
    const uint16_t marker = 1;
    return *reinterpret_cast<const unsigned char*>(&marker) == 1
        ? "little" : "big";
}

uint32_t controlChecksum(const std::string& data) {
    uint32_t crc = 0xFFFFFFFFu;
    for (unsigned char byte : data) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc >> 1) ^
                (0x82F63B78u & static_cast<uint32_t>(-(crc & 1u)));
        }
    }
    return crc ^ 0xFFFFFFFFu;
}

std::string formatControlChecksum(uint32_t checksum) {
    std::ostringstream output;
    output << std::hex << std::nouppercase << std::setfill('0')
           << std::setw(8) << checksum;
    return output.str();
}

std::string currentControlContents(const std::string& systemIdentifier) {
    const std::string prefix = std::string(kCurrentControlMagic) + "\n" +
        "control_format_version=" +
        std::to_string(kControlFormatVersion) + "\n" +
        "catalog_format_version=" +
        std::to_string(kCatalogFormatVersion) + "\n" +
        "heap_format_version=" +
        std::to_string(DATA_FILE_FORMAT_VERSION) + "\n" +
        "block_size=" + std::to_string(BLCKSZ) + "\n" +
        "wal_segment_size=" +
        std::to_string(WALManager::kSegmentSize) + "\n" +
        "byte_order=" + nativeByteOrder() + "\n" +
        "feature_flags=" + std::string(kCurrentFeatureFlags) + "\n" +
        "system_identifier=" + systemIdentifier + "\n";
    return prefix + "control_checksum=" +
        formatControlChecksum(controlChecksum(prefix)) + "\n";
}

BootstrapState& state() {
    static BootstrapState value;
    return value;
}

std::vector<std::string> processArguments() {
    std::ifstream input("/proc/self/cmdline", std::ios::binary);
    if (!input) return {};
    const std::string bytes(
        (std::istreambuf_iterator<char>(input)),
        std::istreambuf_iterator<char>());
    std::vector<std::string> arguments;
    size_t start = 0;
    while (start < bytes.size()) {
        const size_t end = bytes.find('\0', start);
        arguments.push_back(bytes.substr(start, end - start));
        if (end == std::string::npos) break;
        start = end + 1;
    }
    return arguments;
}

bool parseSelectedDirectory(std::filesystem::path& selected,
                            std::string& error) {
    const auto arguments = processArguments();
    std::string argumentValue;
    for (size_t i = 1; i < arguments.size(); ++i) {
        const auto& argument = arguments[i];
        std::string value;
        if (argument == "-D" || argument == "--data-dir") {
            if (i + 1 >= arguments.size() || arguments[i + 1].empty()) {
                error = argument + " requires a directory";
                return false;
            }
            value = arguments[++i];
        } else if (argument.rfind("--data-dir=", 0) == 0) {
            value = argument.substr(11);
            if (value.empty()) {
                error = "--data-dir requires a directory";
                return false;
            }
        } else {
            continue;
        }
        if (!argumentValue.empty() && argumentValue != value) {
            error = "conflicting data directory arguments";
            return false;
        }
        argumentValue = value;
    }

    const char* environment = std::getenv("DBMS_DATA_DIR");
    const std::string environmentValue = environment ? environment : "";
    if (argumentValue.empty() && environmentValue.empty()) {
        error = "an explicit data directory is required (-D/--data-dir or "
                "DBMS_DATA_DIR)";
        return false;
    }

    std::error_code filesystemError;
    const auto launchDirectory = std::filesystem::current_path(filesystemError);
    if (filesystemError) {
        error = "could not resolve launch directory: " +
            filesystemError.message();
        return false;
    }
    const auto normalize = [&](const std::string& raw,
                               std::filesystem::path& result) {
        std::filesystem::path path(raw);
        if (path.is_relative()) path = launchDirectory / path;
        result = std::filesystem::weakly_canonical(path, filesystemError);
        return !filesystemError;
    };

    std::filesystem::path argumentPath;
    std::filesystem::path environmentPath;
    if (!argumentValue.empty() && !normalize(argumentValue, argumentPath)) {
        error = "could not resolve data directory: " + filesystemError.message();
        return false;
    }
    filesystemError.clear();
    if (!environmentValue.empty() &&
        !normalize(environmentValue, environmentPath)) {
        error = "could not resolve DBMS_DATA_DIR: " + filesystemError.message();
        return false;
    }
    if (!argumentValue.empty() && !environmentValue.empty() &&
        argumentPath != environmentPath) {
        error = "-D/--data-dir conflicts with DBMS_DATA_DIR";
        return false;
    }
    selected = !argumentValue.empty() ? argumentPath : environmentPath;
    return true;
}

bool validSystemIdentifier(const std::string& value) {
    if (value.size() != 16) return false;
    for (char c : value) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return false;
    }
    return value != "0000000000000000";
}

std::string newSystemIdentifier() {
    uint64_t value = 0;
    std::ifstream random("/dev/urandom", std::ios::binary);
    if (random) random.read(reinterpret_cast<char*>(&value), sizeof(value));
    if (value == 0) {
        value = static_cast<uint64_t>(
            std::chrono::high_resolution_clock::now()
                .time_since_epoch().count()) ^
            (static_cast<uint64_t>(::getpid()) << 32);
        if (value == 0) value = 1;
    }
    std::ostringstream output;
    output << std::hex << std::setfill('0') << std::setw(16) << value;
    return output.str();
}

enum class ControlFileVersion {
    Current,
    Version2,
    LegacyV1
};

std::string controlVersionUpgradeRequired(ControlFileVersion version) {
    const char* number = version == ControlFileVersion::LegacyV1 ? "1" : "2";
    return "data directory uses control version " + std::string(number) +
        "; offline upgrade is required";
}

bool loadControlFile(const std::filesystem::path& path,
                     std::string& systemIdentifier,
                     ControlFileVersion& version,
                     std::string& error) {
    const int fd = ::open(
        path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) {
        error = "could not open DBMS_CONTROL: " +
            std::string(std::strerror(errno));
        return false;
    }
    struct stat metadata {};
    if (::fstat(fd, &metadata) != 0) {
        const int savedErrno = errno;
        (void)::close(fd);
        error = "could not inspect DBMS_CONTROL: " +
            std::string(std::strerror(savedErrno));
        return false;
    }
    if (!S_ISREG(metadata.st_mode)) {
        (void)::close(fd);
        error = "DBMS_CONTROL is not a regular file";
        return false;
    }
    if (metadata.st_size < 0 ||
        static_cast<uint64_t>(metadata.st_size) > kMaxControlFileBytes) {
        (void)::close(fd);
        error = "DBMS_CONTROL exceeds the maximum supported size";
        return false;
    }
    std::array<char, kMaxControlFileBytes + 1> buffer {};
    size_t bytesRead = 0;
    while (bytesRead < buffer.size()) {
        const ssize_t count = ::read(
            fd, buffer.data() + bytesRead, buffer.size() - bytesRead);
        if (count < 0 && errno == EINTR) continue;
        if (count < 0) {
            const int savedErrno = errno;
            (void)::close(fd);
            error = "could not read DBMS_CONTROL: " +
                std::string(std::strerror(savedErrno));
            return false;
        }
        if (count == 0) break;
        bytesRead += static_cast<size_t>(count);
    }
    (void)::close(fd);
    if (bytesRead > kMaxControlFileBytes) {
        error = "DBMS_CONTROL exceeds the maximum supported size";
        return false;
    }
    if (bytesRead == 0 || buffer[bytesRead - 1] != '\n') {
        error = "invalid DBMS_CONTROL format or missing final LF";
        return false;
    }
    std::istringstream input(std::string(buffer.data(), bytesRead));
    std::string header;
    if (!std::getline(input, header)) {
        error = "invalid DBMS_CONTROL format or magic";
        return false;
    }

    std::string identifier;
    std::string trailing;
    if (header == kLegacyControlMagic) {
        std::string format;
        if (!std::getline(input, format) ||
            !std::getline(input, identifier) ||
            std::getline(input, trailing) ||
            format != "format_version=1" ||
            identifier.rfind("system_identifier=", 0) != 0) {
            error = "invalid DBMS_CONTROL version 1 format";
            return false;
        }
        version = ControlFileVersion::LegacyV1;
    } else if (header == kVersion2ControlMagic) {
        std::array<std::string, 6> fields;
        for (auto& field : fields) {
            if (!std::getline(input, field)) {
                error = "invalid DBMS_CONTROL version 2 format";
                return false;
            }
        }
        if (std::getline(input, trailing) ||
            fields[0] != "control_format_version=2" ||
            fields[1] != "catalog_format_version=1" ||
            fields[2] != "heap_format_version=2" ||
            fields[3] != "block_size=8192" ||
            fields[4] != std::string("byte_order=") + nativeByteOrder() ||
            fields[5].rfind("system_identifier=", 0) != 0) {
            error = "incompatible or invalid DBMS_CONTROL version 2 fields";
            return false;
        }
        identifier = fields[5];
        version = ControlFileVersion::Version2;
    } else if (header == kCurrentControlMagic) {
        std::array<std::string, 9> fields;
        for (auto& field : fields) {
            if (!std::getline(input, field)) {
                error = "invalid DBMS_CONTROL version 3 format";
                return false;
            }
        }
        if (fields[0] != "control_format_version=3" ||
            fields[1] != "catalog_format_version=1" ||
            fields[2] != "heap_format_version=2" ||
            fields[3] != "block_size=8192" ||
            fields[4] != std::string("wal_segment_size=") +
                std::to_string(WALManager::kSegmentSize) ||
            fields[5] != std::string("byte_order=") + nativeByteOrder() ||
            fields[6] != std::string("feature_flags=") +
                kCurrentFeatureFlags ||
            fields[7].rfind("system_identifier=", 0) != 0) {
            error = "incompatible or invalid DBMS_CONTROL version 3 fields";
            return false;
        }
        std::string prefix = header + "\n";
        for (size_t i = 0; i < 8; ++i) prefix += fields[i] + "\n";
        const std::string expectedChecksum =
            "control_checksum=" +
            formatControlChecksum(controlChecksum(prefix));
        if (fields[8] != expectedChecksum) {
            error = "DBMS_CONTROL checksum mismatch";
            return false;
        }
        if (std::getline(input, trailing)) {
            error = "incompatible or invalid DBMS_CONTROL version 3 fields";
            return false;
        }
        identifier = fields[7];
        version = ControlFileVersion::Current;
    } else {
        if (header.rfind("DBMS_CPP_CLUSTER_CONTROL_V", 0) == 0) {
            error = "unsupported DBMS_CONTROL version";
        } else {
            error = "invalid DBMS_CONTROL format or magic";
        }
        return false;
    }

    systemIdentifier = identifier.substr(18);
    if (!validSystemIdentifier(systemIdentifier)) {
        error = "invalid DBMS_CONTROL system identifier";
        return false;
    }
    return true;
}

bool initializeControlFile(const std::filesystem::path& root,
                           std::string& systemIdentifier,
                           std::string& error) {
    const auto control = root / "DBMS_CONTROL";
    std::error_code filesystemError;
    if (std::filesystem::exists(control, filesystemError)) {
        if (filesystemError ||
            !std::filesystem::is_regular_file(control, filesystemError) ||
            filesystemError) {
            error = "DBMS_CONTROL is not a regular file";
            return false;
        }
        ControlFileVersion version = ControlFileVersion::Current;
        if (!loadControlFile(control, systemIdentifier, version, error)) {
            return false;
        }
        if (version != ControlFileVersion::Current) {
            error = controlVersionUpgradeRequired(version) +
                "; run dbms_main -D <data-directory> "
                "--upgrade-data-directory";
            return false;
        }
        return true;
    }
    if (filesystemError) {
        error = "could not inspect DBMS_CONTROL: " + filesystemError.message();
        return false;
    }
    if (std::filesystem::exists(root / "PG_VERSION")) {
        error = "data directory is a PostgreSQL cluster, not a DBMS-C++ cluster";
        return false;
    }

    bool empty = std::filesystem::is_empty(root, filesystemError);
    if (filesystemError) {
        error = "could not inspect data directory: " + filesystemError.message();
        return false;
    }
    const bool legacyDbms =
        std::filesystem::is_regular_file(root / "info" / "tlist.lst") ||
        std::filesystem::is_regular_file(root / ".replication_slots") ||
        std::filesystem::is_regular_file(root / ".dbms_maintenance_jobs") ||
        std::filesystem::is_regular_file(root / "dbms.conf") ||
        std::filesystem::is_regular_file(root / "pg_hba.conf");
    if (!empty && !legacyDbms) {
        error = "non-empty data directory has no DBMS cluster identity";
        return false;
    }

    systemIdentifier = newSystemIdentifier();
    const std::string contents = currentControlContents(systemIdentifier);
    if (!index_file::writeAtomically(control, contents)) {
        error = "could not create durable DBMS_CONTROL";
        return false;
    }
    return true;
}

bool hasHeapFileSuffix(const std::filesystem::path& path) {
    const std::string name = path.filename().string();
    constexpr const char* heapSuffix = ".dt";
    constexpr const char* initSuffix = ".dt.init";
    return (name.size() >= std::strlen(heapSuffix) &&
            name.compare(name.size() - std::strlen(heapSuffix),
                         std::strlen(heapSuffix), heapSuffix) == 0) ||
           (name.size() >= std::strlen(initSuffix) &&
            name.compare(name.size() - std::strlen(initSuffix),
                         std::strlen(initSuffix), initSuffix) == 0);
}

bool hasBTreeIndexSuffix(const std::filesystem::path& path) {
    const std::string name = path.filename().string();
    constexpr const char* indexSuffix = ".idx";
    constexpr const char* encryptedSuffix = ".tde";
    if (name.size() >= std::strlen(encryptedSuffix) &&
        name.compare(name.size() - std::strlen(encryptedSuffix),
                     std::strlen(encryptedSuffix), encryptedSuffix) == 0) {
        return false;
    }
    if (name.size() >= std::strlen(indexSuffix) &&
        name.compare(name.size() - std::strlen(indexSuffix),
                     std::strlen(indexSuffix), indexSuffix) == 0) {
        return true;
    }
    const size_t composite = name.rfind(".idx_");
    return composite != std::string::npos &&
           composite + std::strlen(".idx_") < name.size();
}

bool hasHashIndexSuffix(const std::filesystem::path& path) {
    return path.filename().extension() == ".hidx";
}

bool hasBloomIndexSuffix(const std::filesystem::path& path) {
    return path.filename().extension() == ".bidx";
}

bool hasGinIndexSuffix(const std::filesystem::path& path) {
    return path.filename().extension() == ".gin";
}

std::string displayVerificationPath(const std::filesystem::path& root,
                                    const std::filesystem::path& path) {
    const auto relative = path.lexically_relative(root);
    if (!relative.empty()) {
        const std::string value = relative.generic_string();
        if (value != ".." && value.rfind("../", 0) != 0) return value;
    }
    return path.generic_string();
}

bool readExactlyAt(int fd, void* buffer, size_t size, off_t offset) {
    auto* bytes = static_cast<char*>(buffer);
    size_t consumed = 0;
    while (consumed < size) {
        const ssize_t amount = ::pread(
            fd, bytes + consumed, size - consumed,
            offset + static_cast<off_t>(consumed));
        if (amount < 0 && errno == EINTR) continue;
        if (amount <= 0) return false;
        consumed += static_cast<size_t>(amount);
    }
    return true;
}

class ReadOnlyDescriptor {
public:
    explicit ReadOnlyDescriptor(const std::filesystem::path& path)
        : fd_(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW)) {}
    ~ReadOnlyDescriptor() {
        if (fd_ >= 0) (void)::close(fd_);
    }
    ReadOnlyDescriptor(const ReadOnlyDescriptor&) = delete;
    ReadOnlyDescriptor& operator=(const ReadOnlyDescriptor&) = delete;
    int get() const { return fd_; }

private:
    int fd_ = -1;
};

bool readTablespaceMarker(const std::filesystem::path& marker,
                          std::string& target, std::string& error) {
    target.clear();
    ReadOnlyDescriptor descriptor(marker);
    if (descriptor.get() < 0) {
        error = "could not open tablespace marker without following links: " +
            marker.string();
        return false;
    }
    struct stat status {};
    constexpr off_t maximumBytes = 64 * 1024;
    if (::fstat(descriptor.get(), &status) != 0 ||
        !S_ISREG(status.st_mode) || status.st_size <= 0 ||
        status.st_size > maximumBytes) {
        error = "invalid tablespace marker: " + marker.string();
        return false;
    }
    target.resize(static_cast<size_t>(status.st_size));
    if (!readExactlyAt(
            descriptor.get(), target.data(), target.size(), 0)) {
        error = "could not read tablespace marker: " + marker.string();
        return false;
    }
    if (!target.empty() && target.back() == '\n') target.pop_back();
    if (target.empty() || target.find('\0') != std::string::npos ||
        target.find('\n') != std::string::npos ||
        target.find('\r') != std::string::npos) {
        error = "invalid tablespace marker: " + marker.string();
        return false;
    }
    return true;
}

struct HeapVerificationStats {
    uint64_t files = 0;
    uint64_t blocks = 0;
    uint64_t identityBoundBlocks = 0;
    uint64_t legacyIdentityUnboundBlocks = 0;
};

struct IndexVerificationStats {
    uint64_t files = 0;
    uint64_t pages = 0;
    uint64_t pageBoundPages = 0;
    uint64_t contentOnlyPages = 0;
    uint64_t uncheckedPages = 0;
};

struct HashIndexVerificationStats {
    uint64_t files = 0;
    uint64_t checksumFiles = 0;
    uint64_t uncheckedLegacyFiles = 0;
};

struct BloomIndexVerificationStats {
    uint64_t files = 0;
    uint64_t checksumFiles = 0;
    uint64_t uncheckedLegacyFiles = 0;
};

struct GinIndexVerificationStats {
    uint64_t files = 0;
    uint64_t checksumFiles = 0;
    uint64_t uncheckedLegacyFiles = 0;
};

bool loadVerificationKey(const std::filesystem::path& root,
                         std::string& error) {
    PageCrypto::disable();
    const auto configPath = root / "dbms.conf";
    std::error_code ec;
    const bool hasConfig = std::filesystem::exists(configPath, ec);
    if (ec) {
        error = "could not inspect dbms.conf: " + ec.message();
        return false;
    }
    if (!hasConfig) return true;

    Config config;
    if (!config.load(configPath.string())) {
        error = "invalid dbms.conf; cannot determine checksum decryption key";
        return false;
    }
    if (config.tdeKeyring.empty()) return true;

    std::filesystem::path keyring(config.tdeKeyring);
    if (keyring.is_relative()) keyring = root / keyring;
    std::ifstream input(keyring);
    if (!input) {
        error = "could not read TDE keyring without modifying it: " +
            keyring.string();
        return false;
    }
    std::string key;
    if (!std::getline(input, key)) {
        error = "TDE keyring is empty: " + keyring.string();
        return false;
    }
    const size_t last = key.find_last_not_of(" \t\r\n");
    key.erase(last == std::string::npos ? 0 : last + 1);
    if (!PageCrypto::enable(key)) {
        error = "TDE keyring does not contain a 64-hex-char key: " +
            keyring.string();
        return false;
    }
    return true;
}

bool readTablespaceRoots(const std::filesystem::path& root,
                         std::set<std::filesystem::path>& roots,
                         std::string& error) {
    std::error_code ec;
    std::filesystem::directory_iterator databases(root, ec);
    if (ec) {
        error = "could not enumerate data directory: " + ec.message();
        return false;
    }
    const std::filesystem::directory_iterator end;
    for (; databases != end; databases.increment(ec)) {
        if (ec) {
            error = "could not enumerate data directory: " + ec.message();
            return false;
        }
        const auto databaseStatus = databases->symlink_status(ec);
        if (ec) {
            error = "could not inspect " + databases->path().string() +
                ": " + ec.message();
            return false;
        }
        if (databaseStatus.type() != std::filesystem::file_type::directory)
            continue;

        const auto tablespaceDirectory = databases->path() / "pg_tblspc";
        const bool hasTablespaces =
            std::filesystem::exists(tablespaceDirectory, ec);
        if (ec) {
            error = "could not inspect " + tablespaceDirectory.string() +
                ": " + ec.message();
            return false;
        }
        if (!hasTablespaces) continue;
        const auto tablespaceStatus =
            std::filesystem::symlink_status(tablespaceDirectory, ec);
        if (ec || tablespaceStatus.type() !=
                      std::filesystem::file_type::directory) {
            error = "tablespace metadata path is not a directory: " +
                tablespaceDirectory.string();
            return false;
        }

        std::filesystem::directory_iterator markers(tablespaceDirectory, ec);
        if (ec) {
            error = "could not enumerate tablespace metadata: " + ec.message();
            return false;
        }
        for (; markers != end; markers.increment(ec)) {
            if (ec) {
                error = "could not enumerate tablespace metadata: " +
                    ec.message();
                return false;
            }
            if (markers->path().extension() != ".path") continue;
            const auto markerStatus = markers->symlink_status(ec);
            if (ec || markerStatus.type() !=
                          std::filesystem::file_type::regular) {
                error = "tablespace marker is not a regular file: " +
                    markers->path().string();
                return false;
            }
            std::string target;
            if (!readTablespaceMarker(markers->path(), target, error))
                return false;
            std::filesystem::path tablespaceRoot(target);
            if (tablespaceRoot.is_relative()) tablespaceRoot = root / tablespaceRoot;
            tablespaceRoot = std::filesystem::weakly_canonical(tablespaceRoot, ec);
            if (ec) {
                error = "could not resolve tablespace directory: " + target;
                return false;
            }
            const auto rootStatus =
                std::filesystem::symlink_status(tablespaceRoot, ec);
            if (ec || rootStatus.type() !=
                          std::filesystem::file_type::directory) {
                error = "tablespace directory is unavailable: " +
                    tablespaceRoot.string();
                return false;
            }
            const auto databaseRoot =
                tablespaceRoot / databases->path().filename();
            const bool databaseRootExists =
                std::filesystem::exists(databaseRoot, ec);
            if (ec) {
                error = "could not inspect tablespace database directory: " +
                    databaseRoot.string();
                return false;
            }
            if (!databaseRootExists) continue;
            const auto databaseRootStatus =
                std::filesystem::symlink_status(databaseRoot, ec);
            if (ec || databaseRootStatus.type() !=
                          std::filesystem::file_type::directory) {
                error = "tablespace database path is not a directory: " +
                    databaseRoot.string();
                return false;
            }
            roots.insert(std::filesystem::weakly_canonical(databaseRoot, ec));
            if (ec) {
                error = "could not resolve tablespace database directory: " +
                    databaseRoot.string();
                return false;
            }
        }
        if (ec) {
            error = "could not enumerate tablespace metadata: " + ec.message();
            return false;
        }
    }
    if (ec) {
        error = "could not enumerate data directory: " + ec.message();
        return false;
    }
    return true;
}

bool collectHeapFiles(const std::filesystem::path& scanRoot,
                      std::set<std::filesystem::path>& files,
                      std::string& error) {
    std::error_code ec;
    std::filesystem::recursive_directory_iterator iterator(scanRoot, ec);
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    const std::filesystem::recursive_directory_iterator end;
    for (; iterator != end; iterator.increment(ec)) {
        if (ec) {
            error = "could not enumerate checksum root " + scanRoot.string() +
                ": " + ec.message();
            return false;
        }
        const auto path = iterator->path();
        if (!hasHeapFileSuffix(path)) continue;
        const auto status = iterator->symlink_status(ec);
        if (ec || status.type() != std::filesystem::file_type::regular) {
            error = "heap relation is not a regular file: " + path.string();
            return false;
        }
        files.insert(std::filesystem::weakly_canonical(path, ec));
        if (ec) {
            error = "could not resolve heap relation: " + path.string();
            return false;
        }
    }
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    return true;
}

bool collectBTreeFiles(const std::filesystem::path& scanRoot,
                       std::set<std::filesystem::path>& files,
                       std::string& error) {
    std::error_code ec;
    std::filesystem::recursive_directory_iterator iterator(scanRoot, ec);
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    const std::filesystem::recursive_directory_iterator end;
    for (; iterator != end; iterator.increment(ec)) {
        if (ec) {
            error = "could not enumerate checksum root " + scanRoot.string() +
                ": " + ec.message();
            return false;
        }
        const auto path = iterator->path();
        if (!hasBTreeIndexSuffix(path)) continue;
        const auto status = iterator->symlink_status(ec);
        if (ec || status.type() != std::filesystem::file_type::regular) {
            error = "B+ tree index is not a regular file: " + path.string();
            return false;
        }
        files.insert(std::filesystem::weakly_canonical(path, ec));
        if (ec) {
            error = "could not resolve B+ tree index: " + path.string();
            return false;
        }
    }
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    return true;
}

bool collectHashIndexFiles(const std::filesystem::path& scanRoot,
                           std::set<std::filesystem::path>& files,
                           std::string& error) {
    std::error_code ec;
    std::filesystem::recursive_directory_iterator iterator(scanRoot, ec);
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    const std::filesystem::recursive_directory_iterator end;
    for (; iterator != end; iterator.increment(ec)) {
        if (ec) {
            error = "could not enumerate checksum root " + scanRoot.string() +
                ": " + ec.message();
            return false;
        }
        const auto path = iterator->path();
        if (!hasHashIndexSuffix(path)) continue;
        const auto status = iterator->symlink_status(ec);
        if (ec || status.type() != std::filesystem::file_type::regular) {
            error = "hash index is not a regular file: " + path.string();
            return false;
        }
        files.insert(std::filesystem::weakly_canonical(path, ec));
        if (ec) {
            error = "could not resolve hash index: " + path.string();
            return false;
        }
    }
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    return true;
}

bool verifyHashIndex(const std::filesystem::path& root,
                     const std::filesystem::path& path,
                     HashIndexVerificationStats& stats,
                     std::string& error) {
    using namespace hash_index_format;
    const std::string shown = displayVerificationPath(root, path);
    ReadOnlyDescriptor data(path);
    if (data.get() < 0) {
        error = shown + ": could not open hash index read-only: " +
            std::strerror(errno);
        return false;
    }
    struct stat status {};
    constexpr uint64_t headerBytes =
        sizeof(uint32_t) * 2 + sizeof(uint64_t);
    if (::fstat(data.get(), &status) != 0 || !S_ISREG(status.st_mode) ||
        status.st_size < 0 ||
        static_cast<uint64_t>(status.st_size) < headerBytes) {
        error = shown + ": truncated or invalid hash index file";
        return false;
    }

    std::array<char, headerBytes> header{};
    if (!readExactlyAt(data.get(), header.data(), header.size(), 0)) {
        error = shown + ": short read of hash index header";
        return false;
    }
    uint32_t magic = 0;
    uint32_t version = 0;
    uint64_t count = 0;
    size_t headerOffset = 0;
    std::memcpy(&magic, header.data() + headerOffset, sizeof(magic));
    headerOffset += sizeof(magic);
    std::memcpy(&version, header.data() + headerOffset, sizeof(version));
    headerOffset += sizeof(version);
    std::memcpy(&count, header.data() + headerOffset, sizeof(count));
    if (magic != kMagic || count > kMaxEntries ||
        (version != kLegacyVersion && version != kChecksumVersion)) {
        error = shown + ": invalid hash index header";
        return false;
    }

    const uint64_t fileBytes = static_cast<uint64_t>(status.st_size);
    if (version == kLegacyVersion) {
        if (count > (fileBytes - headerBytes) /
                        (sizeof(uint64_t) * 2)) {
            error = shown + ": truncated legacy hash index";
            return false;
        }
        ++stats.uncheckedLegacyFiles;
        ++stats.files;
        return true;
    }

    if (fileBytes < headerBytes + kChecksumBytes) {
        error = shown + ": truncated checksummed hash index";
        return false;
    }
    const uint64_t payloadBytes = fileBytes - kChecksumBytes;
    if (count > (payloadBytes - headerBytes) /
                    (sizeof(uint64_t) * 2)) {
        error = shown + ": invalid checksummed hash index count";
        return false;
    }
    uint32_t storedChecksum = 0;
    if (!readExactlyAt(data.get(), &storedChecksum, sizeof(storedChecksum),
                       static_cast<off_t>(payloadBytes))) {
        error = shown + ": short read of hash index checksum";
        return false;
    }

    uint64_t remaining = payloadBytes;
    off_t position = 0;
    uint32_t checksumState = 0xFFFFFFFFu;
    std::array<char, 64 * 1024> chunk{};
    while (remaining > 0) {
        const size_t amount = static_cast<size_t>(
            std::min<uint64_t>(remaining, chunk.size()));
        if (!readExactlyAt(data.get(), chunk.data(), amount, position)) {
            error = shown + ": short read while verifying hash index checksum";
            return false;
        }
        checksumState = index_checksum::crc32cUpdate(
            checksumState, chunk.data(), amount);
        position += static_cast<off_t>(amount);
        remaining -= amount;
    }
    if (index_checksum::crc32cFinish(checksumState) != storedChecksum) {
        error = shown + ": hash index checksum mismatch";
        return false;
    }
    ++stats.checksumFiles;
    ++stats.files;
    return true;
}

bool collectBloomIndexFiles(const std::filesystem::path& scanRoot,
                            std::set<std::filesystem::path>& files,
                            std::string& error) {
    std::error_code ec;
    std::filesystem::recursive_directory_iterator iterator(scanRoot, ec);
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    const std::filesystem::recursive_directory_iterator end;
    for (; iterator != end; iterator.increment(ec)) {
        if (ec) {
            error = "could not enumerate checksum root " + scanRoot.string() +
                ": " + ec.message();
            return false;
        }
        const auto path = iterator->path();
        if (!hasBloomIndexSuffix(path)) continue;
        const auto status = iterator->symlink_status(ec);
        if (ec || status.type() != std::filesystem::file_type::regular) {
            error = "Bloom index is not a regular file: " + path.string();
            return false;
        }
        files.insert(std::filesystem::weakly_canonical(path, ec));
        if (ec) {
            error = "could not resolve Bloom index: " + path.string();
            return false;
        }
    }
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    return true;
}

bool collectGinIndexFiles(const std::filesystem::path& scanRoot,
                          std::set<std::filesystem::path>& files,
                          std::string& error) {
    std::error_code ec;
    std::filesystem::recursive_directory_iterator iterator(scanRoot, ec);
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    const std::filesystem::recursive_directory_iterator end;
    for (; iterator != end; iterator.increment(ec)) {
        if (ec) {
            error = "could not enumerate checksum root " + scanRoot.string() +
                ": " + ec.message();
            return false;
        }
        const auto path = iterator->path();
        if (!hasGinIndexSuffix(path)) continue;
        const auto status = iterator->symlink_status(ec);
        if (ec || status.type() != std::filesystem::file_type::regular) {
            error = "GIN index is not a regular file: " + path.string();
            return false;
        }
        files.insert(std::filesystem::weakly_canonical(path, ec));
        if (ec) {
            error = "could not resolve GIN index: " + path.string();
            return false;
        }
    }
    if (ec) {
        error = "could not enumerate checksum root " + scanRoot.string() +
            ": " + ec.message();
        return false;
    }
    return true;
}

bool verifyBloomIndex(const std::filesystem::path& root,
                      const std::filesystem::path& path,
                      BloomIndexVerificationStats& stats,
                      std::string& error) {
    using namespace bloom_index_format;
    const std::string shown = displayVerificationPath(root, path);
    ReadOnlyDescriptor data(path);
    if (data.get() < 0) {
        error = shown + ": could not open Bloom index read-only: " +
            std::strerror(errno);
        return false;
    }
    struct stat status {};
    if (::fstat(data.get(), &status) != 0 || !S_ISREG(status.st_mode) ||
        status.st_size < 0 ||
        static_cast<uint64_t>(status.st_size) < kHeaderBytes) {
        error = shown + ": truncated or invalid Bloom index file";
        return false;
    }

    std::array<char, kHeaderBytes> header{};
    if (!readExactlyAt(data.get(), header.data(), header.size(), 0)) {
        error = shown + ": short read of Bloom index header";
        return false;
    }
    const auto readU32 = [&](size_t offset) {
        uint32_t value = 0;
        for (unsigned i = 0; i < sizeof(value); ++i) {
            value |= static_cast<uint32_t>(
                static_cast<unsigned char>(header[offset + i])) << (8 * i);
        }
        return value;
    };
    const uint32_t magic = readU32(0);
    const uint32_t bits = readU32(sizeof(uint32_t));
    const uint32_t hashes = readU32(sizeof(uint32_t) * 2);
    const uint32_t count = readU32(sizeof(uint32_t) * 3);
    if ((magic != kLegacyMagic && magic != kChecksummedMagic) ||
        hashes == 0 || hashes > 32 || bits % 64 != 0 ||
        (count != 0 && bits == 0)) {
        error = shown + ": invalid Bloom index header";
        return false;
    }

    const uint64_t fileBytes = static_cast<uint64_t>(status.st_size);
    constexpr uint64_t kMinEntryBytes = sizeof(uint32_t) * 2 + sizeof(uint64_t);
    if (magic == kLegacyMagic) {
        if (count > (fileBytes - kHeaderBytes) / kMinEntryBytes) {
            error = shown + ": truncated legacy Bloom index";
            return false;
        }
        ++stats.uncheckedLegacyFiles;
        ++stats.files;
        return true;
    }

    if (fileBytes < kHeaderBytes + kChecksumBytes) {
        error = shown + ": truncated checksummed Bloom index";
        return false;
    }
    const uint64_t payloadBytes = fileBytes - kChecksumBytes;
    if (count > (payloadBytes - kHeaderBytes) / kMinEntryBytes) {
        error = shown + ": invalid checksummed Bloom index count";
        return false;
    }
    std::array<unsigned char, sizeof(uint32_t)> checksumBytes{};
    if (!readExactlyAt(data.get(), checksumBytes.data(), checksumBytes.size(),
                       static_cast<off_t>(payloadBytes))) {
        error = shown + ": short read of Bloom index checksum";
        return false;
    }
    // BLM2 uses the same little-endian encoding for the checksum trailer as
    // the rest of its on-disk fields.
    uint32_t storedChecksum = 0;
    for (unsigned i = 0; i < checksumBytes.size(); ++i) {
        storedChecksum |= static_cast<uint32_t>(checksumBytes[i]) << (8 * i);
    }

    uint64_t remaining = payloadBytes;
    off_t position = 0;
    uint32_t checksumState = 0xFFFFFFFFu;
    std::array<char, 64 * 1024> chunk{};
    while (remaining > 0) {
        const size_t amount = static_cast<size_t>(
            std::min<uint64_t>(remaining, chunk.size()));
        if (!readExactlyAt(data.get(), chunk.data(), amount, position)) {
            error = shown + ": short read while verifying Bloom index checksum";
            return false;
        }
        checksumState = index_checksum::crc32cUpdate(
            checksumState, chunk.data(), amount);
        position += static_cast<off_t>(amount);
        remaining -= amount;
    }
    if (index_checksum::crc32cFinish(checksumState) != storedChecksum) {
        error = shown + ": Bloom index checksum mismatch";
        return false;
    }
    ++stats.checksumFiles;
    ++stats.files;
    return true;
}

bool verifyGinIndex(const std::filesystem::path& root,
                    const std::filesystem::path& path,
                    GinIndexVerificationStats& stats,
                    std::string& error) {
    using namespace gin_index_format;
    const std::string shown = displayVerificationPath(root, path);
    ReadOnlyDescriptor data(path);
    if (data.get() < 0) {
        error = shown + ": could not open GIN index read-only: " +
            std::strerror(errno);
        return false;
    }
    struct stat status {};
    if (::fstat(data.get(), &status) != 0 || !S_ISREG(status.st_mode) ||
        status.st_size < 0) {
        error = shown + ": could not determine regular GIN index size";
        return false;
    }
    const uint64_t fileBytes = static_cast<uint64_t>(status.st_size);

    std::array<char, kMagic.size()> probe{};
    const size_t probeBytes = static_cast<size_t>(
        std::min<uint64_t>(fileBytes, probe.size()));
    if (probeBytes != 0 &&
        !readExactlyAt(data.get(), probe.data(), probeBytes, 0)) {
        error = shown + ": short read of GIN index signature";
        return false;
    }
    if (!containsBinaryMarker(probe.data(), probeBytes)) {
        // V1 is an unchecked text format; report it without claiming it was
        // checksum-verified.
        ++stats.uncheckedLegacyFiles;
        ++stats.files;
        return true;
    }

    if (probeBytes != kMagic.size() ||
        !std::equal(probe.begin(), probe.end(), kMagic.begin()) ||
        fileBytes < kHeaderBytes + kChecksumBytes) {
        error = shown + ": invalid or truncated GIN index signature";
        return false;
    }
    std::array<char, kHeaderBytes> header{};
    if (!readExactlyAt(data.get(), header.data(), header.size(), 0)) {
        error = shown + ": short read of GIN index header";
        return false;
    }
    const uint32_t version = decodeU32(header.data() + kMagic.size());
    const uint64_t entryCount =
        decodeU64(header.data() + kMagic.size() + sizeof(uint32_t));
    if (version != kVersion) {
        error = shown + ": unsupported GIN index version";
        return false;
    }

    const uint64_t payloadBytes = fileBytes - kChecksumBytes;
    if (entryCount > (payloadBytes - kHeaderBytes) / kMinimumEntryBytes) {
        error = shown + ": invalid checksummed GIN index entry count";
        return false;
    }
    std::array<char, kChecksumBytes> checksumBytes{};
    if (!readExactlyAt(data.get(), checksumBytes.data(), checksumBytes.size(),
                       static_cast<off_t>(payloadBytes))) {
        error = shown + ": short read of GIN index checksum";
        return false;
    }
    const uint32_t storedChecksum = decodeU32(checksumBytes.data());

    uint64_t remaining = payloadBytes;
    off_t position = 0;
    uint32_t checksumState = 0xFFFFFFFFu;
    std::array<char, 64 * 1024> chunk{};
    while (remaining > 0) {
        const size_t amount = static_cast<size_t>(
            std::min<uint64_t>(remaining, chunk.size()));
        if (!readExactlyAt(data.get(), chunk.data(), amount, position)) {
            error = shown + ": short read while verifying GIN index checksum";
            return false;
        }
        checksumState = index_checksum::crc32cUpdate(
            checksumState, chunk.data(), amount);
        position += static_cast<off_t>(amount);
        remaining -= amount;
    }
    if (index_checksum::crc32cFinish(checksumState) != storedChecksum) {
        error = shown + ": GIN index checksum mismatch";
        return false;
    }
    ++stats.checksumFiles;
    ++stats.files;
    return true;
}

bool verifyBTreeIndex(const std::filesystem::path& root,
                      const std::filesystem::path& path,
                      IndexVerificationStats& stats,
                      std::string& error) {
    using namespace bptree_format;
    const std::string shown = displayVerificationPath(root, path);
    ReadOnlyDescriptor data(path);
    if (data.get() < 0) {
        error = shown + ": could not open index read-only: " +
            std::strerror(errno);
        return false;
    }
    struct stat dataStatus {};
    if (::fstat(data.get(), &dataStatus) != 0 ||
        !S_ISREG(dataStatus.st_mode) || dataStatus.st_size < 0) {
        error = shown + ": could not determine regular index size";
        return false;
    }
    const uint64_t bytes = static_cast<uint64_t>(dataStatus.st_size);
    if (bytes < kPageSize || bytes % kPageSize != 0) {
        error = shown + ": truncated or misaligned B+ tree index file";
        return false;
    }
    const uint64_t pageCount = bytes / kPageSize;
    if (pageCount > UINT32_MAX) {
        error = shown + ": B+ tree index has too many pages";
        return false;
    }

    std::array<char, kPageSize> page{};
    if (!readExactlyAt(data.get(), page.data(), page.size(), 0)) {
        error = shown + ": block 0: short read";
        return false;
    }
    FileHeader header{};
    std::memcpy(&header, page.data(), sizeof(header));
    if (header.order < 2 || header.order > kMaxNodeOrder ||
        header.nextFreePage < 1 || header.nextFreePage > pageCount ||
        (header.rootPage != 0 &&
         (header.rootPage >= header.nextFreePage ||
          header.rootPage >= pageCount))) {
        error = shown + ": block 0: invalid B+ tree header";
        return false;
    }
    bool bindPageId = false;
    if (header.reserved == kPageBoundChecksumFormat) {
        bindPageId = true;
    } else if (header.reserved != kContentChecksumFormat &&
               header.reserved != 0) {
        error = shown + ": block 0: unknown B+ tree format marker";
        return false;
    }
    if (header.reserved == 0) {
        uint32_t legacyTrailer = 0;
        std::memcpy(&legacyTrailer, page.data() + kChecksumOffset,
                    sizeof(legacyTrailer));
        if (legacyTrailer != 0) {
            error = shown + ": block 0: unexpected legacy checksum trailer";
            return false;
        }
    } else if (!verifyPageChecksum(page.data(), 0, bindPageId)) {
        error = shown + ": block 0: B+ tree header checksum mismatch";
        return false;
    }

    const auto tdePath = std::filesystem::path(path.string() + ".tde");
    ReadOnlyDescriptor tde(tdePath);
    const bool hasTde = tde.get() >= 0;
    uint64_t tdeBytes = 0;
    if (hasTde) {
        struct stat tdeStatus {};
        if (::fstat(tde.get(), &tdeStatus) != 0 ||
            !S_ISREG(tdeStatus.st_mode) || tdeStatus.st_size < 0) {
            error = shown + ": invalid TDE sidecar";
            return false;
        }
        tdeBytes = static_cast<uint64_t>(tdeStatus.st_size);
        const uint64_t maximumTdeBytes = pageCount * PageCrypto::kRecordSize;
        if (tdeBytes % PageCrypto::kRecordSize != 0 ||
            tdeBytes > maximumTdeBytes) {
            error = shown + ": truncated or oversized TDE sidecar";
            return false;
        }
        if (tdeBytes >= PageCrypto::kRecordSize) {
            uint8_t headerRecord[PageCrypto::kRecordSize];
            if (!readExactlyAt(tde.get(), headerRecord,
                               sizeof(headerRecord), 0) ||
                !PageCrypto::isPlaintextRecord(headerRecord)) {
                error = shown + ": block 0: invalid TDE header-page envelope";
                return false;
            }
        }
    } else if (errno != ENOENT) {
        error = shown + ": could not open TDE sidecar read-only: " +
            std::strerror(errno);
        return false;
    }

    for (uint32_t block = 1; block < pageCount; ++block) {
        if (!readExactlyAt(data.get(), page.data(), page.size(),
                           static_cast<off_t>(block) * kPageSize)) {
            error = shown + ": block " + std::to_string(block) +
                ": short read";
            return false;
        }
        if (hasTde) {
            uint8_t record[PageCrypto::kRecordSize];
            PageCrypto::clearRecord(record);
            const off_t recordOffset =
                static_cast<off_t>(block) * PageCrypto::kRecordSize;
            if (static_cast<uint64_t>(recordOffset) < tdeBytes &&
                !readExactlyAt(tde.get(), record, sizeof(record), recordOffset)) {
                error = shown + ": block " + std::to_string(block) +
                    ": truncated TDE envelope";
                return false;
            }
            if (!PageCrypto::isPlaintextRecord(record)) {
                if (!PageCrypto::enabled()) {
                    error = shown + ": block " + std::to_string(block) +
                        ": encrypted page has no configured read-only key";
                    return false;
                }
                if (!PageCrypto::openPage(block, page.data(), page.size(),
                                          record)) {
                    error = shown + ": block " + std::to_string(block) +
                        ": TDE envelope authentication failed";
                    return false;
                }
            }
        }

        if (header.reserved == 0) {
            ++stats.uncheckedPages;
        } else if (!verifyPageChecksum(page.data(), block, bindPageId)) {
            error = shown + ": block " + std::to_string(block) +
                ": B+ tree page checksum mismatch";
            return false;
        } else if (bindPageId) {
            ++stats.pageBoundPages;
        } else {
            ++stats.contentOnlyPages;
        }
    }

    ++stats.files;
    ++stats.pages;  // Header page.
    if (header.reserved == 0) {
        ++stats.uncheckedPages;
    } else if (bindPageId) {
        ++stats.pageBoundPages;
    } else {
        ++stats.contentOnlyPages;
    }
    stats.pages += pageCount - 1;
    return true;
}

bool verifyHeapFile(const std::filesystem::path& root,
                    const std::filesystem::path& path,
                    HeapVerificationStats& stats,
                    std::string& error) {
    const std::string shown = displayVerificationPath(root, path);
    std::error_code ec;
    const auto pendingMarker = path.string() + ".extent_pending";
    if (std::filesystem::exists(pendingMarker, ec)) {
        error = shown + ": pending heap extent publication requires recovery";
        return false;
    }
    if (ec) {
        error = shown + ": could not inspect extent marker: " + ec.message();
        return false;
    }

    ReadOnlyDescriptor data(path);
    if (data.get() < 0) {
        error = shown + ": could not open relation read-only: " +
            std::strerror(errno);
        return false;
    }
    struct stat dataStatus {};
    if (::fstat(data.get(), &dataStatus) != 0 ||
        !S_ISREG(dataStatus.st_mode) || dataStatus.st_size < 0) {
        error = shown + ": could not determine regular relation size";
        return false;
    }
    const uint64_t bytes = static_cast<uint64_t>(dataStatus.st_size);
    if (bytes < PgPage::PAGE_SIZE || bytes % PgPage::PAGE_SIZE != 0) {
        error = shown + ": truncated or misaligned relation file";
        return false;
    }

    std::array<char, PgPage::PAGE_SIZE> page{};
    if (!readExactlyAt(data.get(), page.data(), page.size(), 0)) {
        error = shown + ": block 0: short read";
        return false;
    }
    DataFileHeader header{};
    std::memcpy(&header, page.data(), sizeof(header));
    if (header.magic != DATA_FILE_MAGIC ||
        header.formatVersion != DATA_FILE_FORMAT_VERSION ||
        header.numPages == 0 || header.freeListHead >= header.numPages ||
        header.headerChecksum != computeDataFileHeaderChecksum(header)) {
        error = shown + ": block 0: invalid heap file header/checksum";
        return false;
    }
    const uint64_t declaredBytes =
        static_cast<uint64_t>(header.numPages) * PgPage::PAGE_SIZE;
    if (declaredBytes != bytes) {
        error = shown + ": block 0: declared page count does not match file size";
        return false;
    }

    const auto tdePath = std::filesystem::path(path.string() + ".tde");
    int tdeFd = -1;
    uint64_t tdeBytes = 0;
    ReadOnlyDescriptor tde(tdePath);
    if (tde.get() >= 0) {
        tdeFd = tde.get();
        struct stat tdeStatus {};
        if (::fstat(tdeFd, &tdeStatus) != 0 || !S_ISREG(tdeStatus.st_mode) ||
            tdeStatus.st_size < 0) {
            error = shown + ": invalid TDE sidecar";
            return false;
        }
        tdeBytes = static_cast<uint64_t>(tdeStatus.st_size);
        const uint64_t maximumTdeBytes =
            static_cast<uint64_t>(header.numPages) * PageCrypto::kRecordSize;
        if (tdeBytes % PageCrypto::kRecordSize != 0 ||
            tdeBytes > maximumTdeBytes) {
            error = shown + ": truncated or oversized TDE sidecar";
            return false;
        }
        if (tdeBytes >= PageCrypto::kRecordSize) {
            uint8_t headerRecord[PageCrypto::kRecordSize];
            if (!readExactlyAt(tdeFd, headerRecord, sizeof(headerRecord), 0) ||
                !PageCrypto::isPlaintextRecord(headerRecord)) {
                error = shown + ": block 0: invalid TDE header-page envelope";
                return false;
            }
        }
    } else if (errno != ENOENT) {
        error = shown + ": could not open TDE sidecar read-only: " +
            std::strerror(errno);
        return false;
    }

    for (uint32_t block = 1; block < header.numPages; ++block) {
        if (!readExactlyAt(
                data.get(), page.data(), page.size(),
                static_cast<off_t>(block) * PgPage::PAGE_SIZE)) {
            error = shown + ": block " + std::to_string(block) +
                ": short read";
            return false;
        }
        if (tdeFd >= 0) {
            uint8_t record[PageCrypto::kRecordSize];
            PageCrypto::clearRecord(record);
            const off_t recordOffset =
                static_cast<off_t>(block) * PageCrypto::kRecordSize;
            if (static_cast<uint64_t>(recordOffset) < tdeBytes &&
                !readExactlyAt(tdeFd, record, sizeof(record), recordOffset)) {
                error = shown + ": block " + std::to_string(block) +
                    ": truncated TDE envelope";
                return false;
            }
            if (!PageCrypto::isPlaintextRecord(record)) {
                if (!PageCrypto::enabled()) {
                    error = shown + ": block " + std::to_string(block) +
                        ": encrypted page has no configured read-only key";
                    return false;
                }
                if (!PageCrypto::openPage(
                        block, page.data(), page.size(), record)) {
                    error = shown + ": block " + std::to_string(block) +
                        ": TDE envelope authentication failed";
                    return false;
                }
            }
        }
        const PgPage verifiedPage(page.data());
        if (!verifiedPage.isValid() ||
            (verifiedPage.hasBoundPageId() &&
             !verifiedPage.isValid(block))) {
            error = shown + ": block " + std::to_string(block) +
                ": invalid page checksum or layout";
            return false;
        }
        if (verifiedPage.hasBoundPageId()) {
            ++stats.identityBoundBlocks;
        } else {
            ++stats.legacyIdentityUnboundBlocks;
        }
    }

    ++stats.files;
    stats.blocks += header.numPages;
    return true;
}

bool verifyHeapDataChecksums(const std::filesystem::path& root,
                             std::string& output,
                             std::string& error) {
    if (!loadVerificationKey(root, error)) return false;

    std::error_code ec;
    std::set<std::filesystem::path> scanRoots;
    scanRoots.insert(std::filesystem::weakly_canonical(root, ec));
    if (ec) {
        error = "could not resolve data directory: " + ec.message();
        return false;
    }
    if (!readTablespaceRoots(root, scanRoots, error)) return false;

    std::set<std::filesystem::path> heapFiles;
    std::set<std::filesystem::path> btreeFiles;
    std::set<std::filesystem::path> hashIndexFiles;
    std::set<std::filesystem::path> bloomIndexFiles;
    std::set<std::filesystem::path> ginIndexFiles;
    for (const auto& scanRoot : scanRoots) {
        if (!collectHeapFiles(scanRoot, heapFiles, error)) return false;
        if (!collectBTreeFiles(scanRoot, btreeFiles, error)) return false;
        if (!collectHashIndexFiles(scanRoot, hashIndexFiles, error)) return false;
        if (!collectBloomIndexFiles(scanRoot, bloomIndexFiles, error)) return false;
        if (!collectGinIndexFiles(scanRoot, ginIndexFiles, error)) return false;
    }

    HeapVerificationStats stats;
    for (const auto& path : heapFiles) {
        if (!verifyHeapFile(root, path, stats, error)) return false;
    }
    IndexVerificationStats indexStats;
    for (const auto& path : btreeFiles) {
        if (!verifyBTreeIndex(root, path, indexStats, error)) return false;
    }
    HashIndexVerificationStats hashStats;
    for (const auto& path : hashIndexFiles) {
        if (!verifyHashIndex(root, path, hashStats, error)) return false;
    }
    BloomIndexVerificationStats bloomStats;
    for (const auto& path : bloomIndexFiles) {
        if (!verifyBloomIndex(root, path, bloomStats, error)) return false;
    }
    GinIndexVerificationStats ginStats;
    for (const auto& path : ginIndexFiles) {
        if (!verifyGinIndex(root, path, ginStats, error)) return false;
    }
    output = "heap checksum verification passed\n"
        "B+ tree index scan completed; hash index scan completed; Bloom index scan completed; GIN index scan completed (legacy unchecked data is reported)\nfiles=" +
        std::to_string(stats.files) + "\nblocks=" +
        std::to_string(stats.blocks) + "\nidentity-bound-blocks=" +
        std::to_string(stats.identityBoundBlocks) +
        "\nlegacy-identity-unbound-blocks=" +
        std::to_string(stats.legacyIdentityUnboundBlocks) +
        "\nindex-files=" + std::to_string(indexStats.files) +
        "\nindex-pages=" + std::to_string(indexStats.pages) +
        "\npage-bound-index-pages=" +
        std::to_string(indexStats.pageBoundPages) +
        "\ncontent-only-index-pages=" +
        std::to_string(indexStats.contentOnlyPages) +
        "\nunchecked-index-pages=" +
        std::to_string(indexStats.uncheckedPages) +
        "\nhash-index-files=" + std::to_string(hashStats.files) +
        "\nchecksummed-hash-index-files=" +
        std::to_string(hashStats.checksumFiles) +
        "\nunchecked-legacy-hash-index-files=" +
        std::to_string(hashStats.uncheckedLegacyFiles) +
        "\nbloom-index-files=" + std::to_string(bloomStats.files) +
        "\nchecksummed-bloom-index-files=" +
        std::to_string(bloomStats.checksumFiles) +
        "\nunchecked-legacy-bloom-index-files=" +
        std::to_string(bloomStats.uncheckedLegacyFiles) +
        "\ngin-index-files=" + std::to_string(ginStats.files) +
        "\nchecksummed-gin-index-files=" +
        std::to_string(ginStats.checksumFiles) +
        "\nunchecked-legacy-gin-index-files=" +
        std::to_string(ginStats.uncheckedLegacyFiles);
    return true;
}

}  // namespace

bool processArgumentsRequestVersion() {
    const auto arguments = processArguments();
    for (size_t i = 1; i < arguments.size(); ++i) {
        if (arguments[i] == "--version" || arguments[i] == "-V") return true;
    }
    return false;
}

DataDirectoryUtility requestedDataDirectoryUtility() {
    const auto arguments = processArguments();
    DataDirectoryUtility requested = DataDirectoryUtility::None;
    for (size_t i = 1; i < arguments.size(); ++i) {
        DataDirectoryUtility candidate = DataDirectoryUtility::None;
        if (arguments[i] == "--check-data-directory") {
            candidate = DataDirectoryUtility::Check;
        } else if (arguments[i] == "--upgrade-data-directory") {
            candidate = DataDirectoryUtility::Upgrade;
        } else if (arguments[i] == "--verify-data-checksums") {
            candidate = DataDirectoryUtility::VerifyChecksums;
        }
        if (candidate == DataDirectoryUtility::None) continue;
        if (requested != DataDirectoryUtility::None && requested != candidate) {
            return DataDirectoryUtility::Invalid;
        }
        requested = candidate;
    }
    return requested;
}

bool runDataDirectoryUtility(DataDirectoryUtility utility,
                             std::string& output,
                             std::string& error) {
    if (utility == DataDirectoryUtility::None) {
        error = "no data-directory utility selected";
        return false;
    }
    if (utility == DataDirectoryUtility::Invalid) {
        error = "conflicting data-directory utility arguments";
        return false;
    }
    std::filesystem::path selected;
    if (!parseSelectedDirectory(selected, error)) return false;

    std::error_code filesystemError;
    if (!std::filesystem::is_directory(selected, filesystemError) ||
        filesystemError) {
        error = "data directory does not exist or is not a directory";
        return false;
    }
    DirectoryLock offlineLock;
    if (utility != DataDirectoryUtility::Check &&
        !offlineLock.acquire(selected, error)) return false;
    const auto control = selected / "DBMS_CONTROL";
    if (!std::filesystem::is_regular_file(control, filesystemError) ||
        filesystemError) {
        error = "DBMS_CONTROL is missing or is not a regular file";
        return false;
    }

    std::string identifier;
    ControlFileVersion version = ControlFileVersion::Current;
    if (!loadControlFile(control, identifier, version, error)) return false;

    if (utility == DataDirectoryUtility::VerifyChecksums) {
        if (version != ControlFileVersion::Current) {
            error = controlVersionUpgradeRequired(version);
            return false;
        }
        return verifyHeapDataChecksums(selected, output, error);
    }

    if (utility == DataDirectoryUtility::Check) {
        if (version != ControlFileVersion::Current) {
            error = controlVersionUpgradeRequired(version);
            return false;
        }
        output = "data directory is compatible\n" +
            currentControlContents(identifier);
        return true;
    }

    if (version == ControlFileVersion::Current) {
        output = "data directory is already at control format version 3";
        return true;
    }
    const char* oldVersion = version == ControlFileVersion::LegacyV1 ? "1" : "2";
    if (!index_file::writeAtomically(control, currentControlContents(identifier))) {
        error = "could not durably upgrade DBMS_CONTROL";
        return false;
    }
    output = "upgraded DBMS_CONTROL from version " +
             std::string(oldVersion) + " to version 3; "
             "system identifier preserved";
    return true;
}

bool bootstrapDataDirectory(std::string& error) {
    auto& bootstrap = state();
    if (bootstrap.ready) return true;
    std::filesystem::path selected;
    if (!parseSelectedDirectory(selected, error)) return false;

    std::error_code filesystemError;
    if (!std::filesystem::exists(selected, filesystemError) || filesystemError ||
        !std::filesystem::is_directory(selected, filesystemError) ||
        filesystemError) {
        error = "data directory does not exist or is not a directory";
        return false;
    }
    DirectoryLock instanceLock;
    if (!instanceLock.acquire(selected, error)) return false;
    if (!initializeControlFile(
            selected, bootstrap.systemIdentifier, error)) return false;
    std::filesystem::current_path(selected, filesystemError);
    if (filesystemError) {
        error = "could not enter data directory: " + filesystemError.message();
        return false;
    }
    bootstrap.root = selected;
    bootstrap.lockFd = instanceLock.release();
    bootstrap.ready = true;
    return true;
}

const std::filesystem::path& dataDirectory() {
    return state().root;
}

const std::string& clusterSystemIdentifier() {
    return state().systemIdentifier;
}

}  // namespace dbms
