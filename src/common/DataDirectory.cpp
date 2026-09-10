#include "DataDirectory.h"

#include "access/IndexFileUtil.h"
#include "common/Config.h"
#include "storage/DataFileHeader.h"
#include "storage/PageCrypto.h"
#include "storage/PgPage.h"
#include "interfaces/dbms_defs.h"

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
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace dbms {
namespace {

struct BootstrapState {
    std::filesystem::path root;
    std::string systemIdentifier;
    bool ready = false;
};

constexpr const char* kCurrentControlMagic =
    "DBMS_CPP_CLUSTER_CONTROL_V2";
constexpr const char* kLegacyControlMagic =
    "DBMS_CPP_CLUSTER_CONTROL_V1";
constexpr uint32_t kControlFormatVersion = 2;
constexpr uint32_t kCatalogFormatVersion = 1;

const char* nativeByteOrder() {
    const uint16_t marker = 1;
    return *reinterpret_cast<const unsigned char*>(&marker) == 1
        ? "little" : "big";
}

std::string currentControlContents(const std::string& systemIdentifier) {
    return std::string(kCurrentControlMagic) + "\n" +
        "control_format_version=" +
        std::to_string(kControlFormatVersion) + "\n" +
        "catalog_format_version=" +
        std::to_string(kCatalogFormatVersion) + "\n" +
        "heap_format_version=" +
        std::to_string(DATA_FILE_FORMAT_VERSION) + "\n" +
        "block_size=" + std::to_string(BLCKSZ) + "\n" +
        "byte_order=" + nativeByteOrder() + "\n" +
        "system_identifier=" + systemIdentifier + "\n";
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
    LegacyV1
};

bool loadControlFile(const std::filesystem::path& path,
                     std::string& systemIdentifier,
                     ControlFileVersion& version,
                     std::string& error) {
    std::ifstream input(path);
    if (!input) {
        error = "could not open DBMS_CONTROL";
        return false;
    }
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
    } else if (header == kCurrentControlMagic) {
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
            error = "DBMS_CONTROL version 1 requires offline upgrade; run "
                    "dbms_main -D <data-directory> --upgrade-data-directory";
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

struct HeapVerificationStats {
    uint64_t files = 0;
    uint64_t blocks = 0;
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
            std::ifstream marker(markers->path());
            std::string target;
            if (!marker || !std::getline(marker, target) || target.empty()) {
                error = "invalid tablespace marker: " +
                    markers->path().string();
                return false;
            }
            std::filesystem::path tablespaceRoot(target);
            if (tablespaceRoot.is_relative()) tablespaceRoot = root / tablespaceRoot;
            tablespaceRoot = tablespaceRoot.lexically_normal();
            if (!std::filesystem::is_directory(tablespaceRoot, ec) || ec) {
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
            if (!std::filesystem::is_directory(databaseRoot, ec) || ec) {
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
        if (!PgPage(page.data()).isValid()) {
            error = shown + ": block " + std::to_string(block) +
                ": invalid page checksum or layout";
            return false;
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
    for (const auto& scanRoot : scanRoots) {
        if (!collectHeapFiles(scanRoot, heapFiles, error)) return false;
    }

    HeapVerificationStats stats;
    for (const auto& path : heapFiles) {
        if (!verifyHeapFile(root, path, stats, error)) return false;
    }
    output = "heap checksum verification passed\nfiles=" +
        std::to_string(stats.files) + "\nblocks=" +
        std::to_string(stats.blocks);
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
            error = "data directory uses control version 1; offline upgrade is required";
            return false;
        }
        return verifyHeapDataChecksums(selected, output, error);
    }

    if (utility == DataDirectoryUtility::Check) {
        if (version != ControlFileVersion::Current) {
            error = "data directory uses control version 1; offline upgrade is required";
            return false;
        }
        output = "data directory is compatible\n" +
            std::string("control_format_version=2\n") +
            "catalog_format_version=1\nheap_format_version=2\n" +
            "block_size=8192\nbyte_order=" + nativeByteOrder() + "\n" +
            "system_identifier=" + identifier;
        return true;
    }

    if (version == ControlFileVersion::Current) {
        output = "data directory is already at control format version 2";
        return true;
    }
    if (!index_file::writeAtomically(control, currentControlContents(identifier))) {
        error = "could not durably upgrade DBMS_CONTROL";
        return false;
    }
    output = "upgraded DBMS_CONTROL from version 1 to version 2; "
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
    if (!initializeControlFile(
            selected, bootstrap.systemIdentifier, error)) return false;
    std::filesystem::current_path(selected, filesystemError);
    if (filesystemError) {
        error = "could not enter data directory: " + filesystemError.message();
        return false;
    }
    bootstrap.root = selected;
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
