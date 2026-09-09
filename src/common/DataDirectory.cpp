#include "DataDirectory.h"

#include "access/IndexFileUtil.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <sstream>
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

bool loadControlFile(const std::filesystem::path& path,
                     std::string& systemIdentifier, std::string& error) {
    std::ifstream input(path);
    if (!input) {
        error = "could not open DBMS_CONTROL";
        return false;
    }
    std::string header;
    std::string format;
    std::string identifier;
    std::string trailing;
    if (!std::getline(input, header) ||
        !std::getline(input, format) ||
        !std::getline(input, identifier) ||
        std::getline(input, trailing) ||
        header != "DBMS_CPP_CLUSTER_CONTROL_V1" ||
        format != "format_version=1" ||
        identifier.rfind("system_identifier=", 0) != 0) {
        error = "invalid DBMS_CONTROL format or magic";
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
        return loadControlFile(control, systemIdentifier, error);
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
    const std::string contents =
        "DBMS_CPP_CLUSTER_CONTROL_V1\nformat_version=1\n"
        "system_identifier=" + systemIdentifier + "\n";
    if (!index_file::writeAtomically(control, contents)) {
        error = "could not create durable DBMS_CONTROL";
        return false;
    }
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
