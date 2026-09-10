#pragma once

#include <filesystem>
#include <string>

namespace dbms {

enum class DataDirectoryUtility {
    None,
    Check,
    Upgrade
};

// Resolve -D/--data-dir (or DBMS_DATA_DIR), validate/create the DBMS cluster
// identity, and chdir before any process-global storage/config object exists.
bool bootstrapDataDirectory(std::string& error);

// --version must remain a side-effect-free operation even though the legacy
// executable owns process-global storage objects.
bool processArgumentsRequestVersion();

// Offline control-file utilities are handled before process-global storage
// objects are constructed.  Check is strictly read-only; Upgrade only accepts
// the one explicitly supported predecessor control format.
DataDirectoryUtility requestedDataDirectoryUtility();
bool runDataDirectoryUtility(DataDirectoryUtility utility,
                             std::string& output,
                             std::string& error);

const std::filesystem::path& dataDirectory();
const std::string& clusterSystemIdentifier();

}  // namespace dbms
