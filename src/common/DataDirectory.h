#pragma once

#include <filesystem>
#include <string>

namespace dbms {

// Resolve -D/--data-dir (or DBMS_DATA_DIR), validate/create the DBMS cluster
// identity, and chdir before any process-global storage/config object exists.
bool bootstrapDataDirectory(std::string& error);

// --version must remain a side-effect-free operation even though the legacy
// executable owns process-global storage objects.
bool processArgumentsRequestVersion();

const std::filesystem::path& dataDirectory();
const std::string& clusterSystemIdentifier();

}  // namespace dbms
