#pragma once
#include <filesystem>
#include <string>

// All test databases are prefixed with "__t_" to clearly mark them as test data.
// They are created in the project root engine directory (no subdirectory) so the
// StorageEngine's dbPath() resolves them correctly.
// The build/ directory and project root can be cleaned by removing all "__t_*" dirs.
inline std::string testDbPath(const std::string& name) {
    return "__t_" + name;
}

// Cleanup a test database directory.
inline void cleanupTestDb(const std::string& name) {
    std::string path = testDbPath(name);
    if (std::filesystem::exists(path))
        std::filesystem::remove_all(path);
}
