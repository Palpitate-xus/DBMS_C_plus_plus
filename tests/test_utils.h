#pragma once

#include <filesystem>
#include <string>

// All test databases use the prefix "__t_" and are created in the CWD.
// cleanupAllTestData removes all __t_* directories.
// This ensures DDL parsing of "CREATE DATABASE __t_xxx" works correctly
// (no path separator issues) and the project root stays clean after tests.
inline std::string testDbPath(const std::string& name) {
    return "__t_" + name;
}

// Cleanup a specific test database.
inline void cleanupTestDb(const std::string& name) {
    std::string path = testDbPath(name);
    if (std::filesystem::exists(path))
        std::filesystem::remove_all(path);
}

/** Clear ALL test data directories. Call at the start of each test's main(). */
inline void cleanupAllTestData() {
    if (!std::filesystem::exists(".")) return;
    for (const auto& entry : std::filesystem::directory_iterator(".")) {
        try {
            if (!entry.is_directory()) continue;
            std::string n = entry.path().filename().string();
            if (n.substr(0, 4) == "__t_" ||
                n.find(".txn_backup") != std::string::npos ||
                n.find(".archive") != std::string::npos) {
                std::filesystem::remove_all(entry.path());
            }
        } catch (...) {}
    }
}

/** Final cleanup — call before program exit. */
inline void finalCleanupTestData() {
    cleanupAllTestData();
}
