#pragma once

#include <filesystem>
#include <functional>

namespace dbms {

// Cross-process coordination and crash-safe publication shared by the
// versioned statistics snapshots. The caller must hold the lock while reading
// the existing snapshot and publishing its replacement.
class StatsFileLock {
public:
    explicit StatsFileLock(const std::filesystem::path& snapshotPath);
    ~StatsFileLock();

    StatsFileLock(const StatsFileLock&) = delete;
    StatsFileLock& operator=(const StatsFileLock&) = delete;

    bool ok() const;

private:
    int fd_ = -1;
};

bool syncStatsFile(const std::filesystem::path& path);
bool syncStatsDirectory(const std::filesystem::path& path);

// The writer receives a unique temporary path and must create/truncate and
// fully write it. This helper fsyncs the file, atomically renames it over the
// destination, fsyncs the parent directory, and removes failed temporaries.
bool publishStatsSnapshot(
    const std::filesystem::path& destination,
    const std::function<bool(const std::filesystem::path&)>& writer);

} // namespace dbms
