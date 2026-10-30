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

// Rename publishes the new image before directory fsync establishes its
// durability. Delta-based callers must acknowledge a published image in their
// prepared memory state even when the final durability barrier fails.
enum class StatsSnapshotPublication {
    NotPublished,
    PublishedNotDurable,
    Durable
};

StatsSnapshotPublication publishStatsSnapshotWithStatus(
    const std::filesystem::path& destination,
    const std::function<bool(const std::filesystem::path&)>& writer);

// The caller holds StatsFileLock (or equivalent exclusive coordination).
// The per-destination/process temporary is fully written and synced before
// rename; failed or throwing writers are cleaned up. The legacy bool result
// means Durable, not necessarily that a false result left the name unchanged.
// The writer must create/truncate and
// fully write it. This helper fsyncs the file, atomically renames it over the
// destination, fsyncs the parent directory, and removes failed temporaries.
bool publishStatsSnapshot(
    const std::filesystem::path& destination,
    const std::function<bool(const std::filesystem::path&)>& writer);

} // namespace dbms
