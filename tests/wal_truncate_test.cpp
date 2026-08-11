// WAL truncation test: only archived segments before the retention LSN may
// be removed, and the first retained segment remains discoverable.

#include "WAL.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

using namespace dbms;

int main() {
    const std::filesystem::path walDir = "wal_truncate_test_dir";
    const std::filesystem::path archiveDir = walDir / "archive";
    std::filesystem::remove_all(walDir);

    WALManager wal(walDir);
    const auto segment0 = wal.segmentPath(0);
    const auto segment1 = wal.segmentPath(1);
    std::filesystem::create_directories(walDir);
    {
        std::ofstream(segment0).close();
        std::ofstream(segment1).close();
        std::filesystem::resize_file(segment0, WALManager::kSegmentSize);
        std::filesystem::resize_file(segment1, WALManager::kSegmentSize);
    }

    assert(wal.ensureOpen());
    assert(wal.earliestAvailableLsn() == 0);
    assert(wal.markSegmentReadyForArchive(0));
    assert(!wal.truncateBefore(WALManager::kSegmentSize));
    assert(std::filesystem::exists(segment0));

    assert(wal.archiveSegment(0, archiveDir));
    assert(wal.truncateBefore(WALManager::kSegmentSize));
    assert(!std::filesystem::exists(segment0));
    assert(std::filesystem::exists(segment1));
    assert(wal.earliestAvailableLsn() == WALManager::kSegmentSize);

    // A second checkpoint must not recreate ready markers for already
    // truncated segment numbers.
    assert(wal.markSegmentsReadyBefore(WALManager::kSegmentSize));
    assert(wal.pendingArchiveSegments().empty());

    std::filesystem::remove_all(walDir);
    std::cout << "[WAL TRUNCATE] all passed\n";
    return 0;
}
