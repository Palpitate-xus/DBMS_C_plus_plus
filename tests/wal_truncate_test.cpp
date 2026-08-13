// WAL truncation test: only archived segments before the retention LSN may
// be removed, and the first retained segment remains discoverable.

#include "WAL.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace dbms;

int main() {
    const std::filesystem::path walDir = "wal_truncate_test_dir";
    const std::filesystem::path archiveDir = walDir / "archive";
    std::filesystem::remove_all(walDir);

    WALManager wal(walDir);
    assert(wal.ensureOpen());
    // Build two complete valid segments.  Empty segment files are not valid
    // WAL and must be rejected by the startup integrity scan.
    const std::vector<char> payload(WALManager::kSegmentSize - sizeof(XLogRecHeader), 'x');
    assert(wal.XLogInsert(RM_CHECKPOINT_ID, XLOG_CHECKPOINT_ONLINE, 1, payload) != INVALID_LSN);
    assert(wal.XLogInsert(RM_CHECKPOINT_ID, XLOG_CHECKPOINT_ONLINE, 1, {'y'}) != INVALID_LSN);
    assert(wal.XLogFlush(wal.currentWriteLsn()));
    const auto segment0 = wal.segmentPath(0);
    const auto segment1 = wal.segmentPath(1);
    assert(std::filesystem::exists(segment0));
    assert(std::filesystem::exists(segment1));
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
