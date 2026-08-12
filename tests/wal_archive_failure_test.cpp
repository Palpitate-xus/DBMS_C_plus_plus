// WAL archive failure regression: publishing an archive or its .done marker
// must be atomic and retryable. A failed archive must leave the source WAL and
// .ready state intact, never claiming that truncation is safe.

#include "WAL.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

using namespace dbms;
namespace fs = std::filesystem;

int main() {
    const fs::path walDir = "wal_archive_failure_test_dir";
    const fs::path archiveDir = walDir / "archive";
    fs::remove_all(walDir);

    WALManager wal(walDir / "pg_wal");
    assert(wal.ensureOpen());
    const Lsn lsn = wal.XLogInsert(RM_HEAP_ID, XLOG_HEAP_INSERT, 1, {'x'});
    assert(lsn != INVALID_LSN);
    assert(wal.XLogFlush(lsn));
    assert(wal.markSegmentReadyForArchive(0));

    // A regular file cannot be used as an archive directory. The failure
    // must not remove the source segment or publish archive completion.
    {
        std::ofstream blocker(archiveDir);
        assert(blocker.good());
    }
    assert(!wal.archivePendingSegments(archiveDir));
    assert(fs::exists(wal.segmentPath(0)));
    assert(!wal.isSegmentArchived(0));
    assert(!wal.pendingArchiveSegments().empty());

    fs::remove(archiveDir);
    assert(wal.archivePendingSegments(archiveDir));
    assert(wal.isSegmentArchived(0));
    assert(fs::exists(archiveDir / wal.segmentPath(0).filename()));
    assert(wal.pendingArchiveSegments().empty());

    fs::remove_all(walDir);
    std::cout << "[WAL ARCHIVE FAILURE] atomic retry semantics OK\n";
    return 0;
}
