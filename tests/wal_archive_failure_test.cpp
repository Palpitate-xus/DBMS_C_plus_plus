// WAL archive failure regression: publishing an archive or its .done marker
// must be atomic and retryable. A failed archive must leave the source WAL and
// .ready state intact, never claiming that truncation is safe.

#include "WAL.h"

#include <unistd.h>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

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

    // An archive from another database generation must never be replaced by
    // a same-named segment. Keep the ready marker so the conflict is visible
    // and retryable after an operator resolves the destination.
    fs::create_directory(archiveDir);
    const fs::path archivedSegment =
        archiveDir / wal.segmentPath(0).filename();
    {
        std::ofstream unrelated(archivedSegment, std::ios::binary);
        unrelated << "unrelated WAL generation";
    }
    // A crash after hard-link publication can leave this old temporary name
    // pointing at the archive inode. Reusing it with O_TRUNC would modify the
    // destination before any no-overwrite check runs.
    const fs::path staleTemp = archivedSegment.string() + ".tmp." +
                               std::to_string(::getpid());
    fs::create_hard_link(archivedSegment, staleTemp);
    assert(!wal.archivePendingSegments(archiveDir));
    {
        std::ifstream preserved(archivedSegment, std::ios::binary);
        std::string contents((std::istreambuf_iterator<char>(preserved)),
                             std::istreambuf_iterator<char>());
        assert(contents == "unrelated WAL generation");
    }
    assert(!wal.isSegmentArchived(0));
    assert(!wal.pendingArchiveSegments().empty());
    fs::remove_all(archiveDir);

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

    // A prior copy may have succeeded just before marker publication failed.
    // Retrying an identical archived segment must finish successfully.
    assert(wal.markSegmentReadyForArchive(0));
    assert(wal.archivePendingSegments(archiveDir));
    assert(wal.isSegmentArchived(0));
    assert(wal.pendingArchiveSegments().empty());

    fs::remove_all(walDir);
    std::cout << "[WAL ARCHIVE FAILURE] atomic retry semantics OK\n";
    return 0;
}
