#include "TableManage.h"
#include "WAL.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>

int main() {
    namespace fs = std::filesystem;
    const std::string source = testDbPath("archive_src");
    const std::string occupied = source + ".archive";
    fs::remove_all(source);
    fs::remove_all(occupied);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(source) == dbms::DBStatus::OK);
    assert(engine.createDatabase(occupied) == dbms::DBStatus::OK);
    std::ofstream(occupied + "/keep.txt") << "another database";

    dbms::WALManager* wal = engine.getWAL(source);
    assert(wal != nullptr);
    const dbms::Lsn lsn = wal->XLogInsert(
        dbms::RM_HEAP_ID, dbms::XLOG_HEAP_INSERT, 1, {'x'});
    assert(lsn != dbms::INVALID_LSN);
    assert(wal->XLogFlush(lsn));
    assert(wal->markSegmentReadyForArchive(0));
    const fs::path foreignSegment =
        fs::path(occupied) / wal->segmentPath(0).filename();

    // The archive sidecar path can also be a legal quoted database name.
    // Archiving must fail without writing WAL files into that database.
    assert(!engine.archiveWal(source));
    assert(engine.databaseExists(occupied));
    assert(fs::exists(occupied + "/keep.txt"));
    assert(!fs::exists(foreignSegment));
    assert(!wal->isSegmentArchived(0));

    assert(engine.dropDatabase(occupied) == dbms::DBStatus::OK);
    assert(engine.dropDatabase(source) == dbms::DBStatus::OK);
}
