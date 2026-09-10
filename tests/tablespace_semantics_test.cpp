#include "access/IndexFileUtil.h"
#include "commands/TableManage.h"
#include "interfaces/table_schema.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using dbms::DBStatus;

static std::string readFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

static dbms::TableSchema simpleTable(const std::string& name,
                                     const std::string& tablespace) {
    dbms::TableSchema table;
    table.tablename = name;
    table.tablespace = tablespace;
    table.append(dbms::makeIntColumn("id", false, 2));
    return table;
}

int main() {
    cleanupAllTestData();
    const std::string database = testDbPath("tablespace_semantics");
    const fs::path location =
        fs::absolute(testDbPath("tablespace_semantics_location"));
    const fs::path alternate =
        fs::absolute(testDbPath("tablespace_semantics_alternate"));
    const fs::path failed =
        fs::absolute(testDbPath("tablespace_semantics_failed"));
    fs::remove_all(location);
    fs::remove_all(alternate);
    fs::remove_all(failed);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == DBStatus::OK);

    // Locations are canonicalized and published only after the per-database
    // relation directory is durable. A second name may not alias the same
    // physical root.
    assert(engine.createTablespace(database, "fast_space",
                                   location.string()) == DBStatus::OK);
    const fs::path marker =
        fs::path(database) / "pg_tblspc" / "fast_space.path";
    assert(fs::is_regular_file(marker));
    assert(readFile(marker) == fs::weakly_canonical(location).string() + "\n");
    assert(fs::is_directory(location / database));
    assert(engine.createTablespace(database, "same_location",
                                   location.string()) ==
           DBStatus::INVALID_VALUE);
    assert(engine.createTablespace(database, "pg_default",
                                   alternate.string()) ==
           DBStatus::INVALID_VALUE);
    assert(engine.createTablespace(database, "../escape",
                                   alternate.string()) ==
           DBStatus::INVALID_VALUE);
    assert(engine.createTablespace(database, "inside_database",
                                   fs::absolute(database).string()) ==
           DBStatus::INVALID_VALUE);

    // Concurrent publication has one winner; the losing create neither
    // replaces the marker nor redirects it to a different location.
    DBStatus first = DBStatus::IO_ERROR;
    DBStatus second = DBStatus::IO_ERROR;
    std::thread creator1([&] {
        first = engine.createTablespace(
            database, "concurrent_space", alternate.string());
    });
    std::thread creator2([&] {
        second = engine.createTablespace(
            database, "concurrent_space", failed.string());
    });
    creator1.join();
    creator2.join();
    assert((first == DBStatus::OK &&
            second == DBStatus::TABLE_ALREADY_EXISTS) ||
           (second == DBStatus::OK &&
            first == DBStatus::TABLE_ALREADY_EXISTS));
    const fs::path concurrentMarker =
        fs::path(database) / "pg_tblspc" / "concurrent_space.path";
    const std::string concurrentContents = readFile(concurrentMarker);
    assert(concurrentContents == fs::weakly_canonical(alternate).string() + "\n" ||
           concurrentContents == fs::weakly_canonical(failed).string() + "\n");
    assert(engine.dropTablespace(database, "concurrent_space") ==
           DBStatus::OK);

    // A directory-fsync failure must not leave a marker that later DDL can
    // trust as a successfully-created tablespace.
    dbms::index_file::failNextDirectorySyncForTesting();
    assert(engine.createTablespace(database, "failed_space",
                                   failed.string()) == DBStatus::IO_ERROR);
    assert(!fs::exists(
        fs::path(database) / "pg_tblspc" / "failed_space.path"));

    auto table = simpleTable("items", "fast_space");
    assert(engine.createTable(database, table) == DBStatus::OK);
    assert(engine.insert(database, "items", {{"id", "7"}}) == DBStatus::OK);
    assert(engine.dropTablespace(database, "fast_space") ==
           DBStatus::INVALID_VALUE);

    // A failed publication sync after moving the first fork rolls every fork
    // back and leaves the durable schema on the original tablespace.
    dbms::index_file::failNextDirectorySyncForTesting();
    assert(engine.alterTableTablespace(database, "items", "pg_default") ==
           DBStatus::IO_ERROR);
    assert(engine.getTableSchema(database, "items").tablespace ==
           "fast_space");
    assert(fs::is_regular_file(location / database / "items.dt"));
    assert(!fs::exists(fs::path(database) / "items.dt"));

    dbms::StorageEngine restarted;
    size_t rows = 0;
    restarted.forEachRow(
        database, "items",
        [&](uint32_t, uint16_t, const char*, size_t) { ++rows; });
    assert(rows == 1);

    // Marker parsing is strict and no malformed/symlinked marker is ever
    // followed into a fallback directory.
    assert(dbms::index_file::writeAtomically(
        marker, fs::weakly_canonical(location).string() + "\nextra\n"));
    assert(engine.createTablespace(database, "fast_space",
                                   location.string()) ==
           DBStatus::CORRUPTED_DATA);
    assert(engine.createTable(
               database, simpleTable("corrupt_target", "fast_space")) ==
           DBStatus::INVALID_VALUE);
    assert(!fs::exists(
        fs::path(database) / "pg_tblspc" / "fast_space.missing"));
    assert(dbms::index_file::writeAtomically(
        marker, fs::weakly_canonical(location).string() + "\n"));

    const fs::path realMarker = marker.string() + ".real";
    fs::rename(marker, realMarker);
    fs::create_symlink(realMarker.filename(), marker);
    assert(engine.createTable(
               database, simpleTable("symlink_target", "fast_space")) ==
           DBStatus::INVALID_VALUE);
    fs::remove(marker);
    fs::rename(realMarker, marker);

    // Exercise the EXDEV fallback when a tmpfs is available. The destination
    // is staged and fsynced before the source fork is removed.
    struct stat localStatus {};
    struct stat sharedMemoryStatus {};
    const fs::path crossDevice = fs::path("/dev/shm") /
        ("dbms-cat20-" + std::to_string(::getpid()));
    if (::stat(".", &localStatus) == 0 &&
        ::stat("/dev/shm", &sharedMemoryStatus) == 0 &&
        localStatus.st_dev != sharedMemoryStatus.st_dev) {
        fs::remove_all(crossDevice);
        assert(engine.createTablespace(database, "cross_space",
                                       crossDevice.string()) == DBStatus::OK);
        assert(engine.createTable(
                   database, simpleTable("cross_items", "pg_default")) ==
               DBStatus::OK);
        assert(engine.insert(database, "cross_items", {{"id", "9"}}) ==
               DBStatus::OK);
        assert(engine.alterTableTablespace(
                   database, "cross_items", "cross_space") == DBStatus::OK);
        dbms::StorageEngine crossRestart;
        size_t crossRows = 0;
        crossRestart.forEachRow(
            database, "cross_items",
            [&](uint32_t, uint16_t, const char*, size_t) { ++crossRows; });
        assert(crossRows == 1);
        assert(engine.dropTable(database, "cross_items") == DBStatus::OK);
        assert(engine.dropTablespace(database, "cross_space") == DBStatus::OK);
        fs::remove_all(crossDevice);
    }

    assert(engine.dropTable(database, "items") == DBStatus::OK);
    assert(engine.dropTablespace(database, "fast_space") == DBStatus::OK);
    assert(!fs::exists(marker));
    assert(!fs::exists(location / database));
    assert(fs::is_directory(location));

    assert(engine.dropDatabase(database) == DBStatus::OK);
    fs::remove_all(location);
    fs::remove_all(alternate);
    fs::remove_all(failed);
    finalCleanupTestData();
    std::cout << "[TABLESPACE SEMANTICS] passed" << std::endl;
    return 0;
}
