#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "access/IndexFileUtil.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

namespace {

size_t rowCount(dbms::StorageEngine& engine, const std::string& database,
                const std::string& table = "t") {
    size_t rows = 0;
    assert(engine.forEachRow(
        database, table,
        [&](uint32_t, uint16_t, const char*, size_t) { ++rows; }));
    return rows;
}

unsigned directoryCount(const std::filesystem::path& root) {
    unsigned count = 1;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(root)) {
        if (entry.is_directory()) ++count;
    }
    return count;
}

unsigned mainRestoreDirectoryCount(const std::filesystem::path& backup) {
    unsigned count = 1;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(backup)) {
        const auto relative = std::filesystem::relative(entry.path(), backup);
        const auto first = relative.begin();
        if (first != relative.end() &&
            (*first == "wal_archive" || *first == "tablespaces")) {
            continue;
        }
        if (entry.is_directory()) ++count;
    }
    return count;
}

void assertNoRestoreStaging(const std::filesystem::path& root) {
    if (!std::filesystem::exists(root)) return;
    for (const auto& entry : std::filesystem::directory_iterator(root)) {
        const auto name = entry.path().filename().string();
        assert(name.find(".restore_staging.") == std::string::npos);
        assert(name.find(".restore_archive.") == std::string::npos);
        assert(name.find(".restore_tablespace.") == std::string::npos);
    }
}

void writeText(const std::filesystem::path& path, const std::string& text) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output << text;
    assert(output.good());
}

std::string readText(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

void assertOriginalDatabase(dbms::StorageEngine& engine,
                            const std::string& database,
                            const char* phase) {
    assert(engine.databaseExists(database));
    assert(engine.tableExists(database, "t"));
    const size_t rows = rowCount(engine, database);
    if (rows != 1) {
        std::cerr << phase << ": expected one row, found " << rows
                  << std::endl;
    }
    assert(rows == 1);
}

}  // namespace

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string database = testDbPath("restore_validation_db");
    const std::string invalidBackup =
        testDbPath("restore_validation_invalid_backup");
    const std::string brokenBackup =
        testDbPath("restore_validation_broken_backup");
    const std::string validBackup =
        testDbPath("restore_validation_valid_backup");

    {
        dbms::StorageEngine engine;
        assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
        dbms::TableSchema table;
        table.tablename = "t";
        table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        table.append(dbms::makeIntColumn("id", false, 4, true));
        assert(engine.createTable(database, table) == dbms::DBStatus::OK);
        assert(engine.insert(database, "t", {{"id", "1"}}) ==
               dbms::DBStatus::OK);

        // A restore source that aliases the target must be rejected before
        // the destination directory is touched.
        assert(!engine.physicalRestore(database, database));
        assertOriginalDatabase(engine, database, "same-path rejection");

        // An arbitrary directory is not a physical backup. Reject it without
        // deleting the existing target database.
        std::filesystem::create_directories(invalidBackup);
        {
            std::ofstream sentinel(
                std::filesystem::path(invalidBackup) / "not-a-backup");
            sentinel << "invalid\n";
        }
        assert(!engine.physicalRestore(database, invalidBackup));
        assertOriginalDatabase(engine, database, "invalid-source rejection");

        // Passing the marker check is not enough to make every source entry
        // readable. A copy failure must occur before the live database is
        // replaced, not after it has already been deleted.
        std::filesystem::create_directories(brokenBackup);
        {
            std::ofstream marker(
                std::filesystem::path(brokenBackup) /
                    ".dbms_physical_backup",
                std::ios::binary);
            marker << "DBMS_PHYSICAL_BACKUP_V1\n";
            assert(marker.good());
        }
        std::filesystem::create_symlink(
            "missing-target",
            std::filesystem::path(brokenBackup) / "broken-entry");
        assert(!engine.physicalRestore(database, brokenBackup));
        assertOriginalDatabase(engine, database, "copy-failure rejection");

        assert(engine.physicalBackup(database, validBackup));

        dbms::TableSchema liveOnly;
        liveOnly.tablename = "live_only";
        liveOnly.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        liveOnly.append(dbms::makeIntColumn("id", false, 4, true));
        assert(engine.createTable(database, liveOnly) == dbms::DBStatus::OK);

        // The staged database has the same directory layout as the source
        // database. Fail the next sync, which is the parent-directory sync
        // immediately after the atomic exchange, and require rollback.
        dbms::index_file::failDirectorySyncAfterForTesting(
            mainRestoreDirectoryCount(validBackup));
        assert(!engine.physicalRestore(database, validBackup));
        assertOriginalDatabase(engine, database,
                               "publication-sync rollback");
        assert(engine.tableExists(database, "live_only"));
    }

    // A backup produced by the engine remains restorable after the stricter
    // preflight checks.
    {
        dbms::StorageEngine restored;
        assert(restored.physicalRestore(database, validBackup));
        assertOriginalDatabase(restored, database, "valid restore");
        assert(!restored.tableExists(database, "live_only"));
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / ".dbms_backup_manifest"));
        assert(!std::filesystem::exists(
            std::filesystem::path(database) / "tablespaces"));
    }

    // Main-database publication used to happen before external tablespaces
    // were copied destructively. A later external I/O failure therefore left
    // a mixed generation. Fail the tablespace publication sync after the main
    // exchange and require both roots to roll back together.
    {
        const std::string tablespaceDatabase =
            testDbPath("restore_tablespace_db");
        const std::string tablespaceBackup =
            testDbPath("restore_tablespace_backup");
        const std::string tablespaceLocation =
            testDbPath("restore_tablespace_location");
        dbms::StorageEngine engine;
        assert(engine.createDatabase(tablespaceDatabase, "utf8") ==
               dbms::DBStatus::OK);
        assert(engine.createTablespace(
                   tablespaceDatabase, "fast_space", tablespaceLocation) ==
               dbms::DBStatus::OK);

        dbms::TableSchema externalTable;
        externalTable.tablename = "external_t";
        externalTable.tablespace = "fast_space";
        externalTable.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        externalTable.append(dbms::makeIntColumn("id", false, 4, true));
        assert(engine.createTable(tablespaceDatabase, externalTable) ==
               dbms::DBStatus::OK);
        assert(engine.insert(
                   tablespaceDatabase, "external_t", {{"id", "1"}}) ==
               dbms::DBStatus::OK);
        const auto archiveRoot =
            std::filesystem::path(tablespaceDatabase + ".archive");
        std::filesystem::create_directories(archiveRoot);
        writeText(archiveRoot / "generation", "backup\n");
        assert(engine.physicalBackup(tablespaceDatabase, tablespaceBackup));

        assert(engine.insert(
                   tablespaceDatabase, "external_t", {{"id", "2"}}) ==
               dbms::DBStatus::OK);
        writeText(archiveRoot / "generation", "live\n");
        dbms::TableSchema liveOnly;
        liveOnly.tablename = "live_only";
        liveOnly.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
        liveOnly.append(dbms::makeIntColumn("id", false, 4, true));
        assert(engine.createTable(tablespaceDatabase, liveOnly) ==
               dbms::DBStatus::OK);

        const auto externalBackupRoot =
            std::filesystem::path(tablespaceBackup) / "tablespaces" /
            "fast_space";
        const auto archiveBackupRoot =
            std::filesystem::path(tablespaceBackup) / "wal_archive";
        assert(std::filesystem::is_directory(externalBackupRoot));
        assert(std::filesystem::is_directory(archiveBackupRoot));
        const unsigned successfulSyncs =
            mainRestoreDirectoryCount(tablespaceBackup) +
            directoryCount(archiveBackupRoot) +
            directoryCount(externalBackupRoot) +
            2;  // main and WAL archive generation publication
        dbms::index_file::failDirectorySyncAfterForTesting(successfulSyncs);
        assert(!engine.physicalRestore(tablespaceDatabase, tablespaceBackup));
        assert(engine.tableExists(tablespaceDatabase, "live_only"));
        assert(rowCount(engine, tablespaceDatabase, "external_t") == 2);
        assert(readText(archiveRoot / "generation") == "live\n");
        assertNoRestoreStaging(".");
        assertNoRestoreStaging(tablespaceLocation);

        assert(engine.physicalRestore(tablespaceDatabase, tablespaceBackup));
        assert(!engine.tableExists(tablespaceDatabase, "live_only"));
        assert(rowCount(engine, tablespaceDatabase, "external_t") == 1);
        assert(readText(archiveRoot / "generation") == "backup\n");
        assertNoRestoreStaging(".");
        assertNoRestoreStaging(tablespaceLocation);
    }

    finalCleanupTestData();
    std::cout << "[PHYSICAL RESTORE VALIDATION] all passed" << std::endl;
    return 0;
}
