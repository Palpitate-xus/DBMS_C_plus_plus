#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

namespace {

size_t rowCount(dbms::StorageEngine& engine, const std::string& database) {
    size_t rows = 0;
    assert(engine.forEachRow(
        database, "t",
        [&](uint32_t, uint16_t, const char*, size_t) { ++rows; }));
    return rows;
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
    }

    // A backup produced by the engine remains restorable after the stricter
    // preflight checks.
    {
        dbms::StorageEngine restored;
        assert(restored.physicalRestore(database, validBackup));
        assertOriginalDatabase(restored, database, "valid restore");
    }

    finalCleanupTestData();
    std::cout << "[PHYSICAL RESTORE VALIDATION] all passed" << std::endl;
    return 0;
}
