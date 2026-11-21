#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "access/IndexFileUtil.h"
#include "test_utils.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string database = testDbPath("backup_replacement_source");
    const std::string backup = testDbPath("backup_replacement_image");

    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema obsolete;
    obsolete.tablename = "obsolete_table";
    obsolete.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    obsolete.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, obsolete) == dbms::DBStatus::OK);
    assert(engine.insert(database, "obsolete_table", {{"id", "1"}}) ==
           dbms::DBStatus::OK);
    assert(engine.physicalBackup(database, backup));

    assert(engine.dropTable(database, "obsolete_table") ==
           dbms::DBStatus::OK);
    dbms::TableSchema current;
    current.tablename = "current_table";
    current.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    current.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, current) == dbms::DBStatus::OK);
    assert(engine.insert(database, "current_table", {{"id", "2"}}) ==
           dbms::DBStatus::OK);

    // Fail the parent-directory sync immediately after the atomic exchange.
    // The old generation must be exchanged back before failure is reported.
    bool sawIncompleteStaging = false;
    bool armed = false;
    const auto stagingPrefix = "." + backup + ".backup_staging.";
    const auto failAtPublication = [&](uint64_t bytesCopied) {
        for (const auto& entry : std::filesystem::directory_iterator(".")) {
            const auto name = entry.path().filename().string();
            if (name.rfind(stagingPrefix, 0) != 0) continue;
            const auto marker = entry.path() / ".dbms_physical_backup";
            std::ifstream markerInput(marker, std::ios::binary);
            const std::string markerBytes{std::istreambuf_iterator<char>(markerInput), {}};
            if (markerBytes != "DBMS_PHYSICAL_BACKUP_V3\n") {
                if (std::filesystem::is_regular_file(entry.path() / "tlist.lst")) {
                    assert(!engine.databaseExists(name));
                    sawIncompleteStaging = true;
                }
                continue;
            }
            if (bytesCopied != 0 ||
                !std::filesystem::is_regular_file(entry.path() / ".dbms_backup_manifest")) {
                continue;
            }
            assert(!armed && "the operation has exactly one completed staging directory");
            assert(std::filesystem::is_regular_file(entry.path() / "current_table.stc"));
            assert(std::filesystem::is_regular_file(
                std::filesystem::path(backup) / "obsolete_table.stc"));
            armed = true;
            dbms::index_file::failNextDirectorySyncForTesting();
        }
        return true;
    };
    assert(!engine.physicalBackup(database, backup, failAtPublication));
    assert(sawIncompleteStaging && "observed the copied table list before backup completion");
    assert(armed && "the new generation reached the final progress callback");
    assert(std::filesystem::is_regular_file(
        std::filesystem::path(backup) / "obsolete_table.stc"));
    assert(!std::filesystem::exists(
        std::filesystem::path(backup) / "current_table.stc"));

    // Reusing a destination must replace its complete generation, not merge
    // the new source into files left by the previous backup.
    assert(engine.physicalBackup(database, backup));

    dbms::TableSchema afterBackup;
    afterBackup.tablename = "after_backup";
    afterBackup.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    afterBackup.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, afterBackup) == dbms::DBStatus::OK);
    assert(engine.physicalRestore(database, backup));
    assert(engine.tableExists(database, "current_table"));
    assert(!engine.tableExists(database, "obsolete_table"));
    assert(!engine.tableExists(database, "after_backup"));

    finalCleanupTestData();
    std::cout << "[PHYSICAL BACKUP REPLACEMENT] passed\n";
    return 0;
}
