#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "access/IndexFileUtil.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
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
    unsigned backupDirectoryCount = 1;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(backup)) {
        if (entry.is_directory()) ++backupDirectoryCount;
    }
    dbms::index_file::failDirectorySyncAfterForTesting(
        2 + backupDirectoryCount);
    assert(!engine.physicalBackup(database, backup));
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
