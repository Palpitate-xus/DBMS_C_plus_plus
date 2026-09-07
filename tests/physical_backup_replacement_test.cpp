#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string database = testDbPath("backup_replacement_source");
    const std::string restoredDatabase =
        testDbPath("backup_replacement_restored");
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

    // Reusing a destination must replace its complete generation, not merge
    // the new source into files left by the previous backup.
    assert(engine.physicalBackup(database, backup));

    dbms::StorageEngine restored;
    assert(restored.physicalRestore(restoredDatabase, backup));
    assert(restored.tableExists(restoredDatabase, "current_table"));
    assert(!restored.tableExists(restoredDatabase, "obsolete_table"));

    finalCleanupTestData();
    std::cout << "[PHYSICAL BACKUP REPLACEMENT] passed\n";
    return 0;
}
