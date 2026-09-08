#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/sha256.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace {

size_t rowCount(dbms::StorageEngine& engine, const std::string& database) {
    size_t rows = 0;
    assert(engine.forEachRow(
        database, "t",
        [&](uint32_t, uint16_t, const char*, size_t) { ++rows; }));
    return rows;
}

void assertLiveDatabase(dbms::StorageEngine& engine,
                        const std::string& database) {
    assert(engine.databaseExists(database));
    assert(engine.tableExists(database, "t"));
    assert(rowCount(engine, database) == 1);
}

}  // namespace

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    SHA256 incrementalDigest;
    incrementalDigest.append("a", 1);
    incrementalDigest.append("bc", 2);
    assert(incrementalDigest.finishHex() == SHA256::hash("abc"));

    const std::string database = testDbPath("backup_manifest_source");
    const std::string backup = testDbPath("backup_manifest_image");
    const std::string wrongName = testDbPath("backup_manifest_wrong_name");

    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema table;
    table.tablename = "t";
    table.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    assert(engine.createTable(database, table) == dbms::DBStatus::OK);
    assert(engine.insert(database, "t", {{"id", "1"}}) ==
           dbms::DBStatus::OK);

    assert(engine.physicalBackup(database, backup));
    assert(std::filesystem::is_regular_file(
        std::filesystem::path(backup) / ".dbms_backup_manifest"));

    // Appending to a payload changes both its recorded length and digest.
    {
        std::ofstream output(
            std::filesystem::path(backup) / "t.dt",
            std::ios::binary | std::ios::app);
        output.put('x');
        assert(output.good());
    }
    assert(!engine.physicalRestore(database, backup));
    assertLiveDatabase(engine, database);

    // A missing file must be rejected even when every remaining file is
    // individually readable.
    assert(engine.physicalBackup(database, backup));
    assert(std::filesystem::remove(
        std::filesystem::path(backup) / "t.stc"));
    assert(!engine.physicalRestore(database, backup));
    assertLiveDatabase(engine, database);

    // Likewise, an unlisted file cannot be smuggled into the restored tree.
    assert(engine.physicalBackup(database, backup));
    {
        std::ofstream output(
            std::filesystem::path(backup) / "unexpected-file",
            std::ios::binary);
        output << "not in manifest\n";
        assert(output.good());
    }
    assert(!engine.physicalRestore(database, backup));
    assertLiveDatabase(engine, database);

    assert(engine.physicalBackup(database, backup));
    assert(!engine.physicalRestore(wrongName, backup));
    assert(!engine.databaseExists(wrongName));
    assert(engine.insert(database, "t", {{"id", "2"}}) ==
           dbms::DBStatus::OK);
    assert(engine.physicalRestore(database, backup));
    assert(rowCount(engine, database) == 1);

    finalCleanupTestData();
    std::cout << "[PHYSICAL BACKUP MANIFEST] corruption and file set verified\n";
    return 0;
}
