#include "TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>

int main() {
    namespace fs = std::filesystem;
    const std::string source = testDbPath("bk_src");
    const std::string unrelated = testDbPath("bk_unrelated");
    const std::string other = testDbPath("bk_other");
    const std::string otherBackup = testDbPath("bk_other_image");
    fs::remove_all(source);
    fs::remove_all(unrelated);
    fs::remove_all(other);
    fs::remove_all(otherBackup);

    dbms::StorageEngine engine;
    assert(engine.createDatabase(source) == dbms::DBStatus::OK);
    assert(engine.beginTransaction(source, true) == dbms::DBStatus::OK);
    const std::string occupied = source + ".txn_backup." +
                                 std::to_string(engine.currentTxnId());
    fs::remove_all(occupied);
    assert(engine.createDatabase(occupied) == dbms::DBStatus::OK);
    std::ofstream(occupied + "/keep.txt") << "database data";

    // A transaction snapshot must not remove a real database whose quoted
    // name happens to match the generated snapshot path.
    assert(!engine.createTransactionBackup());
    assert(engine.databaseExists(occupied));
    assert(fs::exists(occupied + "/keep.txt"));
    assert(engine.rollbackTransaction() == dbms::DBStatus::OK);

    // Explicit backup replacement is allowed only for an existing backup
    // generation from this source, never for another database or a directory
    // containing unrelated user files.
    assert(!engine.physicalBackup(source, occupied));
    assert(engine.databaseExists(occupied));
    assert(fs::exists(occupied + "/keep.txt"));
    fs::create_directory(unrelated);
    std::ofstream(unrelated + "/keep.txt") << "unrelated files";
    assert(!engine.physicalBackup(source, unrelated));
    assert(fs::exists(unrelated + "/keep.txt"));
    assert(engine.createDatabase(other) == dbms::DBStatus::OK);
    assert(engine.physicalBackup(other, otherBackup));
    assert(!engine.physicalBackup(source, otherBackup));
    assert(engine.physicalRestore(other, otherBackup));

    assert(engine.dropDatabase(occupied) == dbms::DBStatus::OK);
    assert(engine.dropDatabase(other) == dbms::DBStatus::OK);
    assert(engine.dropDatabase(source) == dbms::DBStatus::OK);
    fs::remove_all(unrelated);
    fs::remove_all(otherBackup);
}
