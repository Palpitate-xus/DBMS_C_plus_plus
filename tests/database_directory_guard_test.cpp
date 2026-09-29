#include "TableManage.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>

int main() {
    namespace fs = std::filesystem;
    const std::string ordinary = testDbPath("directory_guard_ordinary");
    const std::string ordinaryTarget = testDbPath("directory_guard_moved");
    const std::string regularFile = testDbPath("directory_guard_file");
    const std::string valid = testDbPath("directory_guard_valid");
    const std::string alias = testDbPath("directory_guard_alias");
    const std::string backup = testDbPath("directory_guard_backup");
    fs::remove_all(ordinary);
    fs::remove_all(ordinaryTarget);
    fs::remove_all(regularFile);
    fs::remove_all(valid);
    fs::remove_all(alias);
    fs::remove_all(backup);

    dbms::StorageEngine engine;
    fs::create_directory(ordinary);
    std::ofstream(ordinary + "/keep.txt") << "unrelated data";
    assert(!engine.databaseExists(ordinary));
    assert(engine.createDatabase(ordinary) ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(engine.dropDatabase(ordinary) == dbms::DBStatus::DATABASE_NOT_FOUND);
    assert(engine.renameDatabase(ordinary, ordinaryTarget) ==
           dbms::DBStatus::DATABASE_NOT_FOUND);
    assert(fs::exists(ordinary + "/keep.txt"));
    assert(!fs::exists(ordinaryTarget));

    std::ofstream(regularFile) << "not a database";
    assert(!engine.databaseExists(regularFile));
    assert(engine.createDatabase(regularFile) ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(engine.dropDatabase(regularFile) ==
           dbms::DBStatus::DATABASE_NOT_FOUND);
    assert(fs::exists(regularFile));

    assert(engine.createDatabase(valid) == dbms::DBStatus::OK);
    fs::create_directory_symlink(valid, alias);
    assert(!engine.databaseExists(alias));
    assert(engine.dropDatabase(alias) == dbms::DBStatus::DATABASE_NOT_FOUND);
    assert(engine.renameDatabase(alias, ordinaryTarget) ==
           dbms::DBStatus::DATABASE_NOT_FOUND);
    assert(fs::is_symlink(alias));
    assert(engine.databaseExists(valid));
    fs::create_directory(backup);
    std::ofstream(backup + "/tlist.lst");
    std::ofstream(backup + "/.dbms_physical_backup") << "backup snapshot";
    assert(!engine.databaseExists(backup));
    assert(engine.dropDatabase(backup) == dbms::DBStatus::DATABASE_NOT_FOUND);
    assert(fs::exists(backup + "/.dbms_physical_backup"));

    const auto databases = engine.getDatabaseNames();
    assert(std::find(databases.begin(), databases.end(), valid) !=
           databases.end());
    assert(std::find(databases.begin(), databases.end(), alias) ==
           databases.end());
    assert(std::find(databases.begin(), databases.end(), backup) ==
           databases.end());

    fs::remove(alias);
    assert(engine.dropDatabase(valid) == dbms::DBStatus::OK);
    fs::remove_all(ordinary);
    fs::remove(regularFile);
    fs::remove_all(backup);
}
