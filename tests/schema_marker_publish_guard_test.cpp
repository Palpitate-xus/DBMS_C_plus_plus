#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>

namespace fs = std::filesystem;
using dbms::DBStatus;

int main() {
    cleanupAllTestData();
    const std::string database = testDbPath("schema_marker_publish");
    dbms::StorageEngine first;
    assert(first.createDatabase(database, "utf8") == DBStatus::OK);

    const fs::path occupied = fs::path(database) / ".schema_occupied";
    fs::create_symlink("missing_marker_target", occupied);
    assert(fs::is_symlink(occupied));
    assert(!fs::exists(occupied));
    assert(first.createSchema(database, "occupied") ==
           DBStatus::TABLE_ALREADY_EXISTS);
    assert(fs::is_symlink(occupied));
    assert(fs::read_symlink(occupied) == "missing_marker_target");
    fs::remove(occupied);

    dbms::StorageEngine second;
    DBStatus firstResult = DBStatus::IO_ERROR;
    DBStatus secondResult = DBStatus::IO_ERROR;
    std::thread creator1([&] {
        firstResult = first.createSchema(database, "concurrent");
    });
    std::thread creator2([&] {
        secondResult = second.createSchema(database, "concurrent");
    });
    creator1.join();
    creator2.join();
    assert((firstResult == DBStatus::OK &&
            secondResult == DBStatus::TABLE_ALREADY_EXISTS) ||
           (secondResult == DBStatus::OK &&
            firstResult == DBStatus::TABLE_ALREADY_EXISTS));
    assert(fs::is_regular_file(
        fs::path(database) / ".schema_concurrent"));

    assert(first.dropSchema(database, "concurrent", false) == DBStatus::OK);
    assert(first.dropDatabase(database) == DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[SCHEMA MARKER PUBLISH GUARD] passed" << std::endl;
    return 0;
}
