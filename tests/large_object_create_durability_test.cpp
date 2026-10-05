// test_sources: src/storage/LargeObject.cpp
#include "access/IndexFileUtil.h"
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

int main() {
    const std::string directoryFailureDb =
        "large_object_create_directory_sync_failure_test_db";
    fs::remove_all(directoryFailureDb);
    dbms::index_file::failNextDirectorySyncForTesting();
    dbms::LargeObjectManager unavailable(directoryFailureDb);
    assert(unavailable.create() == 0);
    assert(!fs::exists(fs::path(directoryFailureDb) / ".lobjects" /
                       "lo_1.dat"));
    fs::remove_all(directoryFailureDb);

    const std::string objectFailureDb =
        "large_object_create_object_sync_failure_test_db";
    fs::remove_all(objectFailureDb);
    dbms::LargeObjectManager objects(objectFailureDb);
    const fs::path objectDirectory = fs::path(objectFailureDb) / ".lobjects";
    const fs::path firstObject = objectDirectory / "lo_1.dat";

    // Creation must fsync both the new object and its containing directory.
    // If the directory barrier fails after O_EXCL publication, rollback the
    // visible name before reporting allocation failure.
    dbms::index_file::failNextDirectorySyncForTesting();
    assert(objects.create() == 0);
    assert(!fs::exists(firstObject));
    assert(dbms::index_file::syncDirectory(objectDirectory));

    // A fresh manager reconstructs allocation from durable files and may
    // safely reuse the unpublished identifier.
    dbms::LargeObjectManager restarted(objectFailureDb);
    const int objectId = restarted.create();
    assert(objectId == 1);
    assert(fs::is_regular_file(firstObject));
    assert(fs::file_size(firstObject) == 0);
    fs::remove_all(directoryFailureDb);
    fs::remove_all(objectFailureDb);
    std::cout << "[LARGE OBJECT CREATE DURABILITY] passed\n";
    return 0;
}
