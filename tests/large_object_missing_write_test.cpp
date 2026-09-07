// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

int main() {
    const std::string database = "large_object_missing_write_test_db";
    std::filesystem::remove_all(database);

    dbms::LargeObjectManager objects(database);
    assert(!objects.write(42, 0, "orphan"));
    assert(!objects.write(-1, 0, "invalid"));
    assert(!std::filesystem::exists(
        std::filesystem::path(database) / ".lobjects" / "lo_42.dat"));
    assert(!std::filesystem::exists(
        std::filesystem::path(database) / ".lobjects" / "lo_-1.dat"));

    const int objectId = objects.create();
    assert(objectId == 1);
    assert(objects.write(objectId, 0, "valid"));
    assert(objects.read(objectId) == "valid");

    std::filesystem::remove_all(database);
    std::cout << "[LARGE OBJECT MISSING WRITE TEST] passed\n";
    return 0;
}
