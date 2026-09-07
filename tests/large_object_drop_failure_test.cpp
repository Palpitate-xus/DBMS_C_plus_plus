// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

int main() {
    const std::string database = "large_object_drop_failure_test_db";
    std::filesystem::remove_all(database);
    dbms::LargeObjectManager objects(database);
    const int objectId = objects.create();
    assert(objectId > 0);
    assert(objects.write(objectId, 0, "payload"));

    const auto objectPath = std::filesystem::path(database) / ".lobjects" /
        ("lo_" + std::to_string(objectId) + ".dat");
    const auto savedPath = std::filesystem::path(database) / "saved.bin";
    std::filesystem::rename(objectPath, savedPath);
    std::filesystem::create_directory(objectPath);
    {
        std::ofstream blocker(objectPath / "blocker");
        blocker << "occupied";
        assert(blocker.good());
    }

    // A non-empty directory makes remove fail, even when tests run as root.
    // Failed deletion must not discard the manager's cached object size.
    assert(!objects.drop(objectId));
    assert(objects.size(objectId) == 7);
    std::filesystem::remove(objectPath / "blocker");
    std::filesystem::remove(objectPath);
    std::filesystem::rename(savedPath, objectPath);
    assert(objects.read(objectId) == "payload");
    assert(objects.drop(objectId));
    assert(objects.size(objectId) == 0);
    assert(objects.drop(objectId));

    std::filesystem::remove_all(database);
    std::cout << "[LARGE OBJECT DROP FAILURE TEST] passed\n";
}
