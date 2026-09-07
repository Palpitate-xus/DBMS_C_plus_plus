// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

int main() {
    const std::string database = "large_object_truncate_failure_test_db";
    std::filesystem::remove_all(database);

    dbms::LargeObjectManager objects(database);
    const int objectId = objects.create();
    assert(objectId == 1);
    assert(objects.write(objectId, 0, "stable"));

    const int missingId = 999;
    assert(!objects.truncate(missingId, 4096));
    assert(objects.size(missingId) == 0);
    assert(objects.size(objectId) == 6);
    assert(objects.read(objectId) == "stable");

    std::filesystem::remove_all(database);
    std::cout << "[LARGE OBJECT TRUNCATE FAILURE TEST] passed\n";
    return 0;
}
