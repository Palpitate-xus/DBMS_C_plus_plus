// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

int main() {
    const std::string database = "large_object_missing_import_test_db";
    std::filesystem::remove_all(database);

    dbms::LargeObjectManager objects(database);
    const std::filesystem::path input =
        std::filesystem::path(database) / "input.bin";
    {
        std::ofstream file(input, std::ios::binary);
        assert(file);
        file << "imported payload";
        assert(file.good());
    }

    assert(!objects.importFile(42, input.string()));
    assert(!objects.importFile(-1, input.string()));
    assert(!std::filesystem::exists(
        std::filesystem::path(database) / ".lobjects" / "lo_42.dat"));
    assert(!std::filesystem::exists(
        std::filesystem::path(database) / ".lobjects" / "lo_-1.dat"));

    const int objectId = objects.create();
    assert(objectId == 1);
    assert(objects.importFile(objectId, input.string()));
    assert(objects.read(objectId) == "imported payload");

    std::filesystem::remove_all(database);
    std::cout << "[LARGE OBJECT MISSING IMPORT TEST] passed\n";
    return 0;
}
