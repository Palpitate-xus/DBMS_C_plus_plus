// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

int main() {
    const std::string database = "large_object_import_replace_test_db";
    std::filesystem::remove_all(database);

    dbms::LargeObjectManager objects(database);
    const int objectId = objects.create();
    assert(objectId == 1);
    assert(objects.write(objectId, 0, "old payload with a long tail"));

    const std::filesystem::path input =
        std::filesystem::path(database) / "short.bin";
    {
        std::ofstream file(input, std::ios::binary);
        assert(file);
        file << "new";
        assert(file.good());
    }

    assert(objects.importFile(objectId, input.string()));
    assert(objects.read(objectId) == "new");
    assert(objects.size(objectId) == 3);

    // The replacement size must also be visible after reconstructing the
    // manager from disk.
    dbms::LargeObjectManager reopened(database);
    assert(reopened.read(objectId) == "new");
    assert(reopened.size(objectId) == 3);

    std::filesystem::remove_all(database);
    std::cout << "[LARGE OBJECT IMPORT REPLACE TEST] passed\n";
    return 0;
}
