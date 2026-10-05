// test_sources: src/storage/LargeObject.cpp
#include "access/IndexFileUtil.h"
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

namespace fs = std::filesystem;

static std::string readFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

int main() {
    const fs::path database = "large_object_drop_durability_test_db";
    fs::remove_all(database);
    dbms::LargeObjectManager objects(database.string());
    const int objectId = objects.create();
    assert(objectId > 0);
    assert(objects.write(objectId, 0, "payload"));

    const fs::path objectPath = fs::path(database) / ".lobjects" /
        ("lo_" + std::to_string(objectId) + ".dat");

    dbms::index_file::failNextDirectorySyncForTesting();
    assert(!objects.drop(objectId));
    assert(fs::is_regular_file(objectPath));
    assert(readFile(objectPath) == "payload");
    assert(objects.read(objectId) == "payload");
    assert(objects.size(objectId) == 7);

    assert(objects.drop(objectId));
    assert(!fs::exists(objectPath));
    assert(objects.size(objectId) == 0);
    assert(objects.drop(objectId));

    const int cleanupObjectId = objects.create();
    assert(cleanupObjectId > 0);
    assert(objects.write(cleanupObjectId, 0, "cleanup"));
    const fs::path cleanupObjectPath = fs::path(database) / ".lobjects" /
        ("lo_" + std::to_string(cleanupObjectId) + ".dat");
    // Once the canonical-name removal has been synced, failure to sync the
    // cleanup of the hidden backup must not turn the durable DROP into error.
    dbms::index_file::failDirectorySyncAfterForTesting(1);
    assert(objects.drop(cleanupObjectId));
    assert(!fs::exists(cleanupObjectPath));
    assert(objects.size(cleanupObjectId) == 0);

    const fs::path objectDirectory = fs::path(database) / ".lobjects";
    for (const auto& entry : fs::directory_iterator(objectDirectory)) {
        assert(entry.path().filename().string().find(".lo_drop_") != 0);
    }

    fs::remove_all(database);
    std::cout << "[LARGE OBJECT DROP DURABILITY] passed\n";
    return 0;
}
