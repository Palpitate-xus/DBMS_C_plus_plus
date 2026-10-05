// test_sources: src/storage/LargeObject.cpp
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
    const fs::path database = "large_object_symlink_guard_test_db";
    fs::remove_all(database);
    dbms::LargeObjectManager objects(database.string());
    const int objectId = objects.create();
    assert(objectId > 0);
    assert(objects.write(objectId, 0, "payload"));

    const fs::path objectPath = fs::path(database) / ".lobjects" /
        ("lo_" + std::to_string(objectId) + ".dat");
    const fs::path sentinel = fs::path(database) / "outside.bin";
    const fs::path importSource = fs::path(database) / "import.bin";
    const fs::path exportTarget = fs::path(database) / "export.bin";
    {
        std::ofstream(sentinel, std::ios::binary) << "private";
        std::ofstream(importSource, std::ios::binary) << "replacement";
        std::ofstream(exportTarget, std::ios::binary) << "keep";
    }

    fs::remove(objectPath);
    fs::create_symlink(fs::absolute(sentinel), objectPath);
    assert(!objects.write(objectId, 0, "exfiltrate"));
    assert(!objects.truncate(objectId, 0));
    assert(objects.read(objectId).empty());
    assert(!objects.importFile(objectId, importSource.string()));
    assert(!objects.exportFile(objectId, exportTarget.string()));
    assert(readFile(sentinel) == "private");
    assert(readFile(exportTarget) == "keep");

    const fs::path externalDatabase =
        "large_object_symlink_external_test_db";
    const fs::path redirectedDatabase =
        "large_object_symlink_directory_test_db";
    fs::remove_all(externalDatabase);
    fs::remove_all(redirectedDatabase);
    fs::create_directories(externalDatabase / ".lobjects");
    const fs::path externalObject = externalDatabase / ".lobjects" / "lo_1.dat";
    {
        std::ofstream(externalObject, std::ios::binary) << "private";
    }
    fs::create_directories(redirectedDatabase);
    fs::create_directory_symlink(
        fs::absolute(externalDatabase / ".lobjects"),
        redirectedDatabase / ".lobjects");
    dbms::LargeObjectManager redirected(redirectedDatabase.string());
    assert(redirected.create() == 0);
    assert(readFile(externalObject) == "private");

    fs::remove_all(database);
    fs::remove_all(externalDatabase);
    fs::remove_all(redirectedDatabase);
    std::cout << "[LARGE OBJECT SYMLINK GUARD] passed\n";
    return 0;
}
