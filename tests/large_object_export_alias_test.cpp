// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

int main() {
    namespace fs = std::filesystem;
    const std::string database = "large_object_export_alias_test_db";
    fs::remove_all(database);
    dbms::LargeObjectManager objects(database);
    std::string payload(150000, 'x');
    payload[64000] = '\0';
    payload.back() = 'z';

    for (int aliasKind = 0; aliasKind < 3; ++aliasKind) {
        const int objectId = objects.create();
        assert(objectId > 0);
        assert(objects.write(objectId, 0, payload));
        const fs::path source = fs::path(database) / ".lobjects" /
            ("lo_" + std::to_string(objectId) + ".dat");
        fs::path destination = source;
        if (aliasKind == 1) {
            destination = fs::path(database) / "hard-link.bin";
            fs::create_hard_link(source, destination);
        } else if (aliasKind == 2) {
            destination = fs::path(database) / "symbolic-link.bin";
            fs::create_symlink(fs::absolute(source), destination);
        }

        assert(objects.exportFile(objectId, destination.string()));
        assert(objects.read(objectId) == payload);
        assert(fs::file_size(source) == payload.size());
        assert(fs::file_size(destination) == payload.size());
        assert(objects.size(objectId) == payload.size());
    }

    fs::remove_all(database);
    std::cout << "[LARGE OBJECT EXPORT ALIAS TEST] passed\n";
}
