// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <limits>
#include <string>

int main() {
    const std::string database = "large_object_empty_write_test_db";
    std::filesystem::remove_all(database);

    dbms::LargeObjectManager objects(database);
    const int populatedId = objects.create();
    assert(populatedId > 0);
    assert(objects.write(populatedId, 0, "payload"));
    const auto populatedPath = std::filesystem::path(database) / ".lobjects" /
        ("lo_" + std::to_string(populatedId) + ".dat");

    // A zero-byte write cannot extend the file, even beyond its current EOF.
    for (size_t offset : {size_t{0}, size_t{3}, size_t{7}, size_t{100}}) {
        assert(objects.write(populatedId, offset, ""));
        assert(objects.read(populatedId) == "payload");
        assert(objects.size(populatedId) == 7);
        assert(std::filesystem::file_size(populatedPath) == 7);
    }

    const int emptyId = objects.create();
    assert(emptyId > 0);
    const auto emptyPath = std::filesystem::path(database) / ".lobjects" /
        ("lo_" + std::to_string(emptyId) + ".dat");
    for (size_t offset : {size_t{0}, size_t{100}}) {
        assert(objects.write(emptyId, offset, ""));
        assert(objects.read(emptyId).empty());
        assert(objects.size(emptyId) == 0);
        assert(std::filesystem::file_size(emptyPath) == 0);
    }

    // Empty input must not bypass the normal object or offset validation.
    assert(!objects.write(42, 0, ""));
    assert(!objects.write(42, 100, ""));
    assert(!objects.write(0, 0, ""));
    assert(!objects.write(-1, 0, ""));
    assert(!objects.write(populatedId, std::numeric_limits<size_t>::max(), ""));
    assert(!std::filesystem::exists(
        std::filesystem::path(database) / ".lobjects" / "lo_42.dat"));

    dbms::LargeObjectManager reopened(database);
    assert(reopened.size(populatedId) == objects.size(populatedId));
    assert(reopened.size(emptyId) == objects.size(emptyId));
    assert(reopened.read(populatedId) == "payload");
    assert(reopened.read(emptyId).empty());

    std::filesystem::remove_all(database);
    std::cout << "[LARGE OBJECT EMPTY WRITE TEST] passed\n";
    return 0;
}
