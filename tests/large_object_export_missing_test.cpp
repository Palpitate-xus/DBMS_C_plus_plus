// test_sources: src/storage/LargeObject.cpp
#include "storage/LargeObject.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

int main() {
    const std::string database = "large_object_export_missing_test_db";
    std::filesystem::remove_all(database);

    dbms::LargeObjectManager objects(database);
    const std::filesystem::path output =
        std::filesystem::path(database) / "existing.bin";
    {
        std::ofstream file(output, std::ios::binary);
        assert(file);
        file << "keep me";
        assert(file.good());
    }

    assert(!objects.exportFile(999, output.string()));

    std::ifstream file(output, std::ios::binary);
    assert(file);
    const std::string contents{
        std::istreambuf_iterator<char>(file),
        std::istreambuf_iterator<char>()};
    assert(contents == "keep me");
    file.close();

    const int emptyObjectId = objects.create();
    assert(emptyObjectId == 1);
    const std::filesystem::path emptyOutput =
        std::filesystem::path(database) / "empty.bin";
    assert(objects.exportFile(emptyObjectId, emptyOutput.string()));
    assert(std::filesystem::file_size(emptyOutput) == 0);

    assert(objects.write(emptyObjectId, 0, "payload"));
    const std::filesystem::path populatedOutput =
        std::filesystem::path(database) / "populated.bin";
    assert(objects.exportFile(emptyObjectId, populatedOutput.string()));
    std::ifstream populatedFile(populatedOutput, std::ios::binary);
    const std::string populatedContents{
        std::istreambuf_iterator<char>(populatedFile),
        std::istreambuf_iterator<char>()};
    assert(populatedContents == "payload");

    std::filesystem::remove_all(database);
    std::cout << "[LARGE OBJECT EXPORT MISSING TEST] passed\n";
    return 0;
}
