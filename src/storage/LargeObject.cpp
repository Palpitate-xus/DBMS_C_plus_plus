#include "LargeObject.h"

#include <algorithm>
#include <filesystem>
#include <fstream>

namespace dbms {

LargeObjectManager::LargeObjectManager(const std::string& dbPath) : dbPath_(dbPath) {
    std::filesystem::create_directories(dbPath_ + "/.lobjects");
}

int LargeObjectManager::create() {
    int id = nextId_++;
    sizes_[id] = 0;
    return id;
}

bool LargeObjectManager::write(int loId, size_t offset, const std::string& data) {
    auto path = loPath(loId);
    std::filesystem::create_directories(std::filesystem::path(path).parent_path());

    std::fstream fs(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    if (!fs) {
        fs.open(path, std::ios::out | std::ios::binary);
        if (!fs) return false;
    }
    fs.seekp(static_cast<std::streamoff>(offset));
    fs.write(data.data(), static_cast<std::streamsize>(data.size()));

    size_t end = offset + data.size();
    if (end > sizes_[loId]) sizes_[loId] = end;
    return true;
}

std::string LargeObjectManager::read(int loId, size_t offset, size_t length) const {
    auto path = loPath(loId);
    std::ifstream fs(path, std::ios::binary);
    if (!fs) return "";

    fs.seekg(0, std::ios::end);
    size_t fileSize = static_cast<size_t>(fs.tellg());
    if (offset >= fileSize) return "";

    fs.seekg(static_cast<std::streamoff>(offset));
    size_t toRead = length == 0 ? fileSize - offset : std::min(length, fileSize - offset);
    std::string data(toRead, '\0');
    fs.read(data.data(), static_cast<std::streamsize>(toRead));
    return data;
}

bool LargeObjectManager::truncate(int loId, size_t newSize) {
    auto path = loPath(loId);
    std::error_code ec;
    std::filesystem::resize_file(path, newSize, ec);
    sizes_[loId] = newSize;
    return !ec;
}

bool LargeObjectManager::drop(int loId) {
    auto path = loPath(loId);
    std::error_code ec;
    std::filesystem::remove(path, ec);
    sizes_.erase(loId);
    return !ec;
}

size_t LargeObjectManager::size(int loId) const {
    auto it = sizes_.find(loId);
    return (it != sizes_.end()) ? it->second : 0;
}

bool LargeObjectManager::importFile(int loId, const std::string& filePath) {
    std::ifstream in(filePath, std::ios::binary);
    if (!in) return false;
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    return write(loId, 0, data);
}

bool LargeObjectManager::exportFile(int loId, const std::string& filePath) const {
    std::string data = read(loId);
    std::ofstream out(filePath, std::ios::binary);
    if (!out) return false;
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    return true;
}

std::string LargeObjectManager::loPath(int loId) const {
    return dbPath_ + "/.lobjects/lo_" + std::to_string(loId) + ".dat";
}

} // namespace dbms
