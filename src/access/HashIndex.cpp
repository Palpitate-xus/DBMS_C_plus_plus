#include "HashIndex.h"
#include <algorithm>

namespace dbms {

HashIndex::HashIndex(const std::filesystem::path& indexFile)
    : filePath_(indexFile) {}

bool HashIndex::open() {
    if (loaded_) return true;
    loadFromFile();
    loaded_ = true;
    return true;
}

void HashIndex::close() {
    if (loaded_ && dirty_) {
        saveToFile();
    }
    loaded_ = false;
}

void HashIndex::insert(const std::string& key, int64_t rid) {
    map_[key].push_back(rid);
    dirty_ = true;
}

void HashIndex::remove(const std::string& key, int64_t rid) {
    auto it = map_.find(key);
    if (it == map_.end()) return;
    auto& vec = it->second;
    auto vit = std::find(vec.begin(), vec.end(), rid);
    if (vit != vec.end()) {
        vec.erase(vit);
        if (vec.empty()) map_.erase(it);
        dirty_ = true;
    }
}

std::vector<int64_t> HashIndex::search(const std::string& key) const {
    auto it = map_.find(key);
    if (it != map_.end()) return it->second;
    return {};
}

bool HashIndex::contains(const std::string& key) const {
    return map_.find(key) != map_.end();
}

void HashIndex::clear() {
    map_.clear();
    dirty_ = true;
}

void HashIndex::loadFromFile() {
    map_.clear();
    std::ifstream in(filePath_, std::ios::binary);
    if (!in) return;
    size_t count = 0;
    in.read(reinterpret_cast<char*>(&count), sizeof(size_t));
    for (size_t i = 0; i < count && in; ++i) {
        size_t keyLen = 0;
        in.read(reinterpret_cast<char*>(&keyLen), sizeof(size_t));
        if (!in || keyLen > 10000) break;
        std::string key(keyLen, '\0');
        in.read(key.data(), static_cast<std::streamsize>(keyLen));
        size_t valCount = 0;
        in.read(reinterpret_cast<char*>(&valCount), sizeof(size_t));
        if (!in || valCount > 1000000) break;
        std::vector<int64_t> vals(valCount);
        for (size_t j = 0; j < valCount && in; ++j) {
            in.read(reinterpret_cast<char*>(&vals[j]), sizeof(int64_t));
        }
        map_[key] = std::move(vals);
    }
    dirty_ = false;
}

void HashIndex::saveToFile() {
    std::ofstream out(filePath_, std::ios::binary | std::ios::trunc);
    if (!out) return;
    size_t count = map_.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
    for (const auto& [key, vals] : map_) {
        size_t keyLen = key.size();
        out.write(reinterpret_cast<const char*>(&keyLen), sizeof(size_t));
        out.write(key.data(), static_cast<std::streamsize>(keyLen));
        size_t valCount = vals.size();
        out.write(reinterpret_cast<const char*>(&valCount), sizeof(size_t));
        for (int64_t v : vals) {
            out.write(reinterpret_cast<const char*>(&v), sizeof(int64_t));
        }
    }
    dirty_ = false;
}

} // namespace dbms
