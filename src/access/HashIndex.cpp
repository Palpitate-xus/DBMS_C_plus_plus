#include "HashIndex.h"
#include "IndexFileUtil.h"
#include <algorithm>

namespace dbms {

namespace {
constexpr uint32_t HASH_MAGIC = 0x48494458; // HIDX
constexpr uint32_t HASH_VERSION = 1;
constexpr size_t MAX_ENTRIES = 1'000'000;
constexpr size_t MAX_KEY_LENGTH = 10'000;
constexpr size_t MAX_VALUES_PER_KEY = 1'000'000;

template <typename T>
bool readExact(std::istream& in, T& value) {
    in.read(reinterpret_cast<char*>(&value), sizeof(T));
    return static_cast<bool>(in);
}

bool readBytes(std::istream& in, std::string& value, size_t length) {
    if (length > MAX_KEY_LENGTH) return false;
    value.resize(length);
    if (length == 0) return true;
    in.read(value.data(), static_cast<std::streamsize>(length));
    return static_cast<bool>(in);
}

template <typename T>
void appendBytes(std::string& out, const T& value) {
    out.append(reinterpret_cast<const char*>(&value), sizeof(T));
}
}

HashIndex::HashIndex(const std::filesystem::path& indexFile)
    : filePath_(indexFile) {}

bool HashIndex::open() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (loaded_) return true;
    if (!loadFromFile()) return false;
    loaded_ = true;
    return true;
}

bool HashIndex::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (loaded_ && dirty_) {
        if (!saveToFile()) return false;
    }
    loaded_ = false;
    return true;
}

bool HashIndex::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return false;
    return !dirty_ || saveToFile();
}

bool HashIndex::insert(const std::string& key, int64_t rid) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return false;
    map_[key].push_back(rid);
    dirty_ = true;
    return true;
}

bool HashIndex::remove(const std::string& key, int64_t rid) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return false;
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    auto& vec = it->second;
    auto vit = std::find(vec.begin(), vec.end(), rid);
    if (vit == vec.end()) return false;
    vec.erase(vit);
    if (vec.empty()) map_.erase(it);
    dirty_ = true;
    return true;
}

std::vector<int64_t> HashIndex::search(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) return it->second;
    return {};
}

bool HashIndex::contains(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.find(key) != map_.end();
}

void HashIndex::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.clear();
    dirty_ = true;
}

bool HashIndex::isOpen() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return loaded_;
}

bool HashIndex::hasDirtyData() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dirty_;
}

size_t HashIndex::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
}

bool HashIndex::loadFromFile() {
    map_.clear();
    std::ifstream in(filePath_, std::ios::binary);
    if (!in) {
        if (!std::filesystem::exists(filePath_)) {
            dirty_ = false;
            return true;
        }
        return false;
    }
    uint32_t magic = 0;
    uint32_t version = 0;
    uint64_t count = 0;
    if (!readExact(in, magic) || !readExact(in, version) ||
        !readExact(in, count) || magic != HASH_MAGIC || version != HASH_VERSION ||
        count > MAX_ENTRIES) {
        map_.clear();
        return false;
    }
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t keyLen = 0;
        uint64_t valCount = 0;
        std::string key;
        if (!readExact(in, keyLen) || keyLen > MAX_KEY_LENGTH ||
            !readBytes(in, key, static_cast<size_t>(keyLen)) ||
            !readExact(in, valCount) || valCount > MAX_VALUES_PER_KEY) {
            map_.clear();
            return false;
        }
        std::vector<int64_t> vals(static_cast<size_t>(valCount));
        for (auto& value : vals) {
            if (!readExact(in, value)) {
                map_.clear();
                return false;
            }
        }
        map_[key] = std::move(vals);
    }
    if (in.peek() != std::char_traits<char>::eof()) {
        map_.clear();
        return false;
    }
    dirty_ = false;
    return true;
}

bool HashIndex::saveToFile() {
    if (map_.size() > MAX_ENTRIES) return false;
    std::string bytes;
    bytes.reserve(sizeof(uint32_t) * 2 + sizeof(uint64_t));
    appendBytes(bytes, HASH_MAGIC);
    appendBytes(bytes, HASH_VERSION);
    const uint64_t count = map_.size();
    appendBytes(bytes, count);
    for (const auto& [key, vals] : map_) {
        if (key.size() > MAX_KEY_LENGTH || vals.size() > MAX_VALUES_PER_KEY) return false;
        const uint64_t keyLen = key.size();
        appendBytes(bytes, keyLen);
        bytes.append(key);
        const uint64_t valCount = vals.size();
        appendBytes(bytes, valCount);
        for (int64_t v : vals) {
            appendBytes(bytes, v);
        }
    }
    if (!index_file::writeAtomically(filePath_, bytes)) return false;
    dirty_ = false;
    return true;
}

} // namespace dbms
