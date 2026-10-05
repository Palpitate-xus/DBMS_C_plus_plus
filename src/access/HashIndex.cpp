#include "HashIndex.h"
#include "HashIndexFormat.h"
#include "IndexChecksum.h"
#include "IndexFileUtil.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <sys/stat.h>

namespace dbms {

namespace {
using namespace hash_index_format;

template <typename T>
bool readExact(std::istream& in, T& value) {
    in.read(reinterpret_cast<char*>(&value), sizeof(T));
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
    if (!loadFromFile(true)) return false;
    loaded_ = true;
    return true;
}

bool HashIndex::openExisting() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (loaded_) return fileGenerationMatches();
    if (!loadFromFile(false)) return false;
    loaded_ = true;
    return true;
}

bool HashIndex::openForBuild() {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.clear();
    loaded_ = true;
    dirty_ = true;
    generationValid_ = false;
    buildPending_ = true;
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

bool HashIndex::hasStaleFileGeneration() const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!loaded_) return true;
    if (!generationValid_) return !buildPending_;
    return !fileGenerationMatches();
}

void HashIndex::discard() {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.clear();
    loaded_ = false;
    dirty_ = false;
    generationValid_ = false;
    buildPending_ = false;
}

bool HashIndex::hasDirtyData() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dirty_;
}

size_t HashIndex::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
}

bool HashIndex::loadFromFile(bool allowMissing) {
    map_.clear();
    struct stat before {};
    if (::lstat(filePath_.c_str(), &before) != 0) {
        if (errno != ENOENT || !allowMissing) return false;
        dirty_ = false;
        generationValid_ = false;
        buildPending_ = false;
        return true;
    }
    if (!S_ISREG(before.st_mode) || before.st_size < 0) return false;
    if (!captureFileGeneration() || before.st_dev != device_ ||
        before.st_ino != inode_ || before.st_size != fileSize_ ||
        before.st_mtim.tv_sec != mtimeSec_ ||
        before.st_mtim.tv_nsec != mtimeNsec_ ||
        before.st_ctim.tv_sec != ctimeSec_ ||
        before.st_ctim.tv_nsec != ctimeNsec_) {
        return false;
    }
    std::ifstream in(filePath_, std::ios::binary);
    if (!in) return false;
    uint32_t magic = 0;
    uint32_t version = 0;
    uint64_t count = 0;
    if (!readExact(in, magic) || !readExact(in, version) ||
        !readExact(in, count) || magic != kMagic ||
        (version != kLegacyVersion && version != kChecksumVersion) ||
        count > kMaxEntries) {
        map_.clear();
        return false;
    }

    const uint64_t fileBytes = static_cast<uint64_t>(before.st_size);
    uint64_t payloadBytes = fileBytes;
    if (version == kChecksumVersion) {
        if (fileBytes < sizeof(uint32_t) * 2 + sizeof(uint64_t) +
                            kChecksumBytes) {
            map_.clear();
            return false;
        }
        payloadBytes -= kChecksumBytes;

        uint32_t storedChecksum = 0;
        in.seekg(static_cast<std::streamoff>(payloadBytes), std::ios::beg);
        if (!readExact(in, storedChecksum)) {
            map_.clear();
            return false;
        }

        in.clear();
        in.seekg(0, std::ios::beg);
        uint64_t remaining = payloadBytes;
        uint32_t checksumState = 0xFFFFFFFFu;
        std::array<char, 64 * 1024> chunk{};
        while (remaining > 0) {
            const size_t amount = static_cast<size_t>(
                std::min<uint64_t>(remaining, chunk.size()));
            in.read(chunk.data(), static_cast<std::streamsize>(amount));
            if (in.gcount() != static_cast<std::streamsize>(amount)) {
                map_.clear();
                return false;
            }
            checksumState = index_checksum::crc32cUpdate(
                checksumState, chunk.data(), amount);
            remaining -= amount;
        }
        if (index_checksum::crc32cFinish(checksumState) != storedChecksum) {
            map_.clear();
            return false;
        }
        in.clear();
        in.seekg(0, std::ios::beg);
        if (!in) {
            map_.clear();
            return false;
        }
    }

    // Do not let corrupt counts make fields or RID arrays consume the checksum
    // trailer, or allocate based on a count the file cannot possibly contain.
    uint64_t consumed = 0;
    const auto readPayload = [&](char* destination, size_t length) {
        if (length > payloadBytes - consumed) return false;
        if (length != 0) {
            in.read(destination, static_cast<std::streamsize>(length));
            if (!in) return false;
        }
        consumed += length;
        return true;
    };
    const auto readPayloadValue = [&](auto& value) {
        return readPayload(reinterpret_cast<char*>(&value), sizeof(value));
    };

    in.clear();
    in.seekg(0, std::ios::beg);
    uint32_t parsedMagic = 0;
    uint32_t parsedVersion = 0;
    uint64_t parsedCount = 0;
    if (!readPayloadValue(parsedMagic) ||
        !readPayloadValue(parsedVersion) ||
        !readPayloadValue(parsedCount) || parsedMagic != magic ||
        parsedVersion != version || parsedCount != count ||
        parsedCount > (payloadBytes - consumed) /
                    (sizeof(uint64_t) * 2)) {
        map_.clear();
        return false;
    }
    for (uint64_t i = 0; i < parsedCount; ++i) {
        uint64_t keyLen = 0;
        uint64_t valCount = 0;
        std::string key;
        if (!readPayloadValue(keyLen) || keyLen > kMaxKeyLength ||
            keyLen > payloadBytes - consumed) {
            map_.clear();
            return false;
        }
        key.resize(static_cast<size_t>(keyLen));
        if (!readPayload(key.data(), key.size()) ||
            !readPayloadValue(valCount) || valCount > kMaxValuesPerKey ||
            valCount > (payloadBytes - consumed) / sizeof(int64_t)) {
            map_.clear();
            return false;
        }
        std::vector<int64_t> vals(static_cast<size_t>(valCount));
        for (auto& value : vals) {
            if (!readPayloadValue(value)) {
                map_.clear();
                return false;
            }
        }
        map_[key] = std::move(vals);
    }
    if (consumed != payloadBytes || !fileGenerationMatches()) {
        map_.clear();
        return false;
    }
    dirty_ = false;
    return true;
}

bool HashIndex::saveToFile() {
    if (map_.size() > kMaxEntries) return false;
    std::string bytes;
    bytes.reserve(sizeof(uint32_t) * 2 + sizeof(uint64_t));
    appendBytes(bytes, kMagic);
    appendBytes(bytes, kChecksumVersion);
    const uint64_t count = map_.size();
    appendBytes(bytes, count);
    for (const auto& [key, vals] : map_) {
        if (key.size() > kMaxKeyLength || vals.size() > kMaxValuesPerKey) return false;
        const uint64_t keyLen = key.size();
        appendBytes(bytes, keyLen);
        bytes.append(key);
        const uint64_t valCount = vals.size();
        appendBytes(bytes, valCount);
        for (int64_t v : vals) {
            appendBytes(bytes, v);
        }
    }
    const uint32_t checksum =
        index_checksum::crc32c(bytes.data(), bytes.size());
    appendBytes(bytes, checksum);
    if (!index_file::writeAtomically(filePath_, bytes) ||
        !captureFileGeneration()) return false;
    dirty_ = false;
    return true;
}

bool HashIndex::captureFileGeneration() {
    struct stat status {};
    if (::lstat(filePath_.c_str(), &status) != 0 ||
        !S_ISREG(status.st_mode)) {
        generationValid_ = false;
        buildPending_ = false;
        return false;
    }
    device_ = status.st_dev;
    inode_ = status.st_ino;
    fileSize_ = status.st_size;
    mtimeSec_ = status.st_mtim.tv_sec;
    mtimeNsec_ = status.st_mtim.tv_nsec;
    ctimeSec_ = status.st_ctim.tv_sec;
    ctimeNsec_ = status.st_ctim.tv_nsec;
    generationValid_ = true;
    buildPending_ = false;
    return true;
}

bool HashIndex::fileGenerationMatches() const {
    if (!generationValid_) return false;
    struct stat status {};
    return ::lstat(filePath_.c_str(), &status) == 0 &&
           S_ISREG(status.st_mode) && status.st_dev == device_ &&
           status.st_ino == inode_ && status.st_size == fileSize_ &&
           status.st_mtim.tv_sec == mtimeSec_ &&
           status.st_mtim.tv_nsec == mtimeNsec_ &&
           status.st_ctim.tv_sec == ctimeSec_ &&
           status.st_ctim.tv_nsec == ctimeNsec_;
}

} // namespace dbms
