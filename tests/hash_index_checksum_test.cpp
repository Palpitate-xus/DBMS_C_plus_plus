// test_sources: src/access/HashIndex.cpp
#include "access/HashIndex.h"
#include "access/HashIndexFormat.h"

#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

namespace {

template <typename T>
void appendNative(std::string& bytes, const T& value) {
    bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

std::string readFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

void writeFile(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    assert(output.good());
}

}  // namespace

int main() {
    namespace fs = std::filesystem;
    const auto unique = std::chrono::steady_clock::now()
                            .time_since_epoch().count();
    const fs::path path = fs::temp_directory_path() /
        ("dbms_hash_checksum_" + std::to_string(::getpid()) + "_" +
         std::to_string(unique) + ".hidx");
    struct Cleanup {
        fs::path path;
        ~Cleanup() {
            std::error_code ignored;
            fs::remove(path, ignored);
        }
    } cleanup{path};

    {
        dbms::HashIndex index(path);
        assert(index.open());
        assert(index.insert("alpha", 10));
        assert(index.insert("alpha", 11));
        assert(index.flush());
        assert(index.close());
    }
    const std::string saved = readFile(path);
    assert(saved.size() > sizeof(uint32_t));
    uint32_t version = 0;
    std::memcpy(&version, saved.data() + sizeof(uint32_t), sizeof(version));
    assert(version == dbms::hash_index_format::kChecksumVersion);

    {
        dbms::HashIndex index(path);
        assert(index.openExisting());
        assert((index.search("alpha") == std::vector<int64_t>{10, 11}));
        assert(index.close());
    }

    std::string damaged = saved;
    damaged[damaged.size() - sizeof(uint32_t) - 1] ^= 1;
    writeFile(path, damaged);
    dbms::HashIndex broken(path);
    assert(!broken.openExisting());

    std::string legacy;
    const uint32_t magic = dbms::hash_index_format::kMagic;
    const uint32_t version1 = dbms::hash_index_format::kLegacyVersion;
    const uint64_t entries = 1;
    const uint64_t keyLength = 4;
    const uint64_t values = 1;
    const int64_t rid = 37;
    appendNative(legacy, magic);
    appendNative(legacy, version1);
    appendNative(legacy, entries);
    appendNative(legacy, keyLength);
    legacy.append("old!");
    appendNative(legacy, values);
    appendNative(legacy, rid);
    writeFile(path, legacy);
    {
        dbms::HashIndex index(path);
        assert(index.openExisting());
        assert((index.search("old!") == std::vector<int64_t>{rid}));
        assert(index.close());
    }

    std::cout << "[HASH] V2 CRC corruption rejection and V1 compatibility OK\n";
    return 0;
}
