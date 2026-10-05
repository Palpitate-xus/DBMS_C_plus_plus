// test_sources: src/access/BloomIndex.cpp
#include "access/BloomIndex.h"
#include "access/IndexFileUtil.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

namespace {

void putU32(std::string& bytes, uint32_t value) {
    for (unsigned i = 0; i < 4; ++i)
        bytes.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
}

void putU64(std::string& bytes, uint64_t value) {
    for (unsigned i = 0; i < 8; ++i)
        bytes.push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
}

uint32_t getU32(const std::string& bytes, size_t offset) {
    uint32_t value = 0;
    for (unsigned i = 0; i < 4; ++i) {
        value |= static_cast<uint32_t>(
            static_cast<unsigned char>(bytes[offset + i])) << (8 * i);
    }
    return value;
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
        ("dbms_bloom_checksum_" + std::to_string(::getpid()) + "_" +
         std::to_string(unique) + ".bidx");
    struct Cleanup {
        fs::path path;
        ~Cleanup() {
            std::error_code ignored;
            fs::remove(path, ignored);
        }
    } cleanup{path};

    {
        dbms::BloomIndex index(path);
        assert(index.open());
        assert(index.insert("alpha", 10));
        assert(index.insert("alpha", 11));
        assert(index.flush());
        assert(index.close());
    }
    std::string saved = readFile(path);
    assert(saved.size() > 4);
    assert(getU32(saved, 0) == 0x324D4C42u);  // BLM2 checksummed format.
    {
        dbms::BloomIndex index(path);
        assert(index.openExisting());
        assert((index.search("alpha") == std::vector<int64_t>{10, 11}));
        assert(index.insert("beta", 20));
        dbms::index_file::failNextDirectorySyncForTesting();
        assert(!index.flush());
        assert(index.hasDirtyData());
        assert(index.flush());
        assert(!index.hasDirtyData());
        assert(index.close());
    }
    saved = readFile(path);

    std::string damaged = saved;
    damaged[damaged.size() - sizeof(uint32_t) - 1] ^= 1;
    writeFile(path, damaged);
    dbms::BloomIndex broken(path);
    assert(!broken.openExisting());

    // The old BLM1 format has no checksum but remains readable.
    std::string legacy;
    putU32(legacy, 0x314D4C42u);
    putU32(legacy, 64);
    putU32(legacy, 7);
    putU32(legacy, 1);
    putU32(legacy, 4);
    legacy.append("old!");
    putU32(legacy, 1);
    putU64(legacy, 37);
    writeFile(path, legacy);
    {
        dbms::BloomIndex index(path);
        assert(index.openExisting());
        assert((index.search("old!") == std::vector<int64_t>{37}));
        assert(index.close());
    }

    std::cout << "[BLOOM] BLM2 CRC corruption rejection and BLM1 compatibility OK\n";
    return 0;
}
