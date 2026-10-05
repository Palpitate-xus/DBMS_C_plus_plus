#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace dbms::gin_index_format {

constexpr std::array<char, 8> kMagic = {
    '\0', 'D', 'B', 'M', 'S', 'G', '\0', '2'};
constexpr uint32_t kVersion = 2;
constexpr uint32_t kChecksumBytes = sizeof(uint32_t);
constexpr uint64_t kHeaderBytes = kMagic.size() + sizeof(uint32_t) +
                                  sizeof(uint64_t);
constexpr uint64_t kMinimumEntryBytes = sizeof(uint64_t) * 3;

inline void appendU32(std::string& output, uint32_t value) {
    for (unsigned i = 0; i < sizeof(value); ++i) {
        output.push_back(static_cast<char>((value >> (8 * i)) & 0xFFu));
    }
}

inline void appendU64(std::string& output, uint64_t value) {
    for (unsigned i = 0; i < sizeof(value); ++i) {
        output.push_back(static_cast<char>((value >> (8 * i)) & 0xFFu));
    }
}

inline uint32_t decodeU32(const char* bytes) {
    uint32_t value = 0;
    for (unsigned i = 0; i < sizeof(value); ++i) {
        value |= static_cast<uint32_t>(
            static_cast<unsigned char>(bytes[i])) << (8 * i);
    }
    return value;
}

inline uint64_t decodeU64(const char* bytes) {
    uint64_t value = 0;
    for (unsigned i = 0; i < sizeof(value); ++i) {
        value |= static_cast<uint64_t>(
            static_cast<unsigned char>(bytes[i])) << (8 * i);
    }
    return value;
}

inline bool containsBinaryMarker(const char* bytes, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        if (bytes[i] == '\0') return true;
    }
    return false;
}

}  // namespace dbms::gin_index_format
