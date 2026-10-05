#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace dbms::brin_index_format {

constexpr uint32_t kMagic = 0x5342524e;  // SBRN
constexpr uint32_t kLegacyVersion = 1;
constexpr uint32_t kChecksumVersion = 2;
constexpr uint32_t kChecksumBytes = sizeof(uint32_t);
constexpr uint64_t kHeaderBytes = sizeof(uint32_t) * 2 + sizeof(uint64_t);
constexpr uint64_t kMinimumRangeBytes = sizeof(uint32_t) * 2 +
                                        sizeof(uint64_t) * 2;

inline void appendU32(std::string& output, uint32_t value) {
    for (unsigned i = 0; i < sizeof(value); ++i) {
        output.push_back(static_cast<char>((value >> (8 * i)) & 0xffu));
    }
}

inline void appendU64(std::string& output, uint64_t value) {
    for (unsigned i = 0; i < sizeof(value); ++i) {
        output.push_back(static_cast<char>((value >> (8 * i)) & 0xffu));
    }
}

inline bool readU32(std::string_view input, size_t& offset,
                    uint32_t& value) {
    if (offset > input.size() || input.size() - offset < sizeof(value))
        return false;
    value = 0;
    for (unsigned i = 0; i < sizeof(value); ++i) {
        value |= static_cast<uint32_t>(
            static_cast<unsigned char>(input[offset + i])) << (8 * i);
    }
    offset += sizeof(value);
    return true;
}

inline bool readU64(std::string_view input, size_t& offset,
                    uint64_t& value) {
    if (offset > input.size() || input.size() - offset < sizeof(value))
        return false;
    value = 0;
    for (unsigned i = 0; i < sizeof(value); ++i) {
        value |= static_cast<uint64_t>(
            static_cast<unsigned char>(input[offset + i])) << (8 * i);
    }
    offset += sizeof(value);
    return true;
}

inline bool readString(std::string_view input, size_t& offset,
                       std::string& value) {
    uint64_t length = 0;
    if (!readU64(input, offset, length) || length > input.size() - offset)
        return false;
    value.assign(input.data() + offset, static_cast<size_t>(length));
    offset += static_cast<size_t>(length);
    return true;
}

}  // namespace dbms::brin_index_format
