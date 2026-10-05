#pragma once

#include "IndexChecksum.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dbms::spgist_index_format {

constexpr std::array<char, 8> kMagic = {
    '\0', 'D', 'B', 'M', 'S', 'P', '\0', '2'};
constexpr uint32_t kVersion = 2;
constexpr size_t kChecksumBytes = sizeof(uint32_t);
constexpr size_t kHeaderBytes = kMagic.size() + sizeof(uint32_t) +
                                sizeof(uint64_t);
constexpr size_t kEntryBytes = sizeof(uint64_t) * 3;

struct Entry {
    uint64_t rid = 0;
    double x = 0.0;
    double y = 0.0;
};

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

inline uint64_t doubleBits(double value) {
    static_assert(sizeof(double) == sizeof(uint64_t),
                  "SP-GiST V2 requires 64-bit doubles");
    static_assert(std::numeric_limits<double>::is_iec559,
                  "SP-GiST V2 requires IEEE-754 doubles");
    uint64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    return bits;
}

inline double bitsDouble(uint64_t bits) {
    double value = 0.0;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

inline void appendEntryPayload(std::string& payload, uint64_t rid,
                               double x, double y) {
    appendU64(payload, rid);
    appendU64(payload, doubleBits(x));
    appendU64(payload, doubleBits(y));
}

inline std::string encodeV2(uint64_t entryCount,
                            std::string_view entryPayload) {
    std::string output(kMagic.begin(), kMagic.end());
    appendU32(output, kVersion);
    appendU64(output, entryCount);
    if (!entryPayload.empty()) {
        output.append(entryPayload.data(), entryPayload.size());
    }
    appendU32(output, index_checksum::crc32c(output.data(), output.size()));
    return output;
}

inline std::string encodeV2(const std::vector<Entry>& entries) {
    std::string payload;
    for (const auto& entry : entries) {
        appendEntryPayload(payload, entry.rid, entry.x, entry.y);
    }
    return encodeV2(static_cast<uint64_t>(entries.size()), payload);
}

inline bool hasMagic(std::string_view input) {
    return input.size() >= kMagic.size() &&
           std::equal(kMagic.begin(), kMagic.end(), input.begin());
}

inline bool decodeV2(std::string_view input,
                     std::vector<Entry>& entries) {
    if (!hasMagic(input) ||
        input.size() < kHeaderBytes + kChecksumBytes) {
        return false;
    }
    const size_t payloadBytes = input.size() - kChecksumBytes;
    const std::string_view payload = input.substr(0, payloadBytes);
    size_t checksumOffset = payloadBytes;
    uint32_t trailer = 0;
    if (!readU32(input, checksumOffset, trailer) ||
        trailer != index_checksum::crc32c(payload.data(), payload.size())) {
        return false;
    }

    size_t offset = kMagic.size();
    uint32_t version = 0;
    uint64_t entryCount = 0;
    if (!readU32(payload, offset, version) || version != kVersion ||
        !readU64(payload, offset, entryCount) ||
        (payloadBytes - kHeaderBytes) % kEntryBytes != 0 ||
        entryCount != (payloadBytes - kHeaderBytes) / kEntryBytes ||
        entryCount > entries.max_size()) {
        return false;
    }

    std::vector<Entry> decoded;
    try {
        decoded.reserve(static_cast<size_t>(entryCount));
        for (uint64_t i = 0; i < entryCount; ++i) {
            uint64_t xBits = 0;
            uint64_t yBits = 0;
            Entry entry;
            if (!readU64(payload, offset, entry.rid) ||
                !readU64(payload, offset, xBits) ||
                !readU64(payload, offset, yBits)) {
                return false;
            }
            entry.x = bitsDouble(xBits);
            entry.y = bitsDouble(yBits);
            if (entry.rid == 0 ||
                entry.rid > static_cast<uint64_t>(
                    std::numeric_limits<int64_t>::max()) ||
                !std::isfinite(entry.x) || !std::isfinite(entry.y)) {
                return false;
            }
            decoded.push_back(entry);
        }
    } catch (...) {
        return false;
    }
    if (offset != payload.size()) return false;
    entries = std::move(decoded);
    return true;
}

}  // namespace dbms::spgist_index_format
