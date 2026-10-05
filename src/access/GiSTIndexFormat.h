#pragma once

#include "IndexChecksum.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dbms::gist_index_format {

constexpr std::array<char, 8> kMagic = {
    '\0', 'D', 'B', 'M', 'S', 'S', '\0', '2'};
constexpr uint32_t kVersion = 2;
constexpr size_t kChecksumBytes = sizeof(uint32_t);
constexpr size_t kHeaderBytes = kMagic.size() + sizeof(uint32_t) +
                                sizeof(uint64_t);
constexpr size_t kMinimumEntryBytes = sizeof(uint64_t) * 3;

struct Entry {
    uint64_t rid = 0;
    std::string low;
    std::string high;
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

inline void appendEntryPayload(std::string& payload, uint64_t rid,
                               std::string_view low,
                               std::string_view high) {
    appendU64(payload, rid);
    appendU64(payload, static_cast<uint64_t>(low.size()));
    appendU64(payload, static_cast<uint64_t>(high.size()));
    if (!low.empty()) payload.append(low.data(), low.size());
    if (!high.empty()) payload.append(high.data(), high.size());
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
        appendEntryPayload(payload, entry.rid, entry.low, entry.high);
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
    const uint32_t storedChecksum = index_checksum::crc32c(
        payload.data(), payload.size());
    size_t checksumOffset = payloadBytes;
    uint32_t trailer = 0;
    if (!readU32(input, checksumOffset, trailer) ||
        trailer != storedChecksum) {
        return false;
    }

    size_t offset = kMagic.size();
    uint32_t version = 0;
    uint64_t entryCount = 0;
    if (!readU32(payload, offset, version) || version != kVersion ||
        !readU64(payload, offset, entryCount) ||
        entryCount > (payloadBytes - kHeaderBytes) / kMinimumEntryBytes ||
        entryCount > entries.max_size()) {
        return false;
    }

    std::vector<Entry> decoded;
    try {
        decoded.reserve(static_cast<size_t>(entryCount));
        for (uint64_t i = 0; i < entryCount; ++i) {
            Entry entry;
            uint64_t lowLength = 0;
            uint64_t highLength = 0;
            if (!readU64(payload, offset, entry.rid) ||
                !readU64(payload, offset, lowLength) ||
                !readU64(payload, offset, highLength) ||
                lowLength > std::numeric_limits<size_t>::max() ||
                highLength > std::numeric_limits<size_t>::max() ||
                offset > payload.size() ||
                lowLength > payload.size() - offset ||
                highLength > payload.size() - offset -
                                 static_cast<size_t>(lowLength)) {
                return false;
            }
            entry.low.assign(payload.data() + offset,
                             static_cast<size_t>(lowLength));
            offset += static_cast<size_t>(lowLength);
            entry.high.assign(payload.data() + offset,
                              static_cast<size_t>(highLength));
            offset += static_cast<size_t>(highLength);
            decoded.push_back(std::move(entry));
        }
    } catch (...) {
        return false;
    }
    if (offset != payload.size()) return false;
    entries = std::move(decoded);
    return true;
}

}  // namespace dbms::gist_index_format
