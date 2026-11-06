#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include "access/IndexChecksum.h"

namespace dbms::brin_index_format {

constexpr uint32_t kMagic = 0x5342524e;  // SBRN
constexpr uint32_t kLegacyVersion = 1;
constexpr uint32_t kChecksumVersion = 2;
constexpr uint32_t kTypedVersion = 3;
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

// V1/V2 bounds have no comparison identity and may be lexicographic numeric
// extrema. V3 binds typed extrema to the actual physical schema and extent.
constexpr uint32_t kHasNonNull = 1, kHasNull = 2, kUnbounded = 4;
constexpr uint32_t kPartitioned = 1;
constexpr size_t kTypedMinimumBytes = 48;
struct Range {
    uint32_t first = 0, last = 0, flags = 0;
    std::string min, max;
};
struct TypedIndex {
    uint32_t pagesPerRange = 0, heapPages = 0, flags = 0;
    uint64_t physicalRelation = 0;
    std::string orderingIdentity;
    std::vector<Range> ranges;
};
inline void appendString(std::string& out, const std::string& value) {
    appendU64(out, value.size()); out.append(value);
}
inline std::string encodeTyped(const TypedIndex& index) {
    std::string out;
    appendU32(out, kMagic); appendU32(out, kTypedVersion);
    appendU64(out, index.ranges.size());
    appendU32(out, index.pagesPerRange); appendU32(out, index.heapPages);
    appendU32(out, index.flags); appendU64(out, index.physicalRelation);
    appendString(out, index.orderingIdentity);
    for (const auto& r : index.ranges) {
        appendU32(out, r.first); appendU32(out, r.last); appendU32(out, r.flags);
        appendString(out, r.min); appendString(out, r.max);
    }
    appendU32(out, index_checksum::crc32c(out.data(), out.size()));
    return out;
}
inline bool decodeTyped(std::string_view bytes, TypedIndex& out) {
    if (bytes.size() < kTypedMinimumBytes) return false;
    const auto payload = bytes.substr(0, bytes.size() - kChecksumBytes);
    size_t crcOffset = payload.size(); uint32_t crc = 0;
    if (!readU32(bytes, crcOffset, crc) || crcOffset != bytes.size() ||
        index_checksum::crc32c(payload.data(), payload.size()) != crc) return false;
    TypedIndex parsed; size_t offset = 0;
    uint32_t magic = 0, version = 0; uint64_t count = 0;
    if (!readU32(payload, offset, magic) || !readU32(payload, offset, version) ||
        !readU64(payload, offset, count) || magic != kMagic || version != kTypedVersion ||
        !readU32(payload, offset, parsed.pagesPerRange) || !parsed.pagesPerRange ||
        !readU32(payload, offset, parsed.heapPages) || !parsed.heapPages ||
        !readU32(payload, offset, parsed.flags) || (parsed.flags & ~kPartitioned) ||
        !readU64(payload, offset, parsed.physicalRelation) ||
        !readString(payload, offset, parsed.orderingIdentity) || parsed.orderingIdentity.empty() ||
        count > (payload.size() - offset) / 28) return false;
    uint32_t previous = 0;
    for (uint64_t i = 0; i < count; ++i) {
        Range r;
        if (!readU32(payload, offset, r.first) || !readU32(payload, offset, r.last) ||
            !readU32(payload, offset, r.flags) || (r.flags & ~7u) ||
            !(r.flags & (kHasNonNull | kHasNull)) ||
            ((r.flags & kUnbounded) && !(r.flags & kHasNonNull)) ||
            !readString(payload, offset, r.min) || !readString(payload, offset, r.max) ||
            !r.first || r.last < r.first || r.last >= parsed.heapPages ||
            (!(parsed.flags & kPartitioned) && r.first <= previous) ||
            (!(r.flags & kHasNonNull) && (!r.min.empty() || !r.max.empty()))) return false;
        previous = r.last; parsed.ranges.push_back(std::move(r));
    }
    if (offset != payload.size()) return false;
    out = std::move(parsed); return true;
}

}  // namespace dbms::brin_index_format
