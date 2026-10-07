#pragma once

#include "storage/WAL.h"
#include <cstring>
#include <optional>
#include <string>
#include <vector>

namespace dbms::heap_wal_identity {

constexpr uint32_t kImageMagic = 0x31444948u;  // HID1
constexpr uint32_t kVersion = 1;
constexpr uint32_t kEventMagic = 0x31454752u;  // RGE1
constexpr size_t kMaxName = 64;

template <typename T>
inline void append(std::vector<char>& bytes, const T& value) {
    const auto* begin = reinterpret_cast<const char*>(&value);
    bytes.insert(bytes.end(), begin, begin + sizeof(value));
}

template <typename T>
inline bool read(const std::vector<char>& bytes, size_t& offset, T& value) {
    if (offset > bytes.size() || bytes.size() - offset < sizeof(value)) return false;
    std::memcpy(&value, bytes.data() + offset, sizeof(value));
    offset += sizeof(value);
    return true;
}

inline bool padding(const std::vector<char>& bytes, size_t offset) {
    if (offset > bytes.size() || bytes.size() - offset > MAXALIGN - 1) return false;
    for (; offset < bytes.size(); ++offset)
        if (bytes[offset] != 0) return false;
    return true;
}

inline void appendImage(std::vector<char>& bytes, uint64_t relationId) {
    if (relationId == 0) return;  // Legacy name-only schema/record.
    append(bytes, kImageMagic);
    append(bytes, kVersion);
    append(bytes, relationId);
}

inline bool nameIdentity(const std::vector<char>& bytes, std::string& name,
                          std::optional<uint64_t>& relationId) {
    name.clear();
    relationId.reset();
    size_t offset = 0;
    uint32_t length = 0;
    if (!read(bytes, offset, length) || length == 0 || length >= kMaxName ||
        length > bytes.size() - offset) return false;
    name.assign(bytes.data() + offset, length);
    offset += length;
    if (name.find('\0') != std::string::npos) return false;
    if (padding(bytes, offset)) return true;
    uint32_t magic = 0, version = 0;
    uint64_t identity = 0;
    if (!read(bytes, offset, magic) || !read(bytes, offset, version) ||
        !read(bytes, offset, identity) || magic != kImageMagic ||
        version != kVersion || identity == 0 || !padding(bytes, offset)) return false;
    relationId = identity;
    return true;
}

// Legacy page images may have alignment padding, never an arbitrary
// unknown trailing extension. Decode a complete structural image first.
inline bool image(const XLogRecord& record, std::optional<uint64_t>& relationId) {
    relationId.reset();
    if (record.rmid() != RM_HEAP_ID ||
        (record.info() != XLOG_HEAP_PAGE_BEFORE && record.info() != XLOG_HEAP_PAGE_AFTER))
        return false;
    size_t offset = 0;
    uint32_t nameLength = 0;
    if (!read(record.data, offset, nameLength) || nameLength == 0 ||
        nameLength >= kMaxName || nameLength > record.data.size() - offset)
        return false;
    offset += nameLength;
    uint32_t block = 0, fork = 0, pageLength = 0;
    uint16_t slot = 0;
    if (!read(record.data, offset, block) || !read(record.data, offset, fork) ||
        !read(record.data, offset, slot) || !read(record.data, offset, pageLength) ||
        pageLength == 0 || pageLength > record.data.size() - offset)
        return false;
    offset += pageLength;
    if (padding(record.data, offset)) return true;
    uint32_t magic = 0, version = 0;
    uint64_t identity = 0;
    if (!read(record.data, offset, magic) || !read(record.data, offset, version) ||
        !read(record.data, offset, identity) || magic != kImageMagic ||
        version != kVersion || identity == 0 || !padding(record.data, offset))
        return false;
    relationId = identity;
    return true;
}

struct Event {
    uint64_t relationId = 0;
    std::string name;
};

inline std::vector<char> event(uint64_t relationId, const std::string& name) {
    std::vector<char> bytes;
    if (relationId == 0 || name.empty() || name.size() >= kMaxName ||
        name.find('\0') != std::string::npos) return bytes;
    append(bytes, kEventMagic);
    append(bytes, kVersion);
    append(bytes, relationId);
    append(bytes, static_cast<uint32_t>(name.size()));
    bytes.insert(bytes.end(), name.begin(), name.end());
    return bytes;
}

inline bool event(const XLogRecord& record, Event& result) {
    result = {};
    if (record.rmid() != RM_SMGR_ID ||
        (record.info() != XLOG_SMGR_RELATION_CREATE &&
         record.info() != XLOG_SMGR_RELATION_RETIRE)) return false;
    size_t offset = 0;
    uint32_t magic = 0, version = 0, length = 0;
    if (!read(record.data, offset, magic) || !read(record.data, offset, version) ||
        !read(record.data, offset, result.relationId) || !read(record.data, offset, length) ||
        magic != kEventMagic || version != kVersion || result.relationId == 0 ||
        length == 0 || length >= kMaxName || length > record.data.size() - offset)
        return false;
    result.name.assign(record.data.data() + offset, length);
    offset += length;
    return result.name.find('\0') == std::string::npos && padding(record.data, offset);
}

}  // namespace dbms::heap_wal_identity
