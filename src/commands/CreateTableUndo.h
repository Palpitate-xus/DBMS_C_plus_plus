#pragma once

#include "commands/RelationNameCodec.h"
#include "common/sha256.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

// Compact, self-checking records for a CREATE TABLE undo journal. This file
// defines bytes only: callers own durability, ordering, and XID/record binding.
namespace dbms::create_table_undo {

struct Header {
    uint64_t xid = 0;
    uint32_t timeline = 0;
    uint64_t anchorLsn = 0;
};

struct Birth {
    uint64_t physicalId = 0;
    std::string storageName;
    std::string tablespace;
};

inline constexpr size_t digestBytes = 64; // SHA256::hash returns lowercase hex.
inline constexpr size_t maxRecordBytes = 512;

// Matches the existing tablespace marker naming restrictions. pg_global is
// not a user table location; pg_default is represented explicitly.
inline bool validTablespace(std::string_view name) {
    return !name.empty() && name.size() < relation_name_codec::identifierWidth &&
        name.front() != '.' && name != "pg_global" &&
        relation_name_codec::safe(name) &&
        name.find_first_of("|:,\n\r") == std::string_view::npos;
}

inline bool validHeader(const Header& value) {
    return value.xid != 0 && value.timeline != 0;
}

inline bool validBirth(const Birth& value) {
    return value.physicalId != 0 &&
        value.physicalId != std::numeric_limits<uint64_t>::max() &&
        relation_name_codec::valid(value.storageName) &&
        validTablespace(value.tablespace);
}

namespace detail {
inline std::string seal(std::string bytes) {
    if (bytes.size() > maxRecordBytes - digestBytes) return {};
    bytes += SHA256::hash(bytes);
    return bytes;
}

inline bool unseal(std::string_view bytes, std::string_view& payload) {
    payload = {};
    if (bytes.size() < digestBytes || bytes.size() > maxRecordBytes) return false;
    payload = bytes.substr(0, bytes.size() - digestBytes);
    if (SHA256::hash(std::string(payload)) != bytes.substr(payload.size())) {
        payload = {};
        return false;
    }
    return true;
}

inline bool text(std::string_view bytes, size_t& offset, std::string& result) {
    uint64_t length = 0;
    if (!relation_name_codec::read(bytes, offset, 2, length) ||
        length == 0 || offset > bytes.size() || length > bytes.size() - offset)
        return false;
    result.assign(bytes.substr(offset, static_cast<size_t>(length)));
    offset += static_cast<size_t>(length);
    return true;
}
} // namespace detail

inline std::string encodeHeader(const Header& value) {
    if (!validHeader(value)) return {};
    std::string bytes("CTUH", 4);
    relation_name_codec::append(bytes, 1, 4);
    relation_name_codec::append(bytes, value.xid, 8);
    relation_name_codec::append(bytes, value.timeline, 4);
    relation_name_codec::append(bytes, value.anchorLsn, 8);
    return detail::seal(std::move(bytes));
}

inline bool decodeHeader(std::string_view bytes, Header& result) {
    result = {};
    std::string_view payload;
    if (!detail::unseal(bytes, payload) || payload.size() != 28 ||
        payload.substr(0, 4) != "CTUH") return false;
    size_t offset = 4;
    uint64_t version = 0, xid = 0, timeline = 0, anchor = 0;
    if (!relation_name_codec::read(payload, offset, 4, version) || version != 1 ||
        !relation_name_codec::read(payload, offset, 8, xid) ||
        !relation_name_codec::read(payload, offset, 4, timeline) ||
        !relation_name_codec::read(payload, offset, 8, anchor) ||
        offset != payload.size()) return false;
    Header decoded{xid, static_cast<uint32_t>(timeline), anchor};
    if (!validHeader(decoded)) return false;
    result = decoded;
    return true;
}

inline std::string encodeBirth(const Birth& value) {
    if (!validBirth(value)) return {};
    std::string bytes("CTUB", 4);
    relation_name_codec::append(bytes, 1, 4);
    relation_name_codec::append(bytes, value.physicalId, 8);
    relation_name_codec::append(bytes, value.storageName.size(), 2);
    bytes += value.storageName;
    relation_name_codec::append(bytes, value.tablespace.size(), 2);
    bytes += value.tablespace;
    return detail::seal(std::move(bytes));
}

inline bool decodeBirth(std::string_view bytes, Birth& result) {
    result = {};
    std::string_view payload;
    if (!detail::unseal(bytes, payload) || payload.size() < 20 ||
        payload.substr(0, 4) != "CTUB") return false;
    size_t offset = 4;
    uint64_t version = 0, identity = 0;
    Birth decoded;
    if (!relation_name_codec::read(payload, offset, 4, version) || version != 1 ||
        !relation_name_codec::read(payload, offset, 8, identity) ||
        !detail::text(payload, offset, decoded.storageName) ||
        !detail::text(payload, offset, decoded.tablespace) ||
        offset != payload.size()) return false;
    decoded.physicalId = identity;
    if (!validBirth(decoded)) return false;
    result = std::move(decoded);
    return true;
}

} // namespace dbms::create_table_undo
