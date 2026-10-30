#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

// Storage identifiers are not SQL identifiers. A qualified relation has two
// independent 63-byte components, joined by the existing "__" storage spelling.
// Keep logical identifier widths and existing schema9-D layouts unchanged.
namespace dbms::relation_name_codec {

inline constexpr size_t identifierWidth = 64;
inline constexpr size_t storageWidth = 2 * (identifierWidth - 1) + 2 + 1;
inline constexpr size_t maxListBytes = 128 * 1024 * 1024;

inline bool safe(std::string_view name) {
    return !name.empty() && name != "." && name != ".." &&
        name.find_first_of(std::string_view("\0/\\", 3)) == std::string_view::npos;
}

inline bool valid(std::string_view name) {
    if (!safe(name) || name.size() >= storageWidth) return false;
    if (name.size() < identifierWidth) return true; // Preserve legacy spellings.
    for (size_t split = name.find("__"); split != std::string_view::npos;
         split = name.find("__", split + 1)) {
        if (split > 0 && split < identifierWidth && split + 2 < name.size() &&
            name.size() - split - 2 < identifierWidth &&
            safe(name.substr(0, split)) && safe(name.substr(split + 2))) return true;
    }
    // Session-local physical names carry the backend PID, not an SQL schema.
    if (name.substr(0, 6) == "__tmp_") {
        const auto end = name.find('_', 6);
        return end > 6 && end != std::string_view::npos && end - 6 <= 20 &&
            name.size() - end - 1 < identifierWidth && safe(name.substr(end + 1)) &&
            std::all_of(name.begin() + 6, name.begin() + end,
                        [](char c) { return c >= '0' && c <= '9'; });
    }
    return false;
}

inline const std::string& listMagic() {
    // A backslash cannot occur in any valid legacy table name. The first old
    // fixed record is deliberately nonempty, so old recovery cannot silently
    // treat a new list as an empty database. New decoding never guesses width.
    static const std::string magic = [] {
        std::string value = "DBMS_RELATION_LIST_V2\\";
        value.resize(identifierWidth, '\0');
        return value;
    }();
    return magic;
}

inline uint64_t checksum(std::string_view bytes) {
    uint64_t result = 14695981039346656037ULL;
    for (const unsigned char byte : bytes) {
        result ^= byte;
        result *= 1099511628211ULL;
    }
    return result;
}

inline void append(std::string& bytes, uint64_t value, size_t width) {
    for (size_t i = 0; i < width; ++i) {
        bytes.push_back(static_cast<char>(value & 0xff)); value >>= 8;
    }
}

inline bool read(std::string_view bytes, size_t& offset, size_t width, uint64_t& value) {
    if (offset > bytes.size() || width > bytes.size() - offset) return false;
    value = 0;
    for (size_t i = 0; i < width; ++i)
        value |= uint64_t(static_cast<unsigned char>(bytes[offset++])) << (8 * i);
    return true;
}

inline std::optional<std::string> encode(const std::vector<std::string>& names) {
    if (names.size() > maxListBytes / identifierWidth) return std::nullopt;
    bool extended = false;
    std::set<std::string_view> distinct;
    for (const auto& name : names) {
        if (!valid(name) || !distinct.insert(name).second) return std::nullopt;
        extended = extended || name.size() >= identifierWidth;
    }
    std::string bytes;
    if (!extended) {
        for (const auto& name : names) {
            bytes += name; bytes.append(identifierWidth - name.size(), '\0');
        }
        return bytes;
    }
    bytes = listMagic(); append(bytes, names.size(), 4);
    for (const auto& name : names) {
        if (bytes.size() + name.size() + 10 > maxListBytes) return std::nullopt;
        append(bytes, name.size(), 2); bytes += name;
    }
    append(bytes, checksum(bytes), 8);
    return bytes;
}

inline std::optional<std::vector<std::string>> decode(std::string_view bytes) {
    if (bytes.size() > maxListBytes) return std::nullopt;
    std::vector<std::string> names;
    std::set<std::string> distinct;
    if (bytes.substr(0, identifierWidth) != listMagic()) {
        if (bytes.size() % identifierWidth) return std::nullopt;
        for (size_t offset = 0; offset < bytes.size(); offset += identifierWidth) {
            const auto record = bytes.substr(offset, identifierWidth);
            const auto end = record.find('\0');
            if (end == std::string_view::npos || end == 0 ||
                record.find_first_not_of('\0', end) != std::string_view::npos) return std::nullopt;
            std::string name(record.substr(0, end));
            if (!valid(name) || !distinct.insert(name).second) return std::nullopt;
            names.push_back(std::move(name));
        }
        return names;
    }
    size_t offset = identifierWidth;
    uint64_t count = 0;
    if (!read(bytes, offset, 4, count) || bytes.size() - offset < 8 ||
        count > (bytes.size() - offset - 8) / 3) return std::nullopt;
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t length = 0;
        if (!read(bytes, offset, 2, length) || length == 0 || length >= storageWidth ||
            offset > bytes.size() || length > bytes.size() - offset) return std::nullopt;
        std::string name(bytes.substr(offset, length)); offset += length;
        if (!valid(name) || !distinct.insert(name).second) return std::nullopt;
        names.push_back(std::move(name));
    }
    const size_t payloadEnd = offset;
    uint64_t hash = 0;
    if (!read(bytes, offset, 8, hash) || offset != bytes.size() ||
        hash != checksum(bytes.substr(0, payloadEnd))) return std::nullopt;
    return names;
}

} // namespace dbms::relation_name_codec
