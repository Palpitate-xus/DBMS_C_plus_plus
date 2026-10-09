#pragma once

#include "common/DbError.h"
#include <charconv>
#include <cstdint>
#include <string>
#include <vector>

namespace dbms::numeric_type_detail {
// PostgreSQL numeric typmod: precision in the upper 16 bits, signed scale
// in the lower 11 bits, then the four-byte varlena header offset.
inline int32_t pack(const std::vector<std::string>& values) {
    if (values.empty()) return -1;
    if (values.size() > 2) throw DbError("22023", "invalid numeric modifiers");
    const auto number = [](const std::string& text) {
        int value = 0;
        const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
        if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size())
            throw DbError("22023", "invalid numeric modifiers");
        return value;
    };
    const int precision = number(values[0]);
    const int scale = values.size() == 2 ? number(values[1]) : 0;
    if (precision < 1 || precision > 1000)
        throw DbError("22023", "numeric precision must be between 1 and 1000");
    if (scale < -1000 || scale > 1000)
        throw DbError("22023", "numeric scale must be between -1000 and 1000");
    return static_cast<int32_t>((uint32_t(precision) << 16) | (uint32_t(scale) & 0x7ff)) + 4;
}

inline std::vector<std::string> unpack(int32_t packed) {
    if (packed == -1) return {};
    if (packed < 4) throw DbError("22023", "invalid numeric modifier");
    const uint32_t raw = static_cast<uint32_t>(packed - 4);
    const int precision = raw >> 16;
    const int scale = static_cast<int>((raw & 0x7ff) ^ 0x400) - 0x400;
    const std::vector<std::string> values{std::to_string(precision), std::to_string(scale)};
    if (pack(values) != packed) throw DbError("22023", "invalid numeric modifier");
    return values;
}
} // namespace dbms::numeric_type_detail
