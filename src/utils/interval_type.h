#pragma once

#include "common/DbError.h"
#include <charconv>
#include <cstdint>
#include <string>
#include <vector>

namespace dbms::interval_type_detail {
// PostgreSQL's public interval typmod layout: field mask in the upper
// sixteen bits, fractional-second precision in the lower sixteen bits.
inline constexpr int year = 1 << 2, month = 1 << 1, day = 1 << 3;
inline constexpr int hour = 1 << 10, minute = 1 << 11, second = 1 << 12;
inline constexpr int fullRange = 0x7fff, fullPrecision = 0xffff;

inline const char* fields(int mask) {
    switch (mask) {
        case year: return "YEAR";
        case month: return "MONTH";
        case day: return "DAY";
        case hour: return "HOUR";
        case minute: return "MINUTE";
        case second: return "SECOND";
        case year | month: return "YEAR TO MONTH";
        case day | hour: return "DAY TO HOUR";
        case day | hour | minute: return "DAY TO MINUTE";
        case day | hour | minute | second: return "DAY TO SECOND";
        case hour | minute: return "HOUR TO MINUTE";
        case hour | minute | second: return "HOUR TO SECOND";
        case minute | second: return "MINUTE TO SECOND";
        case fullRange: return "";
        default: return nullptr;
    }
}

inline int fieldMask(const std::string& lower) {
    if (lower == "year") return year;
    if (lower == "month") return month;
    if (lower == "day") return day;
    if (lower == "hour") return hour;
    if (lower == "minute") return minute;
    if (lower == "second") return second;
    if (lower == "year to month") return year | month;
    if (lower == "day to hour") return day | hour;
    if (lower == "day to minute") return day | hour | minute;
    if (lower == "day to second") return day | hour | minute | second;
    if (lower == "hour to minute") return hour | minute;
    if (lower == "hour to second") return hour | minute | second;
    if (lower == "minute to second") return minute | second;
    return 0;
}

struct Modifiers {
    int range = fullRange;
    int precision = fullPrecision;
    int32_t packed() const {
        if (range == fullRange && precision == fullPrecision) return -1;
        return static_cast<int32_t>((uint32_t(range) << 16) | uint32_t(precision));
    }
};

inline Modifiers modifiers(const std::vector<std::string>& values) {
    Modifiers result;
    if (values.empty()) return result;
    if (values.size() > 2) throw DbError("22023", "invalid interval type modifier");
    const auto number = [](const std::string& text, int& value) {
        const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
        return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
    };
    if (!number(values[0], result.range) || !fields(result.range))
        throw DbError("22023", "invalid interval field mask");
    if (values.size() == 2) {
        if (!number(values[1], result.precision) || result.precision < 0)
            throw DbError("22023", "interval precision must not be negative");
        if (result.precision > 6) result.precision = 6;
    }
    return result;
}

// Render the stored semantic mask back into genuine SQL grammar, rather
// than leaking internal interval(mask,precision) into another parser.
inline std::string render(const std::string& base, const std::vector<std::string>& values) {
    if (values.empty()) return base;
    const auto spec = modifiers(values);
    const auto phrase = fields(spec.range);
    std::string result = base;
    if (*phrase) result += std::string(" ") + phrase;
    if (values.size() == 2) result += "(" + values[1] + ")";
    return result;
}
} // namespace dbms::interval_type_detail
