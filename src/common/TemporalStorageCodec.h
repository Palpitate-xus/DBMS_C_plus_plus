#pragma once

#include "DateType.h"

#include <cstdint>
#include <limits>
#include <optional>
#include <string>

namespace dbms::temporal_storage_detail {

// Physical finite timestamps are microseconds relative to PostgreSQL's
// 2000-01-01 epoch. These helpers consume canonical semantic-cast output,
// not arbitrary SQL input spellings. Legacy second codecs remain separate.
inline constexpr int64_t MicrosPerSecond = 1000000;
inline constexpr int64_t MicrosPerDay = 86400 * MicrosPerSecond;

inline int64_t postgresEpochSeconds() {
    return Date(2000, 1, 1).convert() * 86400;
}

struct FractionParts {
    std::string whole;
    int64_t micros = 0;
};

inline std::optional<FractionParts> canonicalFraction(
    const std::string& value, size_t timeStart) {
    const size_t first = value.find(':', timeStart);
    if (first == std::string::npos) return std::nullopt;
    const size_t second = value.find(':', first + 1);
    if (second == std::string::npos) return std::nullopt;
    FractionParts result{value, 0};
    const size_t dot = value.find('.');
    if (dot == std::string::npos) return result;
    if (dot <= second || value.find('.', dot + 1) != std::string::npos)
        return std::nullopt;
    const size_t zone = value.find_first_of("+-Zz", second + 1);
    if (zone != std::string::npos && dot > zone) return std::nullopt;
    size_t end = dot + 1;
    while (end < value.size() && value[end] >= '0' && value[end] <= '9') {
        if (end - dot > 6) return std::nullopt;
        result.micros = result.micros * 10 + value[end] - '0';
        ++end;
    }
    if (end == dot + 1) return std::nullopt;
    for (size_t digits = end - dot - 1; digits < 6; ++digits)
        result.micros *= 10;
    result.whole.erase(dot, end - dot);
    return result;
}

inline std::optional<int64_t> parseTimeMicros(const std::string& value) {
    const auto split = canonicalFraction(value, 0);
    if (!split) return std::nullopt;
    const auto& whole = split->whole;
    const size_t first = whole.find(':');
    const size_t second = whole.find(':', first + 1);
    if (whole.find(':', second + 1) != std::string::npos) return std::nullopt;
    int hour = 0, minute = 0, secondValue = 0;
    if (!parseTemporalUnsigned(std::string_view(whole).substr(0, first), 2, hour) ||
        !parseTemporalUnsigned(std::string_view(whole).substr(first + 1, second - first - 1), 2, minute) ||
        !parseTemporalUnsigned(std::string_view(whole).substr(second + 1), 2, secondValue) ||
        hour > 24 || minute > 59 || secondValue > 59 ||
        (hour == 24 && (minute || secondValue || split->micros)))
        return std::nullopt;
    return (hour * 3600LL + minute * 60LL + secondValue) * MicrosPerSecond + split->micros;
}

inline std::optional<int64_t> parseTimestampMicros(const std::string& value) {
    if (value == "infinity") return TIMESTAMP_POSITIVE_INFINITY;
    if (value == "-infinity") return TIMESTAMP_NEGATIVE_INFINITY;
    const size_t space = value.find(' ');
    if (space == std::string::npos) return std::nullopt;
    const auto split = canonicalFraction(value, space + 1);
    if (!split) return std::nullopt;
    const int64_t seconds = parseTimestampToSeconds(split->whole);
    if (seconds == 0 || isInfiniteTimestamp(seconds)) return std::nullopt;
    const __int128 micros = (static_cast<__int128>(seconds) - postgresEpochSeconds()) *
        MicrosPerSecond + split->micros;
    if (micros <= TIMESTAMP_NEGATIVE_INFINITY || micros >= TIMESTAMP_POSITIVE_INFINITY)
        return std::nullopt;
    return static_cast<int64_t>(micros);
}

inline std::optional<int64_t> roundTimestampMicros(int64_t value, int precision) {
    if (precision < -1 || precision > 6) return std::nullopt;
    if (value == INT64_MIN) return std::nullopt; // physical NULL, not a datum
    if (precision == -1 || precision == 6 || isInfiniteTimestamp(value)) return value;
    int64_t scale = 1;
    for (int digit = precision; digit < 6; ++digit) scale *= 10;
    const __int128 magnitude = value < 0 ? -static_cast<__int128>(value) : value;
    const __int128 rounded = ((magnitude + scale / 2) / scale) * scale;
    const __int128 result = value < 0 ? -rounded : rounded;
    // Finite precision adjustment must not overflow or manufacture a sentinel.
    if (result <= TIMESTAMP_NEGATIVE_INFINITY || result >= TIMESTAMP_POSITIVE_INFINITY)
        return std::nullopt;
    return static_cast<int64_t>(result);
}

inline std::optional<int64_t> roundTimeMicros(int64_t value, int precision) {
    if (value < 0 || value > MicrosPerDay) return std::nullopt;
    const auto rounded = roundTimestampMicros(value, precision);
    if (!rounded || *rounded < 0 || *rounded > MicrosPerDay) return std::nullopt;
    return rounded;
}

inline std::string fractionText(int64_t micros) {
    if (!micros) return {};
    std::string text = std::to_string(micros);
    text.insert(text.begin(), 6 - text.size(), '0');
    while (text.back() == '0') text.pop_back();
    return "." + text;
}

inline std::string formatTimeMicros(int64_t value) {
    if (value < 0 || value > MicrosPerDay) return {};
    if (value == MicrosPerDay) return "24:00:00";
    return formatTimeSeconds(static_cast<int32_t>(value / MicrosPerSecond)) +
        fractionText(value % MicrosPerSecond);
}

inline std::string formatTimestampMicros(int64_t value, bool withTimeZone = false) {
    if (isInfiniteTimestamp(value)) return formatTimestampSeconds(value);
    int64_t seconds = value / MicrosPerSecond;
    int64_t fraction = value % MicrosPerSecond;
    if (fraction < 0) { --seconds; fraction += MicrosPerSecond; }
    const __int128 absolute = static_cast<__int128>(postgresEpochSeconds()) + seconds;
    if (absolute < 0 || absolute >= TIMESTAMP_POSITIVE_INFINITY) return {};
    std::string text = formatTimestampSeconds(static_cast<int64_t>(absolute));
    if (text.empty()) return {};
    text += fractionText(fraction);
    if (withTimeZone) text += "+00";
    return text;
}

inline std::string formatTimestampMicrosWithOffset(int64_t value, int offsetMinutes) {
    if (isInfiniteTimestamp(value)) return formatTimestampSeconds(value);
    int64_t seconds = value / MicrosPerSecond;
    int64_t fraction = value % MicrosPerSecond;
    if (fraction < 0) { --seconds; fraction += MicrosPerSecond; }
    const __int128 absolute = static_cast<__int128>(postgresEpochSeconds()) + seconds;
    if (absolute < 0 || absolute >= TIMESTAMP_POSITIVE_INFINITY) return {};
    std::string text = formatTimestampWithTz(static_cast<int64_t>(absolute), offsetMinutes);
    if (text.empty()) return {};
    const size_t space = text.find(' ');
    const size_t sign = space == std::string::npos
        ? std::string::npos : text.find_first_of("+-", space + 1);
    if (sign == std::string::npos) return {};
    text.insert(sign, fractionText(fraction));
    return text;
}

} // namespace dbms::temporal_storage_detail
