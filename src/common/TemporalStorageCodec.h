#pragma once

#include "DateType.h"
#include "utils/interval.h"

#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
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

// Decimal time text used by the protocol and by arrays with timezone offsets.
// The clock retains timezone-independent microseconds, including 24:00.
inline std::optional<int64_t> parseClockMicros(const std::string& text) {
    if (const auto clock = parseTimeMicros(text)) return clock;
    const auto first = text.find_first_not_of(" \t\r\n\f\v");
    if (first == std::string::npos) return std::nullopt;
    const auto last = text.find_last_not_of(" \t\r\n\f\v");
    const std::string clock = text.substr(first, last - first + 1);
    const auto colon = clock.find(':');
    if (colon == std::string::npos) return std::nullopt;
    const auto secondColon = clock.find(':', colon + 1);
    if (secondColon != std::string::npos && clock.find(':', secondColon + 1) != std::string::npos)
        return std::nullopt;
    int hours = 0, minutes = 0, seconds = 0;
    const auto minuteEnd = secondColon == std::string::npos ? clock.size() : secondColon;
    if (!parseTemporalUnsigned(clock.substr(0, colon), 2, hours) ||
        !parseTemporalUnsigned(clock.substr(colon + 1, minuteEnd - colon - 1), 2, minutes))
        return std::nullopt;
    if (secondColon != std::string::npos) {
        const auto tail = clock.substr(secondColon + 1);
        const auto dot = tail.find('.');
        if (!parseTemporalUnsigned(tail.substr(0, dot), 2, seconds)) return std::nullopt;
        if (dot != std::string::npos && (dot + 1 == tail.size() ||
            tail.find_first_not_of("0123456789", dot + 1) != std::string::npos)) return std::nullopt;
    }
    if (hours > 24 || minutes > 59 || seconds > 60) return std::nullopt;
    const auto parts = parseIntervalInput(clock);
    if (!intervalInputSqlState(parts).empty() || parts.months || parts.days ||
        parts.micros < 0 || parts.micros > MicrosPerDay) return std::nullopt;
    return parts.micros;
}

struct ZonedClock {
    int64_t localMicros;
    int32_t secondsWest;
};

inline constexpr int32_t ZonedClockOffsetLimit = 16 * 3600;

inline std::optional<int32_t> parseZonedClockOffset(std::string_view offset) {
    int hours = 0, minutes = 0, seconds = 0;
    const auto colon = offset.find(':');
    if (colon == std::string_view::npos) {
        if (offset.size() <= 2) {
            if (!parseTemporalUnsigned(offset, 2, hours)) return std::nullopt;
        } else if (offset.size() <= 4) {
            const size_t hourEnd = offset.size() - 2;
            if (!parseTemporalUnsigned(offset.substr(0, hourEnd), 2, hours) ||
                !parseTemporalUnsigned(offset.substr(hourEnd), 2, minutes)) return std::nullopt;
        } else return std::nullopt;
    } else {
        const auto secondColon = offset.find(':', colon + 1);
        const auto minuteEnd = secondColon == std::string_view::npos ? offset.size() : secondColon;
        if (!parseTemporalUnsigned(offset.substr(0, colon), 2, hours) ||
            !parseTemporalUnsigned(offset.substr(colon + 1, minuteEnd - colon - 1), 2, minutes))
            return std::nullopt;
        if (secondColon != std::string_view::npos &&
            !parseTemporalUnsigned(offset.substr(secondColon + 1), 2, seconds)) return std::nullopt;
    }
    if (hours > 15 || minutes > 59 || seconds > 59) return std::nullopt;
    return (hours * 60 + minutes) * 60 + seconds;
}

inline std::string_view trimZonedClockInput(std::string_view text) {
    const auto first = text.find_first_not_of(" \t\r\n\f\v");
    if (first == std::string_view::npos) return {};
    return text.substr(first, text.find_last_not_of(" \t\r\n\f\v") - first + 1);
}

// Comparisons and binary output need only the successfully decoded datum.
// SQL input also distinguishes malformed fields, an out-of-range clock,
// and an invalid numeric displacement.
inline std::optional<ZonedClock> parseZonedClock(std::string_view value,
                                               const char** sqlState = nullptr) {
    const auto fail = [sqlState](const char* state) -> std::optional<ZonedClock> {
        if (sqlState) *sqlState = state;
        return std::nullopt;
    };
    const auto text = trimZonedClockInput(value);
    if (text.empty()) return fail("22007");
    int32_t secondsWest = 0;
    auto clock = text;
    if (text.back() == 'Z' || text.back() == 'z') {
        clock.remove_suffix(1);
    } else {
        const auto zone = text.find_last_of("+-");
        if (zone == std::string_view::npos || zone == 0) return fail("22007");
        const auto offset = text.substr(zone + 1);
        if (offset.empty() || offset.find_first_not_of("0123456789:") != std::string_view::npos)
            return fail("22007");
        const auto east = parseZonedClockOffset(offset);
        if (!east) return fail("22009");
        secondsWest = text[zone] == '+' ? -*east : *east;
        clock = text.substr(0, zone);
    }
    clock = trimZonedClockInput(clock);
    const auto dot = clock.find('.');
    const auto whole = clock.substr(0, dot);
    const auto first = whole.find(':');
    if (first == std::string_view::npos) return fail("22007");
    const auto second = whole.find(':', first + 1);
    const auto minuteEnd = second == std::string_view::npos ? whole.size() : second;
    int hours = 0, minutes = 0, seconds = 0;
    if (!parseTemporalUnsigned(whole.substr(0, first), 2, hours) ||
        !parseTemporalUnsigned(whole.substr(first + 1, minuteEnd - first - 1), 2, minutes) ||
        (second != std::string_view::npos &&
         !parseTemporalUnsigned(whole.substr(second + 1), 2, seconds))) return fail("22007");
    if (dot != std::string_view::npos && second == std::string_view::npos) {
        // With a decimal but only two fields, SQL reads minutes and seconds.
        seconds = minutes;
        minutes = hours;
        hours = 0;
    }
    if (hours > 24 || minutes > 59 || seconds > 60) return fail("22008");

    int64_t fraction = 0;
    if (dot != std::string_view::npos) {
        const auto digits = clock.substr(dot + 1);
        if (digits.find_first_not_of("0123456789") != std::string_view::npos) return fail("22007");
        if (digits.size() <= 6) {
            for (const char digit : digits) fraction = fraction * 10 + digit - '0';
            for (size_t place = digits.size(); place < 6; ++place) fraction *= 10;
        } else {
            // Match PostgreSQL's first rounding step for sub-microsecond input.
            const std::string decimal(clock.substr(dot));
            errno = 0;
            const double parsed = std::strtod(decimal.c_str(), nullptr);
            if (errno != 0) return fail("22007");
            fraction = static_cast<int64_t>(std::rint(parsed * MicrosPerSecond));
        }
    }
    const auto micros = (hours * 3600LL + minutes * 60LL + seconds) * MicrosPerSecond + fraction;
    if (micros > MicrosPerDay) return fail("22008");
    return ZonedClock{micros, secondsWest};
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

inline std::string formatZonedClock(const ZonedClock& value) {
    if (value.secondsWest <= -ZonedClockOffsetLimit || value.secondsWest >= ZonedClockOffsetLimit)
        return {};
    auto text = formatTimeMicros(value.localMicros);
    if (text.empty()) return text;
    text += value.secondsWest > 0 ? '-' : '+';
    const int offset = std::abs(value.secondsWest);
    const auto addPair = [&text](int field) {
        text += static_cast<char>('0' + field / 10);
        text += static_cast<char>('0' + field % 10);
    };
    addPair(offset / 3600);
    if (offset % 3600) {
        text += ':';
        addPair(offset / 60 % 60);
        if (offset % 60) {
            text += ':';
            addPair(offset % 60);
        }
    }
    return text;
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
