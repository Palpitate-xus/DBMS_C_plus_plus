// Implementation-only FLOAT8 display codec, shared by expression output and
// heap extraction. It must not replace index-key or geometry normalization.
#ifndef DBMS_FLOAT8_OUTPUT_IMPL_INCLUDED
#define DBMS_FLOAT8_OUTPUT_IMPL_INCLUDED

#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace dbms::float8_output_detail {
struct Decimal {
    std::uint64_t coefficient;
    int scale;
    bool roundTrips;
};

inline Decimal candidate(double value, int precision = 0) {
    char buffer[64];
    const auto converted = precision == 0
        ? std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::general)
        : std::to_chars(buffer, buffer + sizeof(buffer), value, std::chars_format::general, precision);
    if (converted.ec != std::errc())
        throw std::logic_error("could not format finite FLOAT8");
    std::uint64_t coefficient = 0;
    int fractional = 0;
    bool afterPoint = false;
    const char* cursor = buffer;
    for (; cursor != converted.ptr && *cursor != 'e'; ++cursor) {
        if (*cursor == '.') { afterPoint = true; continue; }
        coefficient = coefficient * 10 + static_cast<unsigned>(*cursor - '0');
        fractional += afterPoint;
    }
    int exponent = 0;
    if (cursor != converted.ptr) {
        ++cursor;
        if (*cursor == '+') ++cursor;
        const auto parsed = std::from_chars(cursor, converted.ptr, exponent);
        if (parsed.ec != std::errc() || parsed.ptr != converted.ptr)
            throw std::logic_error("invalid FLOAT8 display exponent");
    }
    bool roundTrips = true;
    if (precision != 0) {
        double parsed = 0;
        const auto decoded = std::from_chars(buffer, converted.ptr, parsed, std::chars_format::general);
        roundTrips = decoded.ec == std::errc() && decoded.ptr == converted.ptr && parsed == value;
    }
    return {coefficient, exponent - fractional, roundTrips};
}

// Compare c*10^scale with n*2^exponent EXACTLY. The normalized midpoint's
// odd significand fits 55 bits. Division by five must be exact; multiplication
// is bounded before it happens. No floating midpoint or large shift is used.
inline bool equalsMidpoint(Decimal decimal, std::uint64_t n, int exponent) {
    if (!decimal.coefficient) return false;
    while ((n & 1) == 0) { n >>= 1; ++exponent; }
    int binaryExponent = decimal.scale;
    while ((decimal.coefficient & 1) == 0) { decimal.coefficient >>= 1; ++binaryExponent; }
    if (decimal.scale >= 0) {
        for (int i = 0; i < decimal.scale; ++i) {
            if (decimal.coefficient > n / 5) return false;
            decimal.coefficient *= 5;
        }
    } else {
        for (int i = 0; i > decimal.scale; --i) {
            if (decimal.coefficient % 5) return false;
            decimal.coefficient /= 5;
        }
    }
    return decimal.coefficient == n && binaryExponent == exponent;
}

inline std::string format(double value) {
    static_assert(sizeof(double) == sizeof(std::uint64_t) && std::numeric_limits<double>::is_iec559);
    if (std::isnan(value)) return "NaN";
    if (std::isinf(value)) return std::signbit(value) ? "-Infinity" : "Infinity";
    const bool negative = std::signbit(value);
    if (value == 0) return negative ? "-0" : "0";
    value = std::fabs(value);

    std::uint64_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    const auto fraction = bits & ((std::uint64_t{1} << 52) - 1);
    const auto biased = static_cast<int>((bits >> 52) & 2047);
    const auto mantissa = fraction | (biased ? std::uint64_t{1} << 52 : 0);
    const int exponent = (biased ? biased : 1) - 1023 - 52 - 2;
    // A power of two above the minimum normal has a closer lower neighbor.
    // Subnormals and the minimum normal have symmetric rounding intervals.
    const auto lower = 4 * mantissa - (fraction == 0 && biased > 1 ? 1 : 2);
    const auto upper = 4 * mantissa + 2;
    const auto boundary = [&](Decimal decimal) {
        return equalsMidpoint(decimal, lower, exponent) || equalsMidpoint(decimal, upper, exponent);
    };
    Decimal decimal = candidate(value);
    if (boundary(decimal)) {
        // PostgreSQL excludes exact rounding midpoints, even if a reader
        // rounding ties to even would recover the value (notably 1e23).
        // Choose the first precision whose nearest candidate round-trips and
        // is strictly interior. A low precision may lie outside the interval.
        bool found = false;
        for (int precision = 1; precision <= std::numeric_limits<double>::max_digits10; ++precision) {
            const auto next = candidate(value, precision);
            if (next.roundTrips && !boundary(next)) { decimal = next; found = true; break; }
        }
        if (!found) throw std::logic_error("no interior FLOAT8 display candidate");
    }
    while (decimal.coefficient % 10 == 0) { decimal.coefficient /= 10; ++decimal.scale; }
    const auto digits = std::to_string(decimal.coefficient);
    const int length = static_cast<int>(digits.size());
    const int scientificExponent = length + decimal.scale - 1;
    std::string result = negative ? "-" : "";
    if (scientificExponent >= -4 && scientificExponent < 15) {
        const int point = length + decimal.scale;
        if (point <= 0) result += "0." + std::string(-point, '0') + digits;
        else if (point >= length) result += digits + std::string(point - length, '0');
        else result += digits.substr(0, point) + "." + digits.substr(point);
    } else {
        result += digits.front();
        if (length > 1) result += "." + digits.substr(1);
        auto magnitude = std::to_string(scientificExponent < 0 ? -scientificExponent : scientificExponent);
        if (magnitude.size() < 2) magnitude.insert(0, 1, '0');
        result += scientificExponent < 0 ? "e-" : "e+";
        result += magnitude;
    }
    return result;
}
} // namespace dbms::float8_output_detail
#endif
