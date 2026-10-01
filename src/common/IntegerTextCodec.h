#pragma once

#include <cctype>
#include <cstdint>
#include <limits>
#include <string_view>

namespace dbms {

enum class IntegerTextError { None, InvalidSyntax, OutOfRange };

struct IntegerTextResult {
    int64_t value = 0;
    IntegerTextError error = IntegerTextError::None;
};

// PostgreSQL integer text input: signed decimal or 0x/0o/0b, digit
// separators, surrounding whitespace, and the declared signed width.
inline IntegerTextResult parsePostgresIntegerText(
    std::string_view text, unsigned bits) {
    const auto invalid = [] {
        return IntegerTextResult{0, IntegerTextError::InvalidSyntax};
    };
    const auto overflow = [] {
        return IntegerTextResult{0, IntegerTextError::OutOfRange};
    };
    if (bits != 16 && bits != 32 && bits != 64) return invalid();
    size_t pos = 0;
    const auto whitespace = [](unsigned char c) {
        return std::isspace(c) != 0;
    };
    while (pos < text.size() && whitespace(text[pos])) ++pos;
    bool negative = false;
    if (pos < text.size() && (text[pos] == '+' || text[pos] == '-')) {
        negative = text[pos++] == '-';
    }
    unsigned base = 10;
    bool prefixed = false;
    if (pos + 1 < text.size() && text[pos] == '0') {
        const char prefix = text[pos + 1];
        if (prefix == 'x' || prefix == 'X') base = 16;
        else if (prefix == 'o' || prefix == 'O') base = 8;
        else if (prefix == 'b' || prefix == 'B') base = 2;
        if (base != 10) {
            pos += 2;
            prefixed = true;
        }
    }
    const auto digit = [base](unsigned char c) -> int {
        unsigned value;
        if (c >= '0' && c <= '9') value = c - '0';
        else if (c >= 'a' && c <= 'f') value = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') value = c - 'A' + 10;
        else return -1;
        return value < base ? static_cast<int>(value) : -1;
    };
    const uint64_t minimumMagnitude = uint64_t{1} << (bits - 1);
    uint64_t magnitude = 0;
    size_t digits = 0;
    while (pos < text.size()) {
        const int value = digit(text[pos]);
        if (value >= 0) {
            // Keep one last-digit envelope above the signed magnitude.
            // Invalid trailing syntax is checked before the final sign/range
            // check, matching PG's small overflows versus huge overflow input.
            if (magnitude > minimumMagnitude / base) return overflow();
            magnitude = magnitude * base + static_cast<unsigned>(value);
            ++digits;
            ++pos;
        } else if (text[pos] == '_') {
            if ((digits == 0 && !prefixed) || pos + 1 >= text.size() ||
                digit(text[pos + 1]) < 0) return invalid();
            ++pos;
        } else {
            break;
        }
    }
    if (digits == 0) return invalid();
    while (pos < text.size() && whitespace(text[pos])) ++pos;
    if (pos != text.size()) return invalid();
    const uint64_t maximumMagnitude =
        negative ? minimumMagnitude : minimumMagnitude - 1;
    if (magnitude > maximumMagnitude) return overflow();
    if (negative && magnitude == (uint64_t{1} << 63)) {
        return {std::numeric_limits<int64_t>::min(), IntegerTextError::None};
    }
    const int64_t value = static_cast<int64_t>(magnitude);
    return {negative ? -value : value, IntegerTextError::None};
}

} // namespace dbms
