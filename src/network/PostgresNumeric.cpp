#include "PostgresNumeric.h"

#include "types/numeric.h"

#include <algorithm>
#include <cctype>
#include <limits>

namespace dbms {

namespace {

constexpr uint16_t kNumericPositive = 0x0000;
constexpr uint16_t kNumericNegative = 0x4000;
constexpr uint16_t kNumericNaN = 0xC000;
constexpr uint16_t kNumericPositiveInfinity = 0xD000;
constexpr uint16_t kNumericNegativeInfinity = 0xF000;
constexpr size_t kNumericHeaderSize = 8;

uint16_t readUInt16(const std::vector<uint8_t>& raw, size_t offset) {
    return static_cast<uint16_t>((static_cast<uint16_t>(raw[offset]) << 8) |
                                 static_cast<uint16_t>(raw[offset + 1]));
}

int16_t readInt16(const std::vector<uint8_t>& raw, size_t offset) {
    return static_cast<int16_t>(readUInt16(raw, offset));
}

void appendUInt16(std::vector<uint8_t>& raw, uint16_t value) {
    raw.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    raw.push_back(static_cast<uint8_t>(value & 0xff));
}

bool isNumericSign(uint16_t sign) {
    return sign == kNumericPositive || sign == kNumericNegative ||
           sign == kNumericNaN || sign == kNumericPositiveInfinity ||
           sign == kNumericNegativeInfinity;
}

bool appendDecimalGroups(const std::string& integerPart,
                         const std::string& fractionalPart,
                         std::vector<uint16_t>& groups,
                         int16_t& weight) {
    std::string integer = integerPart;
    const size_t firstNonZero = integer.find_first_not_of('0');
    if (firstNonZero == std::string::npos) integer = "0";
    else if (firstNonZero > 0) integer.erase(0, firstNonZero);

    const size_t integerPadded = ((integer.size() + 3) / 4) * 4;
    integer.insert(0, integerPadded - integer.size(), '0');
    std::string fraction = fractionalPart;
    if (!fraction.empty()) fraction.append((4 - fraction.size() % 4) % 4, '0');

    const size_t integerGroups = integer.size() / 4;
    const size_t groupCount = integerGroups + fraction.size() / 4;
    if (groupCount > static_cast<size_t>(std::numeric_limits<int16_t>::max())) return false;

    std::vector<uint16_t> allGroups;
    allGroups.reserve(groupCount);
    const std::string digits = integer + fraction;
    for (size_t offset = 0; offset < digits.size(); offset += 4) {
        uint16_t value = 0;
        for (size_t i = 0; i < 4; ++i) {
            const char digit = digits[offset + i];
            if (digit < '0' || digit > '9') return false;
            value = static_cast<uint16_t>(value * 10 + digit - '0');
        }
        allGroups.push_back(value);
    }

    size_t leading = 0;
    while (leading < allGroups.size() && allGroups[leading] == 0) ++leading;
    if (leading == allGroups.size()) {
        groups.clear();
        weight = 0;
        return true;
    }
    size_t trailing = allGroups.size();
    while (trailing > leading && allGroups[trailing - 1] == 0) --trailing;

    const int64_t calculatedWeight = static_cast<int64_t>(integerGroups) - 1 -
                                     static_cast<int64_t>(leading);
    if (calculatedWeight < std::numeric_limits<int16_t>::min() ||
        calculatedWeight > std::numeric_limits<int16_t>::max()) return false;
    weight = static_cast<int16_t>(calculatedWeight);
    groups.assign(allGroups.begin() + static_cast<std::ptrdiff_t>(leading),
                  allGroups.begin() + static_cast<std::ptrdiff_t>(trailing));
    return true;
}

bool splitNumericText(const std::string& value, bool& negative,
                      std::string& integerPart, std::string& fractionalPart) {
    Numeric numeric;
    try {
        numeric = Numeric(value);
    } catch (...) {
        return false;
    }
    const std::string canonical = numeric.toString();
    if (!numeric.isFinite()) return false;

    size_t offset = 0;
    negative = !canonical.empty() && canonical.front() == '-';
    if (negative) offset = 1;
    const size_t dot = canonical.find('.', offset);
    if (dot == std::string::npos) {
        integerPart = canonical.substr(offset);
        fractionalPart.clear();
    } else {
        integerPart = canonical.substr(offset, dot - offset);
        fractionalPart = canonical.substr(dot + 1);
    }
    return !integerPart.empty();
}

} // namespace

bool decodePostgresNumeric(const std::vector<uint8_t>& raw, std::string& text) {
    text.clear();
    if (raw.size() < kNumericHeaderSize) return false;

    const uint16_t digitCount = readUInt16(raw, 0);
    const int16_t weight = readInt16(raw, 2);
    const uint16_t sign = readUInt16(raw, 4);
    const int16_t displayScale = readInt16(raw, 6);
    if (!isNumericSign(sign) || displayScale < 0 ||
        raw.size() != kNumericHeaderSize + static_cast<size_t>(digitCount) * 2) {
        return false;
    }
    if (sign == kNumericNaN) {
        if (digitCount != 0) return false;
        text = "NaN";
        return true;
    }
    if (sign == kNumericPositiveInfinity || sign == kNumericNegativeInfinity) {
        if (digitCount != 0) return false;
        text = sign == kNumericNegativeInfinity ? "-Infinity" : "Infinity";
        return true;
    }

    // The engine intentionally caps Numeric precision at 1000 digits. This
    // also bounds allocation for untrusted binary protocol input.
    const size_t maxGroups = (static_cast<size_t>(Numeric::kMaxPrecision) + 3) / 4;
    if (digitCount > maxGroups || displayScale > Numeric::kMaxPrecision) return false;
    if (digitCount == 0) {
        text = "0";
        return true;
    }

    std::string digits;
    digits.reserve(static_cast<size_t>(digitCount) * 4);
    for (size_t i = 0; i < digitCount; ++i) {
        const uint16_t group = readUInt16(raw, kNumericHeaderSize + i * 2);
        if (group > 9999) return false;
        const int thousands = group / 1000;
        const int hundreds = (group / 100) % 10;
        const int tens = (group / 10) % 10;
        const int ones = group % 10;
        digits.push_back(static_cast<char>('0' + thousands));
        digits.push_back(static_cast<char>('0' + hundreds));
        digits.push_back(static_cast<char>('0' + tens));
        digits.push_back(static_cast<char>('0' + ones));
    }

    const int decimalPosition = (static_cast<int>(weight) + 1) * 4;
    std::string integerPart;
    std::string fractionalPart;
    if (decimalPosition <= 0) {
        integerPart = "0";
        fractionalPart.assign(static_cast<size_t>(-decimalPosition), '0');
        fractionalPart += digits;
    } else if (decimalPosition >= static_cast<int>(digits.size())) {
        integerPart = digits;
        integerPart.append(static_cast<size_t>(decimalPosition) - digits.size(), '0');
    } else {
        integerPart = digits.substr(0, static_cast<size_t>(decimalPosition));
        fractionalPart = digits.substr(static_cast<size_t>(decimalPosition));
    }

    const size_t firstNonZero = integerPart.find_first_not_of('0');
    if (firstNonZero == std::string::npos) integerPart = "0";
    else if (firstNonZero > 0) integerPart.erase(0, firstNonZero);
    if (fractionalPart.size() > static_cast<size_t>(displayScale)) {
        fractionalPart.resize(static_cast<size_t>(displayScale));
    } else {
        fractionalPart.append(static_cast<size_t>(displayScale) - fractionalPart.size(), '0');
    }

    if (integerPart.size() + fractionalPart.size() > Numeric::kMaxPrecision) return false;
    text = integerPart;
    if (displayScale > 0) {
        text.push_back('.');
        text += fractionalPart;
    }
    if (sign == kNumericNegative) text.insert(text.begin(), '-');
    try {
        const Numeric validated(text);
        (void)validated;
    } catch (...) {
        text.clear();
        return false;
    }
    return true;
}

bool encodePostgresNumeric(const std::string& value, std::vector<uint8_t>& raw) {
    raw.clear();
    Numeric numeric;
    try {
        numeric = Numeric(value);
    } catch (...) {
        return false;
    }
    const std::string canonical = numeric.toString();
    if (canonical == "NaN") {
        appendUInt16(raw, 0);
        appendUInt16(raw, 0);
        appendUInt16(raw, kNumericNaN);
        appendUInt16(raw, 0);
        return true;
    }
    if (canonical == "Infinity" || canonical == "-Infinity") {
        appendUInt16(raw, 0);
        appendUInt16(raw, 0);
        appendUInt16(raw, canonical.front() == '-' ? kNumericNegativeInfinity
                                                   : kNumericPositiveInfinity);
        appendUInt16(raw, 0);
        return true;
    }

    bool negative = false;
    std::string integerPart;
    std::string fractionalPart;
    if (!splitNumericText(value, negative, integerPart, fractionalPart) ||
        fractionalPart.size() > static_cast<size_t>(std::numeric_limits<int16_t>::max())) {
        return false;
    }
    std::vector<uint16_t> groups;
    int16_t weight = 0;
    if (!appendDecimalGroups(integerPart, fractionalPart, groups, weight) ||
        groups.size() > static_cast<size_t>(std::numeric_limits<int16_t>::max())) {
        return false;
    }
    appendUInt16(raw, static_cast<uint16_t>(groups.size()));
    appendUInt16(raw, static_cast<uint16_t>(weight));
    appendUInt16(raw, negative ? kNumericNegative : kNumericPositive);
    appendUInt16(raw, static_cast<uint16_t>(fractionalPart.size()));
    for (uint16_t group : groups) appendUInt16(raw, group);
    return true;
}

} // namespace dbms
