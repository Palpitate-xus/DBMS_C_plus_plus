#pragma once

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

namespace dbms {

// PostgreSQL's on-wire address-family tags (PGSQL_AF_INET/PGSQL_AF_INET6).
constexpr uint8_t kPgNetworkFamilyInet = 2;
constexpr uint8_t kPgNetworkFamilyInet6 = 3;

struct NetworkAddressValue {
    uint8_t family = 0;
    uint8_t bits = 0;
    std::array<uint8_t, 16> address{};

    size_t byteLength() const {
        return family == kPgNetworkFamilyInet ? 4 :
               family == kPgNetworkFamilyInet6 ? 16 : 0;
    }

    uint8_t maxBits() const {
        return family == kPgNetworkFamilyInet ? 32 :
               family == kPgNetworkFamilyInet6 ? 128 : 0;
    }

    bool hasHostBits() const {
        const size_t bytes = byteLength();
        if (bytes == 0 || bits > maxBits()) return true;
        const size_t whole = bits / 8;
        const unsigned partial = bits % 8;
        if (partial != 0 && whole < bytes) {
            const uint8_t hostMask = static_cast<uint8_t>(
                (1U << (8U - partial)) - 1U);
            if ((address[whole] & hostMask) != 0) return true;
        }
        const size_t firstHostByte = whole + (partial != 0 ? 1 : 0);
        for (size_t i = firstHostByte; i < bytes; ++i) {
            if (address[i] != 0) return true;
        }
        return false;
    }

    std::string toString(bool cidr) const {
        char text[INET6_ADDRSTRLEN]{};
        const int af = family == kPgNetworkFamilyInet ? AF_INET : AF_INET6;
        if (byteLength() == 0 ||
            inet_ntop(af, address.data(), text, sizeof(text)) == nullptr) {
            return {};
        }
        std::string result(text);
        if (cidr || bits != maxBits()) {
            result += "/" + std::to_string(bits);
        }
        return result;
    }
};

inline bool parseNetworkAddress(const std::string& input,
                                NetworkAddressValue& output,
                                bool requireNetworkAddress = false) {
    output = {};
    if (input.empty() || input.find('\0') != std::string::npos ||
        std::any_of(input.begin(), input.end(), [](unsigned char c) {
            return std::isspace(c) != 0;
        })) {
        return false;
    }
    const size_t slash = input.find('/');
    if (slash != std::string::npos && input.find('/', slash + 1) != std::string::npos)
        return false;
    const std::string addressText = input.substr(0, slash);
    if (addressText.empty()) return false;

    const bool ipv6 = addressText.find(':') != std::string::npos;
    output.family = ipv6 ? kPgNetworkFamilyInet6 : kPgNetworkFamilyInet;
    const uint8_t maximum = ipv6 ? 128 : 32;
    size_t octets = 0;
    if (ipv6) {
        if (inet_pton(AF_INET6, addressText.c_str(), output.address.data()) != 1) return false;
    } else {
        size_t position = 0;
        const auto decimal = [](char c) { return c >= '0' && c <= '9'; };
        const auto hexadecimal = [](char c) -> int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            return -1;
        };
        // CIDR also has a legacy packed hexadecimal network notation.
        // An odd final nibble occupies the high half of its byte.
        if (requireNetworkAddress && addressText.size() >= 2 && addressText[0] == '0' &&
            (addressText[1] == 'x' || addressText[1] == 'X')) {
            position = 2;
            while (position < addressText.size()) {
                if (octets == 4) return false;
                const int high = hexadecimal(addressText[position++]);
                if (high < 0) return false;
                unsigned value = static_cast<unsigned>(high) << 4;
                if (position < addressText.size()) {
                    const int low = hexadecimal(addressText[position++]);
                    if (low < 0) return false;
                    value |= static_cast<unsigned>(low);
                }
                output.address[octets++] = static_cast<uint8_t>(value);
            }
        } else {
            while (position < addressText.size()) {
                if (octets == 4 || !decimal(addressText[position])) return false;
                unsigned value = 0;
                while (position < addressText.size() && decimal(addressText[position])) {
                    const unsigned digit = static_cast<unsigned>(addressText[position++] - '0');
                    if (value > (255U - digit) / 10U) return false;
                    value = value * 10U + digit;
                }
                output.address[octets++] = static_cast<uint8_t>(value);
                if (position == addressText.size()) break;
                if (addressText[position++] != '.') return false;
                if (position == addressText.size()) {
                    // INET's legacy input permits a final dot; the ordinary
                    // octet-count/mask checks still apply. CIDR does not.
                    if (requireNetworkAddress) return false;
                    break;
                }
            }
        }
        if (octets == 0) return false;
    }
    output.bits = maximum;

    if (slash != std::string::npos) {
        const std::string prefix = input.substr(slash + 1);
        if (prefix.empty() || (ipv6 && prefix.size() > 1 && prefix[0] == '0') ||
            !std::all_of(prefix.begin(), prefix.end(), [](unsigned char c) {
                return std::isdigit(c) != 0;
            })) {
            return false;
        }
        unsigned parsed = 0;
        for (char c : prefix) {
            const unsigned digit = static_cast<unsigned>(c - '0');
            if (parsed > (static_cast<unsigned>(maximum) - digit) / 10U)
                return false;
            parsed = parsed * 10U + digit;
        }
        output.bits = static_cast<uint8_t>(parsed);
    } else if (!ipv6) {
        if (!requireNetworkAddress && octets != 4) return false;
        if (requireNetworkAddress) {
            const auto first = output.address[0];
            const unsigned inferred = first >= 240 ? 32 : first >= 224 ? 8 :
                                      first >= 192 ? 24 : first >= 128 ? 16 : 8;
            output.bits = static_cast<uint8_t>(std::max<size_t>(inferred, octets * 8));
            if (output.bits == 8 && first == 224) output.bits = 4;
        }
    }
    // INET accepts an abbreviated host only when an explicit mask does not
    // demand additional whole octets. Unspecified address bytes remain zero.
    if (!ipv6 && !requireNetworkAddress && output.bits / 8 > octets) return false;
    return !requireNetworkAddress || !output.hasHostBits();
}

inline int compareNetworkAddresses(const NetworkAddressValue& left,
                                   const NetworkAddressValue& right) {
    if (left.family != right.family)
        return left.family < right.family ? -1 : 1;
    const size_t bytes = left.byteLength();
    const unsigned commonBits = std::min(left.bits, right.bits);
    for (unsigned bit = 0; bit < commonBits; ++bit) {
        const bool a = (left.address[bit / 8] & (1U << (7U - bit % 8))) != 0;
        const bool b = (right.address[bit / 8] & (1U << (7U - bit % 8))) != 0;
        if (a != b) return a ? 1 : -1;
    }
    if (left.bits != right.bits) return left.bits < right.bits ? -1 : 1;
    const int full = std::memcmp(left.address.data(), right.address.data(), bytes);
    return full < 0 ? -1 : full > 0 ? 1 : 0;
}

inline bool networkContains(const NetworkAddressValue& container,
                            const NetworkAddressValue& contained,
                            bool allowEqual) {
    if (container.family != contained.family ||
        container.bits > contained.bits ||
        (!allowEqual && container.bits == contained.bits)) {
        return false;
    }
    for (unsigned bit = 0; bit < container.bits; ++bit) {
        const uint8_t mask = static_cast<uint8_t>(1U << (7U - bit % 8));
        if ((container.address[bit / 8] & mask) !=
            (contained.address[bit / 8] & mask)) {
            return false;
        }
    }
    return true;
}

inline bool networkOverlaps(const NetworkAddressValue& left,
                            const NetworkAddressValue& right) {
    return networkContains(left, right, true) ||
           networkContains(right, left, true);
}

enum class MacAddressInputError { None, InvalidSyntax, OutOfRange };

inline std::array<uint8_t,8> expandMacAddress48(const std::array<uint8_t,8>& value) {
    return {value[0],value[1],value[2],0xff,0xfe,value[3],value[4],value[5]};
}

namespace mac_codec_detail {
inline int hexDigit(unsigned char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}
inline void spaces(const std::string& input,size_t& position) {
    while (position < input.size() &&
           std::isspace(static_cast<unsigned char>(input[position]))) ++position;
}
// EUI-48's historical hex fields support signs, 0x and a maximum field width
// in the grouped spellings. Accumulation saturates safely before converting
// to the 32-bit field, rather than overflowing a signed scanf destination.
inline bool field(const std::string& input,size_t& position,size_t width,uint32_t& value) {
    spaces(input,position);
    const size_t end = width ? position + std::min(width,input.size()-position) : input.size();
    bool negative = false;
    if (position < end && (input[position] == '+' || input[position] == '-'))
        negative = input[position++] == '-';
    const bool prefix = end-position >= 2 && input[position] == '0' &&
                        (input[position+1] == 'x' || input[position+1] == 'X');
    if (prefix) position += 2;
    const size_t firstDigit = position;
    uint64_t magnitude = 0;
    bool overflow = false;
    while (position < end) {
        const int digit = hexDigit(static_cast<unsigned char>(input[position]));
        if (digit < 0) break;
        if (magnitude > (std::numeric_limits<uint64_t>::max()-static_cast<unsigned>(digit))/16U) {
            magnitude = std::numeric_limits<uint64_t>::max();
            overflow = true;
        } else magnitude = magnitude*16U + static_cast<unsigned>(digit);
        ++position;
    }
    if (position == firstDigit && !prefix) return false;
    value = static_cast<uint32_t>(negative && !overflow ? uint64_t{0}-magnitude : magnitude);
    return true;
}
} // namespace mac_codec_detail

inline bool parseMacAddress(const std::string& input, size_t byteCount,
                            std::array<uint8_t, 8>& output,
                            MacAddressInputError* failure = nullptr) {
    output.fill(0);
    if (failure) *failure = MacAddressInputError::InvalidSyntax;
    if ((byteCount != 6 && byteCount != 8) || input.find('\0') != std::string::npos) return false;
    std::array<uint8_t,8> parsed{};
    if (byteCount == 6) {
        // Seven distinct formats: separators cannot be deleted or mixed.
        for (size_t layout = 0; layout < 7; ++layout) {
            const size_t group = layout < 2 ? 1 : layout < 4 ? 3 : layout < 6 ? 2 : 6;
            const char separator = layout == 0 || layout == 2 ? ':' : layout == 4 ? '.' : '-';
            size_t position = 0;
            std::array<uint32_t,6> fields{};
            bool matched = true;
            for (size_t i = 0; i < fields.size(); ++i) {
                if (!mac_codec_detail::field(input,position,layout < 2 ? 0 : 2,fields[i])) {
                    matched = false; break;
                }
                if (i+1 < fields.size() && (i+1)%group == 0) {
                    if (position == input.size() || input[position++] != separator) {
                        matched = false; break;
                    }
                }
            }
            mac_codec_detail::spaces(input,position);
            if (!matched || position != input.size()) continue;
            for (size_t i = 0; i < fields.size(); ++i) {
                if (fields[i] > 255) {
                    if (failure) *failure = MacAddressInputError::OutOfRange;
                    return false;
                }
                parsed[i] = static_cast<uint8_t>(fields[i]);
            }
            output = parsed;
            if (failure) *failure = MacAddressInputError::None;
            return true;
        }
        return false;
    }
    size_t position = 0, count = 0;
    char separator = '\0';
    mac_codec_detail::spaces(input,position);
    while (position < input.size() && input.size()-position >= 2) {
        if (count == parsed.size()) return false;
        const int high = mac_codec_detail::hexDigit(static_cast<unsigned char>(input[position]));
        const int low = mac_codec_detail::hexDigit(static_cast<unsigned char>(input[position+1]));
        if (high < 0 || low < 0) return false;
        parsed[count++] = static_cast<uint8_t>(high*16 + low);
        position += 2;
        if (position < input.size() &&
            (input[position] == ':' || input[position] == '-' || input[position] == '.')) {
            if (separator && separator != input[position]) return false;
            separator = input[position++];
        }
        if ((count == 6 || count == 8) && position < input.size() &&
            std::isspace(static_cast<unsigned char>(input[position]))) {
            mac_codec_detail::spaces(input,position);
            if (position != input.size()) return false;
        }
    }
    // Preserve the input function's legacy single-character suffix behavior
    // after a complete EUI value, as verified against actual PostgreSQL 18.6.
    if (count == 6) parsed = expandMacAddress48(parsed);
    else if (count != 8) return false;
    output = parsed;
    if (failure) *failure = MacAddressInputError::None;
    return true;
}

inline std::string formatMacAddress(const uint8_t* bytes, size_t byteCount) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(byteCount * 3 - 1);
    for (size_t i = 0; i < byteCount; ++i) {
        if (i != 0) result.push_back(':');
        result.push_back(digits[bytes[i] >> 4]);
        result.push_back(digits[bytes[i] & 0x0f]);
    }
    return result;
}

} // namespace dbms
