#pragma once

#include <arpa/inet.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstring>
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
    if (input.empty() ||
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
    const int af = ipv6 ? AF_INET6 : AF_INET;
    const uint8_t maximum = ipv6 ? 128 : 32;
    if (inet_pton(af, addressText.c_str(), output.address.data()) != 1) return false;
    output.bits = maximum;

    if (slash != std::string::npos) {
        const std::string prefix = input.substr(slash + 1);
        if (prefix.empty() ||
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
    }
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

inline bool parseMacAddress(const std::string& input, size_t byteCount,
                            std::array<uint8_t, 8>& output) {
    output.fill(0);
    if (byteCount != 6 && byteCount != 8) return false;
    std::string hex;
    hex.reserve(byteCount * 2);
    for (unsigned char c : input) {
        if (c == ':' || c == '-' || c == '.') continue;
        if (!std::isxdigit(c)) return false;
        hex.push_back(static_cast<char>(c));
    }
    if (hex.size() != byteCount * 2) return false;
    auto digit = [](char c) -> uint8_t {
        if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return static_cast<uint8_t>(c - 'a' + 10);
    };
    for (size_t i = 0; i < byteCount; ++i) {
        output[i] = static_cast<uint8_t>(digit(hex[i * 2]) * 16U +
                                         digit(hex[i * 2 + 1]));
    }
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
