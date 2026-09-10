#pragma once

#include <array>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <limits>
#include <mutex>
#include <random>
#include <string>

namespace dbms {

class UuidValue {
public:
    using Bytes = std::array<uint8_t, 16>;

    UuidValue() = default;
    explicit UuidValue(const Bytes& bytes) : bytes_(bytes) {}

    const Bytes& bytes() const { return bytes_; }

    static bool parse(const std::string& input, UuidValue& output) {
        std::string text = input;
        if (!text.empty() && (text.front() == '{' || text.back() == '}')) {
            if (text.size() < 2 || text.front() != '{' ||
                text.back() != '}') {
                return false;
            }
            text = text.substr(1, text.size() - 2);
        }
        if (text.empty()) return false;

        std::string hex;
        hex.reserve(32);
        bool previousHyphen = false;
        for (const unsigned char character : text) {
            if (character == '-') {
                // PostgreSQL accepts a hyphen after any group of four hex
                // digits, in addition to the standard 8-4-4-4-12 layout.
                if (hex.empty() || hex.size() % 4 != 0 || previousHyphen)
                    return false;
                previousHyphen = true;
                continue;
            }
            if (!std::isxdigit(character) || hex.size() == 32)
                return false;
            hex.push_back(static_cast<char>(
                std::tolower(character)));
            previousHyphen = false;
        }
        if (hex.size() != 32 || previousHyphen) return false;

        Bytes bytes{};
        for (size_t i = 0; i < bytes.size(); ++i) {
            const int high = hexDigit(hex[i * 2]);
            const int low = hexDigit(hex[i * 2 + 1]);
            if (high < 0 || low < 0) return false;
            bytes[i] = static_cast<uint8_t>((high << 4) | low);
        }
        output = UuidValue(bytes);
        return true;
    }

    std::string toString() const {
        static constexpr char hex[] = "0123456789abcdef";
        std::string result;
        result.reserve(36);
        for (size_t i = 0; i < bytes_.size(); ++i) {
            if (i == 4 || i == 6 || i == 8 || i == 10)
                result.push_back('-');
            result.push_back(hex[bytes_[i] >> 4]);
            result.push_back(hex[bytes_[i] & 0x0f]);
        }
        return result;
    }

    std::string indexKey() const {
        // BPTree keys are limited to 20 bytes.  Store UUID's native 16-byte
        // representation so distinct values that share a long textual prefix
        // cannot be truncated to the same physical key.  The RFC byte order is
        // also PostgreSQL's UUID comparison order.
        return std::string(reinterpret_cast<const char*>(bytes_.data()),
                           bytes_.size());
    }

    bool isRfc9562Variant() const {
        return (bytes_[8] & 0xc0U) == 0x80U;
    }

    int version() const {
        return isRfc9562Variant() ? ((bytes_[6] >> 4) & 0x0f) : -1;
    }

    // Returns Unix epoch microseconds for RFC UUID versions 1 and 7.
    bool extractUnixMicros(int64_t& result) const {
        if (!isRfc9562Variant()) return false;
        if (version() == 7) {
            uint64_t milliseconds = 0;
            for (size_t i = 0; i < 6; ++i)
                milliseconds = (milliseconds << 8) | bytes_[i];
            const uint64_t subMilliseconds =
                (static_cast<uint64_t>(bytes_[6] & 0x0f) << 8) |
                bytes_[7];
            const uint64_t micros = milliseconds * 1000U +
                (subMilliseconds * 1000U) / 4096U;
            if (micros > static_cast<uint64_t>(
                             std::numeric_limits<int64_t>::max())) {
                return false;
            }
            result = static_cast<int64_t>(micros);
            return true;
        }
        if (version() == 1) {
            const uint64_t timeLow =
                (static_cast<uint64_t>(bytes_[0]) << 24) |
                (static_cast<uint64_t>(bytes_[1]) << 16) |
                (static_cast<uint64_t>(bytes_[2]) << 8) | bytes_[3];
            const uint64_t timeMid =
                (static_cast<uint64_t>(bytes_[4]) << 8) | bytes_[5];
            const uint64_t timeHigh =
                (static_cast<uint64_t>(bytes_[6] & 0x0f) << 8) |
                bytes_[7];
            const uint64_t uuidTicks =
                (timeHigh << 48) | (timeMid << 32) | timeLow;
            constexpr int64_t kUuidEpochTicks = 122192928000000000LL;
            const __int128 unixMicros =
                (static_cast<__int128>(uuidTicks) - kUuidEpochTicks) / 10;
            if (unixMicros < std::numeric_limits<int64_t>::min() ||
                unixMicros > std::numeric_limits<int64_t>::max()) {
                return false;
            }
            result = static_cast<int64_t>(unixMicros);
            return true;
        }
        return false;
    }

    static UuidValue generateV4() {
        Bytes bytes{};
        fillRandom(bytes.data(), bytes.size());
        bytes[6] = static_cast<uint8_t>((bytes[6] & 0x0fU) | 0x40U);
        bytes[8] = static_cast<uint8_t>((bytes[8] & 0x3fU) | 0x80U);
        return UuidValue(bytes);
    }

    static bool generateV7At(int64_t unixMicros, bool monotonic,
                             UuidValue& output) {
        if (unixMicros < 0) return false;
        constexpr uint64_t kMaxMilliseconds = (uint64_t{1} << 48) - 1;
        uint64_t milliseconds = static_cast<uint64_t>(unixMicros) / 1000U;
        if (milliseconds > kMaxMilliseconds) return false;
        const uint64_t microsWithinMillisecond =
            static_cast<uint64_t>(unixMicros) % 1000U;
        uint64_t prefix = (milliseconds << 12) |
            ((microsWithinMillisecond * 4096U) / 1000U);
        uint64_t randomTail = random62();

        if (monotonic) {
            static std::mutex stateMutex;
            static uint64_t lastPrefix = 0;
            static uint64_t lastTail = 0;
            std::lock_guard<std::mutex> lock(stateMutex);
            if (prefix <= lastPrefix) {
                prefix = lastPrefix;
                if (lastTail == ((uint64_t{1} << 62) - 1)) {
                    if (prefix == ((uint64_t{1} << 60) - 1)) return false;
                    ++prefix;
                    randomTail = random62();
                } else {
                    randomTail = lastTail + 1;
                }
            }
            lastPrefix = prefix;
            lastTail = randomTail;
        }

        milliseconds = prefix >> 12;
        const uint64_t subMilliseconds = prefix & 0x0fffU;
        Bytes bytes{};
        for (int index = 5; index >= 0; --index) {
            bytes[static_cast<size_t>(index)] =
                static_cast<uint8_t>(milliseconds & 0xffU);
            milliseconds >>= 8;
        }
        bytes[6] = static_cast<uint8_t>(
            0x70U | ((subMilliseconds >> 8) & 0x0fU));
        bytes[7] = static_cast<uint8_t>(subMilliseconds & 0xffU);
        bytes[8] = static_cast<uint8_t>(0x80U |
            ((randomTail >> 56) & 0x3fU));
        for (int index = 15; index >= 9; --index) {
            bytes[static_cast<size_t>(index)] =
                static_cast<uint8_t>(randomTail & 0xffU);
            randomTail >>= 8;
        }
        output = UuidValue(bytes);
        return true;
    }

    static UuidValue generateV7() {
        const auto now = std::chrono::time_point_cast<
            std::chrono::microseconds>(std::chrono::system_clock::now());
        const int64_t micros = now.time_since_epoch().count();
        UuidValue result;
        (void)generateV7At(micros, true, result);
        return result;
    }

    friend bool operator==(const UuidValue& left, const UuidValue& right) {
        return left.bytes_ == right.bytes_;
    }
    friend bool operator<(const UuidValue& left, const UuidValue& right) {
        return left.bytes_ < right.bytes_;
    }

private:
    static int hexDigit(char character) {
        if (character >= '0' && character <= '9') return character - '0';
        if (character >= 'a' && character <= 'f')
            return character - 'a' + 10;
        if (character >= 'A' && character <= 'F')
            return character - 'A' + 10;
        return -1;
    }

    static void fillRandom(uint8_t* destination, size_t length) {
        std::random_device random;
        for (size_t i = 0; i < length; ++i)
            destination[i] = static_cast<uint8_t>(random() & 0xffU);
    }

    static uint64_t random62() {
        uint64_t result = 0;
        uint8_t bytes[8]{};
        fillRandom(bytes, sizeof(bytes));
        for (const uint8_t byte : bytes)
            result = (result << 8) | byte;
        return result & ((uint64_t{1} << 62) - 1);
    }

    Bytes bytes_{};
};

}  // namespace dbms
