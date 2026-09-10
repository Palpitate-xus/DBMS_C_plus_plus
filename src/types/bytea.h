#pragma once

#include <cctype>
#include <cstdint>
#include <string>
#include <utility>

namespace dbms {

// Logical PostgreSQL bytea value.  The engine's textual boundary uses the
// canonical lowercase hex representation, while operations and protocol I/O
// work on the decoded bytes.
class ByteaValue {
public:
    ByteaValue() = default;

    static ByteaValue fromBytes(std::string bytes) {
        ByteaValue result;
        result.bytes_ = std::move(bytes);
        return result;
    }

    static bool parse(const std::string& input, ByteaValue& output) {
        std::string bytes;
        if (input.size() >= 2 && input[0] == '\\' &&
            (input[1] == 'x' || input[1] == 'X')) {
            int highNibble = -1;
            for (size_t index = 2; index < input.size(); ++index) {
                const unsigned char character =
                    static_cast<unsigned char>(input[index]);
                if (std::isspace(character)) {
                    // PostgreSQL permits whitespace between byte pairs, but
                    // not between the two digits of a byte.
                    if (highNibble >= 0) return false;
                    continue;
                }
                const int nibble = hexDigit(character);
                if (nibble < 0) return false;
                if (highNibble < 0) {
                    highNibble = nibble;
                } else {
                    bytes.push_back(static_cast<char>(
                        (highNibble << 4) | nibble));
                    highNibble = -1;
                }
            }
            if (highNibble >= 0) return false;
        } else {
            for (size_t index = 0; index < input.size();) {
                if (input[index] != '\\') {
                    bytes.push_back(input[index++]);
                    continue;
                }
                if (index + 1 < input.size() && input[index + 1] == '\\') {
                    bytes.push_back('\\');
                    index += 2;
                    continue;
                }
                if (index + 3 >= input.size() || input[index + 1] < '0' ||
                    input[index + 1] > '3' || input[index + 2] < '0' ||
                    input[index + 2] > '7' || input[index + 3] < '0' ||
                    input[index + 3] > '7') {
                    return false;
                }
                const unsigned value =
                    static_cast<unsigned>(input[index + 1] - '0') * 64U +
                    static_cast<unsigned>(input[index + 2] - '0') * 8U +
                    static_cast<unsigned>(input[index + 3] - '0');
                bytes.push_back(static_cast<char>(value));
                index += 4;
            }
        }
        output = fromBytes(std::move(bytes));
        return true;
    }

    const std::string& bytes() const { return bytes_; }

    std::string toString() const {
        static constexpr char hex[] = "0123456789abcdef";
        std::string result;
        result.reserve(2 + bytes_.size() * 2);
        result = "\\x";
        for (const unsigned char byte : bytes_) {
            result.push_back(hex[byte >> 4]);
            result.push_back(hex[byte & 0x0f]);
        }
        return result;
    }

    friend bool operator==(const ByteaValue& left, const ByteaValue& right) {
        return left.bytes_ == right.bytes_;
    }

    friend bool operator<(const ByteaValue& left, const ByteaValue& right) {
        const size_t shared =
            left.bytes_.size() < right.bytes_.size()
                ? left.bytes_.size() : right.bytes_.size();
        for (size_t index = 0; index < shared; ++index) {
            const auto leftByte = static_cast<unsigned char>(left.bytes_[index]);
            const auto rightByte = static_cast<unsigned char>(right.bytes_[index]);
            if (leftByte != rightByte) return leftByte < rightByte;
        }
        return left.bytes_.size() < right.bytes_.size();
    }

private:
    static int hexDigit(unsigned char character) {
        if (character >= '0' && character <= '9') return character - '0';
        if (character >= 'a' && character <= 'f')
            return character - 'a' + 10;
        if (character >= 'A' && character <= 'F')
            return character - 'A' + 10;
        return -1;
    }

    std::string bytes_;
};

inline bool isByteaTypeName(const std::string& typeName) {
    std::string lowered;
    lowered.reserve(typeName.size());
    for (const unsigned char character : typeName)
        lowered.push_back(static_cast<char>(std::tolower(character)));
    return lowered == "bytea" || lowered == "blob" ||
           lowered == "binary" || lowered == "varbinary";
}

}  // namespace dbms
