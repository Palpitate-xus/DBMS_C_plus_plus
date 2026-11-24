#pragma once

#include <string>
#include <string_view>

namespace dbms {

// PostgreSQL's catalog "char" type stores one byte. Its text input uses
// the first byte or a three-digit octal escape.
inline unsigned char parseInternalCharByte(std::string_view input) {
    if (input.size() == 4 && input.front() == '\\' &&
        input[1] >= '0' && input[1] <= '7' && input[2] >= '0' && input[2] <= '7' &&
        input[3] >= '0' && input[3] <= '7') {
        return static_cast<unsigned char>((input[1] - '0') * 64 +
            (input[2] - '0') * 8 + input[3] - '0');
    }
    return input.empty() ? 0 : static_cast<unsigned char>(input.front());
}

inline std::string internalCharText(unsigned char byte) {
    if (byte >= 128)
        return {'\\', static_cast<char>('0' + (byte >> 6)),
            static_cast<char>('0' + ((byte >> 3) & 7)), static_cast<char>('0' + (byte & 7))};
    return byte ? std::string(1, static_cast<char>(byte)) : std::string{};
}

} // namespace dbms
