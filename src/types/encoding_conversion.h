#pragma once

#include <cctype>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace dbms {

enum class BuiltinEncoding { Utf8, Latin1, SqlAscii };

inline bool parseBuiltinEncoding(std::string name, BuiltinEncoding& encoding) {
    std::string normalized;
    normalized.reserve(name.size());
    for (const unsigned char character : name) {
        if (std::isspace(character) || character == '-' || character == '_')
            continue;
        normalized.push_back(static_cast<char>(std::toupper(character)));
    }
    if (normalized == "UTF8" || normalized == "UNICODE") {
        encoding = BuiltinEncoding::Utf8;
        return true;
    }
    if (normalized == "LATIN1" || normalized == "ISO88591") {
        encoding = BuiltinEncoding::Latin1;
        return true;
    }
    if (normalized == "SQLASCII" || normalized == "ASCII") {
        encoding = BuiltinEncoding::SqlAscii;
        return true;
    }
    return false;
}

inline bool decodeUtf8(const std::string& input,
                       std::vector<uint32_t>& codePoints) {
    codePoints.clear();
    codePoints.reserve(input.size());
    for (size_t index = 0; index < input.size();) {
        const auto byte = [&](size_t offset) {
            return static_cast<unsigned char>(input[index + offset]);
        };
        const auto continuation = [&](size_t offset) {
            return index + offset < input.size() &&
                   (byte(offset) & 0xc0U) == 0x80U;
        };
        const unsigned char first = byte(0);
        uint32_t codePoint = 0;
        size_t width = 0;
        if (first >= 0x01 && first <= 0x7f) {
            codePoint = first;
            width = 1;
        } else if (first >= 0xc2 && first <= 0xdf && continuation(1)) {
            codePoint = ((first & 0x1fU) << 6) | (byte(1) & 0x3fU);
            width = 2;
        } else if (first >= 0xe0 && first <= 0xef && continuation(1) &&
                   continuation(2) && !(first == 0xe0 && byte(1) < 0xa0) &&
                   !(first == 0xed && byte(1) >= 0xa0)) {
            codePoint = ((first & 0x0fU) << 12) |
                        ((byte(1) & 0x3fU) << 6) | (byte(2) & 0x3fU);
            width = 3;
        } else if (first >= 0xf0 && first <= 0xf4 && continuation(1) &&
                   continuation(2) && continuation(3) &&
                   !(first == 0xf0 && byte(1) < 0x90) &&
                   !(first == 0xf4 && byte(1) > 0x8f)) {
            codePoint = ((first & 0x07U) << 18) |
                        ((byte(1) & 0x3fU) << 12) |
                        ((byte(2) & 0x3fU) << 6) | (byte(3) & 0x3fU);
            width = 4;
        } else {
            return false;
        }
        codePoints.push_back(codePoint);
        index += width;
    }
    return true;
}

inline void appendUtf8CodePoint(uint32_t codePoint, std::string& output) {
    if (codePoint <= 0x7f) {
        output.push_back(static_cast<char>(codePoint));
    } else if (codePoint <= 0x7ff) {
        output.push_back(static_cast<char>(0xc0U | (codePoint >> 6)));
        output.push_back(static_cast<char>(0x80U | (codePoint & 0x3fU)));
    } else if (codePoint <= 0xffff) {
        output.push_back(static_cast<char>(0xe0U | (codePoint >> 12)));
        output.push_back(static_cast<char>(
            0x80U | ((codePoint >> 6) & 0x3fU)));
        output.push_back(static_cast<char>(0x80U | (codePoint & 0x3fU)));
    } else {
        output.push_back(static_cast<char>(0xf0U | (codePoint >> 18)));
        output.push_back(static_cast<char>(
            0x80U | ((codePoint >> 12) & 0x3fU)));
        output.push_back(static_cast<char>(
            0x80U | ((codePoint >> 6) & 0x3fU)));
        output.push_back(static_cast<char>(0x80U | (codePoint & 0x3fU)));
    }
}

inline bool decodeBuiltinCharacters(const std::string& input,
                                    BuiltinEncoding encoding,
                                    std::vector<uint32_t>& codePoints) {
    if (encoding == BuiltinEncoding::Utf8)
        return decodeUtf8(input, codePoints);
    codePoints.clear();
    codePoints.reserve(input.size());
    for (const unsigned char byte : input) {
        if (byte == 0 ||
            (encoding == BuiltinEncoding::SqlAscii && byte > 0x7f)) {
            return false;
        }
        codePoints.push_back(byte);
    }
    return true;
}

inline bool encodeBuiltinCharacters(const std::vector<uint32_t>& codePoints,
                                    BuiltinEncoding encoding,
                                    std::string& output) {
    output.clear();
    for (const uint32_t codePoint : codePoints) {
        if (codePoint == 0 || codePoint > 0x10ffff ||
            (codePoint >= 0xd800 && codePoint <= 0xdfff)) {
            output.clear();
            return false;
        }
        if (encoding == BuiltinEncoding::Utf8) {
            appendUtf8CodePoint(codePoint, output);
        } else {
            const uint32_t maximum =
                encoding == BuiltinEncoding::Latin1 ? 0xffU : 0x7fU;
            if (codePoint > maximum) {
                output.clear();
                return false;
            }
            output.push_back(static_cast<char>(codePoint));
        }
    }
    return true;
}

inline bool convertBuiltinEncoding(const std::string& input,
                                   BuiltinEncoding source,
                                   BuiltinEncoding destination,
                                   std::string& output,
                                   size_t* characterCount = nullptr) {
    std::vector<uint32_t> codePoints;
    if (!decodeBuiltinCharacters(input, source, codePoints)) return false;
    if (characterCount) *characterCount = codePoints.size();
    return encodeBuiltinCharacters(codePoints, destination, output);
}

}  // namespace dbms
