#pragma once

#include <string>

namespace dbms {

// Keep every pre-existing, unambiguous sequence filename unchanged. Names
// that cannot be represented by the legacy one-dot format use a reserved
// two-dot prefix, which no legacy sequence filename could have passed the
// old validator. The payload is schema + NUL + relation in base64url; it is
// reversible and cannot contain a path separator.
inline std::string sequenceStorageName(const std::string& schema,
                                       const std::string& relation) {
    const std::string effectiveSchema = schema.empty() ? "public" : schema;
    const std::string legacy = effectiveSchema == "public"
        ? relation : effectiveSchema + "." + relation;
    const auto firstDot = legacy.find('.');
    const bool legacyFormat = firstDot == std::string::npos ||
        (firstDot != 0 && firstDot + 1 < legacy.size() &&
         legacy.find('.', firstDot + 1) == std::string::npos);
    if (legacyFormat) return legacy;

    const std::string bytes = effectiveSchema + '\0' + relation;
    constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string encoded = "seqv2..";
    encoded.reserve(7 + (bytes.size() * 4 + 2) / 3);
    for (size_t i = 0; i < bytes.size(); i += 3) {
        const unsigned char a = static_cast<unsigned char>(bytes[i]);
        const unsigned char b = i + 1 < bytes.size()
            ? static_cast<unsigned char>(bytes[i + 1]) : 0;
        const unsigned char c = i + 2 < bytes.size()
            ? static_cast<unsigned char>(bytes[i + 2]) : 0;
        encoded += alphabet[a >> 2];
        encoded += alphabet[((a & 0x03) << 4) | (b >> 4)];
        if (i + 1 < bytes.size()) {
            encoded += alphabet[((b & 0x0f) << 2) | (c >> 6)];
        }
        if (i + 2 < bytes.size()) encoded += alphabet[c & 0x3f];
    }
    return encoded;
}

} // namespace dbms
