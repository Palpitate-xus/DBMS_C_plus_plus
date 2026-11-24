#pragma once

#include "common/DbError.h"

#include <cerrno>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>

namespace dbms {

inline uint32_t parseOidInput(const std::string& text) {
    const auto* begin = text.c_str();
    char* end = nullptr;
    errno = 0;
    const uint64_t value = std::strtoull(begin, &end, 0);
    if (errno == ERANGE)
        throw DbError("22003", "OID out of range");
    if (end != begin) {
        while (end != begin + text.size() && std::isspace(static_cast<unsigned char>(*end))) ++end;
        if (end == begin + text.size()) {
            const auto oid = static_cast<uint32_t>(value);
            // PostgreSQL accepts the sign-extended representation of a
            // negative int32 as input, as well as the full unsigned range.
            constexpr auto maximum = std::numeric_limits<uint32_t>::max();
            if (value == oid || (oid > std::numeric_limits<int32_t>::max() &&
                                value == (static_cast<uint64_t>(oid) | (UINT64_MAX ^ maximum))))
                return oid;
            throw DbError("22003", "OID out of range");
        }
    }
    throw DbError("22P02", "invalid input syntax for type oid: \"" + text + "\"");
}

} // namespace dbms
